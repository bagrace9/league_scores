"""
Entry point for the league scores pipeline.

Orchestrates the full run in order:
  1. Ensure all database tables and the payouts lookup table exist.
  2. Scrape UDisc for new event export links across all configured leagues.
  3. Download and import any events not yet in the database.
  4. Rebuild handicaps, final scores, and views from the newly imported data.
"""
import scrape_udisc
import database
from datetime import date
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import tempfile
from config import get_storage_config
from file import File
from league_bootstrap import bootstrap_leagues_if_empty
from logger import setup_logging

logger = setup_logging()
HANDICAPS_SQL_PATH = 'sql/bigquery/create_handicaps_table.sql'
ADJUSTED_SCORES_SQL_PATH = 'sql/bigquery/create_adjusted_scores.sql'
CREATE_TABLES_SQL_PATH = 'sql/bigquery/create_perm_tables.sql'
PLAYERS_SQL_PATH = 'sql/bigquery/create_season_players_summary_table.sql'
SEASON_LOG_SQL_PATH = 'sql/bigquery/create_season_log_table.sql'
SEASON_EVENT_SUMMARY_SQL_PATH = 'sql/bigquery/create_season_event_summary_table.sql'

download_files = True # Set to True to enable downloading and importing new files from UDisc; set to False to only run updates on already imported data (handicaps, final scores, views, etc)

def main():
    
    storage_config = get_storage_config()
    
    database.execute_sql_script(CREATE_TABLES_SQL_PATH)
    database.sync_permanent_table_schemas()
    database.prune_abandoned_scores()
    database.drop_template_tables() # Drop templates after sync
    
    if not database.payouts_table_exists():
        logger.info('Payouts table missing. Creating payouts table.')
        database.create_payout_table()
    
    
    logger.info('Starting league event download run')

    #Track already downloaded events so we only download new files.
    imported_urls = database.fetch_imported_event_urls()
    downloaded_files = []
    archive_files = storage_config.get('archive_files', True)
    archive_bucket = storage_config.get('bucket')
    archive_subdir = 'imported_files'
    imported_dir = Path(__file__).parent / 'exports' / 'Imported'
    download_dir = Path(__file__).parent / 'exports'

    if archive_bucket and archive_files:
        download_dir = Path(tempfile.gettempdir()) / 'league_scores'

    if not archive_files:
        logger.info('File archive disabled: imported files will be deleted after import.')
    elif archive_bucket:
        logger.info(f"Archive target configured: gs://{archive_bucket}/{archive_subdir}")
        logger.info(f"Using temporary download directory: {download_dir}")
    else:
        logger.info(f"Archive target configured: local path {imported_dir}")

    league_rows = bootstrap_leagues_if_empty()
    league_ids = [league_id for (league_id,) in league_rows]
    
    
    if download_files:
        def process_event_link(league_id, event_link):
            """Worker to fetch and download from a single event link."""
            downloads = []
            download_links, event_end_date = scrape_udisc.find_download_links_on_page(event_link)
            for export_url in download_links:
                if export_url in imported_urls or not export_url.endswith('leaderboard/export'):
                    continue
                
                result = scrape_udisc.download_event_data(export_url, download_dir=str(download_dir))
                if result['success']:
                    result['event_end_date'] = event_end_date
                    df_file = File.from_download_result(result)
                    downloads.append((league_id, df_file))
            return downloads

        # Use threads for network-bound scraping tasks
        with ThreadPoolExecutor(max_workers=10) as executor:
            future_to_url = {}
            for league_id in league_ids:
                for league_url in database.fetch_league_urls(league_id):
                    # Pagination in get_event_links is still sequential per league
                    event_links = scrape_udisc.get_event_links(league_url)
                    for link in event_links:
                        future_to_url[executor.submit(process_event_link, league_id, link)] = link
            
            for future in as_completed(future_to_url):
                try:
                    results = future.result()
                    for lid, dfile in results:
                        downloaded_files.append((lid, dfile))
                        imported_urls.add(dfile.export_url)
                        logger.info(f"Queued for import: {dfile.filename}")
                except Exception as e:
                    logger.error(f"Worker failed for {future_to_url[future]}: {e}")
        
        if not downloaded_files:
            logger.info("No new files to import.")
            
        else:
            logger.info(f"Finished downloading files. Starting import of {len(downloaded_files)} new files.")

            all_event_rows = []
            all_raw_rows = []
            all_hole_rows = []
            finished_event_ids = []
            import_manifest = [] # List of (event_id, downloaded_file)

            for league_id, downloaded_file in downloaded_files:
                try:
                    is_finished = downloaded_file.event_end_date < date.today() if downloaded_file.event_end_date else True
                    existing_event = database.fetch_event_by_url(downloaded_file.export_url)
                    
                    # If it's already in the DB and finished, we've likely handled it (or it's an edge case)
                    if existing_event and existing_event['is_imported'] and is_finished:
                        continue

                    event_id = existing_event['event_id'] if existing_event else None
                    data = database.prepare_import_data(league_id, downloaded_file, event_id=event_id)
                    
                    if data:
                        # Add event metadata if the event record doesn't exist yet
                        if not existing_event:
                            all_event_rows.append(data['event_row'])
                        
                        if is_finished:
                            all_raw_rows.extend(data['raw_score_rows'])
                            all_hole_rows.extend(data['hole_score_rows'])
                            finished_event_ids.append(data['event_id'])
                            import_manifest.append((data['event_id'], downloaded_file))
                            logger.info(f"Prepared full import: {downloaded_file.filename}")
                        else:
                            downloaded_file.delete_from_disk()
                            logger.info(f"Added unfinished event metadata only: {downloaded_file.filename}")
                            
                except Exception as error:
                    logger.error(f"Failed to parse file {downloaded_file.filename}: {error}")

            if all_event_rows or all_raw_rows:
                database.execute_bulk_import(all_event_rows, all_raw_rows, all_hole_rows, mark_imported_ids=finished_event_ids)
                logger.info(f"Bulk import completed. {len(all_event_rows)} new event records, {len(finished_event_ids)} events fully imported.")
                
                metadata_updates = []
                for event_id, downloaded_file in import_manifest:
                    if not archive_files:
                        downloaded_file.delete_from_disk()
                        metadata_updates.append({'event_id': event_id, 'file_name': downloaded_file.filename, 'file_path': None})
                    elif archive_bucket:
                        if downloaded_file.upload_to_gcs(archive_bucket, archive_subdir):
                            metadata_updates.append({'event_id': event_id, 'file_name': downloaded_file.filename, 'file_path': downloaded_file.filepath})
                        else:
                            downloaded_file.delete_from_disk()
                    else:
                        if downloaded_file.move_to_directory(imported_dir):
                            metadata_updates.append({'event_id': event_id, 'file_name': downloaded_file.filename, 'file_path': downloaded_file.filepath})
                
                if metadata_updates:
                    database.bulk_update_event_file_metadata(metadata_updates)
                    logger.info("Updated file metadata for all imported events.")

            logger.info('Finished importing files.')
        
    
    database.apply_event_updates()

    # Run in dependency order.
    for label, path in [
        ('handicap', HANDICAPS_SQL_PATH),
        ('adjusted scores', ADJUSTED_SCORES_SQL_PATH),
        ('season event summary', SEASON_EVENT_SUMMARY_SQL_PATH),
        ('players', PLAYERS_SQL_PATH),
        ('season log', SEASON_LOG_SQL_PATH),
    ]:
        try:
            logger.info(f"Running {label} script: {path}")
            database.execute_sql_script(path)
            logger.info(f"Finished {label} update.")
        except Exception as error:
            logger.error(f"Failed running {label} script ({path}): {error}")


if __name__ == "__main__":
    main()