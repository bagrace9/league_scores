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
import tempfile
from config import get_storage_config
from file import File
from logger import setup_logging, upload_log_to_gcs

logger = setup_logging()
VIEWS_SQL_PATH = 'sql/bigquery/create_views.sql'
HANDICAPS_SQL_PATH = 'sql/bigquery/create_handicaps_table.sql'
ADJUSTED_SCORES_SQL_PATH = 'sql/bigquery/create_adjusted_scores.sql'
CREATE_TABLES_SQL_PATH = 'sql/bigquery/create_perm_tables.sql'
PLAYERS_SQL_PATH = 'sql/bigquery/create_season_players_summary_table.sql'
SEASON_LOG_SQL_PATH = 'sql/bigquery/create_season_log_table.sql'
SEASON_EVENT_SUMMARY_SQL_PATH = 'sql/bigquery/create_season_event_summary_table.sql'


def maybe_upload_log_to_gcs():
    """Upload run log to GCS when enabled by configuration."""
    storage_config = get_storage_config()
    upload_log = storage_config.get('upload_log_to_gcs', False)
    if not upload_log:
        return

    bucket = storage_config.get('bucket')
    if not bucket:
        logger.warning('UPLOAD_LOG_TO_GCS is enabled but GCS_BUCKET is not set. Skipping log upload.')
        return

    configured_prefix = (storage_config.get('log_prefix') or storage_config.get('prefix') or '').strip('/')
    try:
        uri = upload_log_to_gcs(logger, bucket, configured_prefix)
        logger.info(f'Uploaded run log to {uri}')
    except Exception as error:
        logger.warning(f'Failed to upload run log to GCS: {error}')


def main():
    
    database.execute_sql_script(CREATE_TABLES_SQL_PATH)
    if not database.payouts_table_exists():
        logger.info('Payouts table missing. Creating payouts table.')
        database.create_payout_table()
    
    logger.info('Starting league event download run')

    # Track already downloaded events so we only download new files.
    imported_urls = database.fetch_imported_event_urls()
    downloaded_files = []
    storage_config = get_storage_config()
    archive_files = storage_config.get('archive_files', True)
    archive_bucket = storage_config.get('bucket')
    archive_prefix = (storage_config.get('prefix') or '').strip('/')
    archive_subdir = f"{archive_prefix}/Imported" if archive_prefix else 'Imported'
    imported_dir = Path(__file__).parent / 'exports' / 'Imported'
    download_dir = Path(__file__).parent / 'exports'

    if archive_bucket and archive_files:
        download_dir = Path(tempfile.gettempdir()) / 'league_scores' / (archive_prefix or 'default')

    if not archive_files:
        logger.info('File archive disabled: imported files will be deleted after import.')
    elif archive_bucket:
        logger.info(f"Archive target configured: gs://{archive_bucket}/{archive_subdir}")
        logger.info(f"Using temporary download directory: {download_dir}")
    else:
        logger.info(f"Archive target configured: local path {imported_dir}")

    league_rows = database.fetch_leagues()
    league_ids = [league_id for (league_id,) in league_rows]

    for league_id in league_ids:
        league_urls = database.fetch_league_urls(league_id)
        for league_url in league_urls:
            event_links = scrape_udisc.get_event_links(league_url)
            for event_link in event_links:
                download_links = scrape_udisc.find_download_links_on_page(event_link)
                for export_url in download_links:
                    if export_url in imported_urls:
                        logger.info(f"Already imported event, skipping download: {export_url}")
                        continue
                    elif export_url.endswith('leaderboard/export'):
                        result = scrape_udisc.download_event_data(export_url, download_dir=str(download_dir))
                        if result['success']:
                            downloaded_file = File.from_download_result(result)

                            if (
                                downloaded_file.event_end_date is not None
                                and downloaded_file.event_end_date >= date.today()
                            ):
                                deleted = downloaded_file.delete_from_disk()
                                if deleted:
                                    logger.info(
                                        f"Deleted file for unfinished event: {downloaded_file.filename} (event_end_date={downloaded_file.event_end_date})"
                                    )
                                else:
                                    logger.warning(
                                        f"Could not delete unfinished event file: {downloaded_file.filename}"
                                    )
                                del downloaded_file
                                continue

                            downloaded_files.append((league_id, downloaded_file))
                            imported_urls.add(export_url)
                            logger.info(f"Queued for import: {downloaded_file.filename}")
                        else:
                            logger.error(f"Failed to download {export_url}: {result['error']}")
    
    if not downloaded_files:
        logger.info("No new files to import.")
        
    else:
        logger.info(f"Finished downloading files. Starting import of {len(downloaded_files)} new files.")

        for league_id, downloaded_file in downloaded_files:
            try:
                event_id = database.import_downloaded_file(league_id, downloaded_file)
                logger.info(
                    f"Imported file into events/raw_scores/hole_scores (event_id={event_id}): {downloaded_file.filename}"
                )

                if not archive_files:
                    database.update_event_file_metadata(
                        event_id,
                        downloaded_file.filename,
                        None,
                    )
                    deleted = downloaded_file.delete_from_disk()
                    if deleted:
                        logger.info(f"Deleted imported file from local disk: {downloaded_file.filepath}")
                    else:
                        logger.warning(f"Could not delete imported file from local disk: {downloaded_file.filepath}")
                elif archive_bucket:
                    archived = downloaded_file.upload_to_gcs(archive_bucket, archive_subdir)
                    if archived:
                        database.update_event_file_metadata(
                            event_id,
                            downloaded_file.filename,
                            downloaded_file.filepath,
                        )
                        logger.info(f"Archived imported file to {downloaded_file.filepath}")
                    else:
                        logger.warning(
                            f"Could not archive imported file to GCS: {downloaded_file.filename}; error={downloaded_file.error}"
                        )
                        deleted = downloaded_file.delete_from_disk()
                        if deleted:
                            logger.info(
                                f"Deleted temporary local file after GCS archive failure: {downloaded_file.filepath}"
                            )
                        else:
                            logger.warning(
                                f"Could not delete temporary local file after GCS archive failure: {downloaded_file.filepath}"
                            )
                else:
                    moved = downloaded_file.move_to_directory(imported_dir)
                    if moved:
                        database.update_event_file_metadata(
                            event_id,
                            downloaded_file.filename,
                            downloaded_file.filepath,
                        )
                        logger.info(f"Moved imported file to {downloaded_file.filepath}")
                    else:
                        logger.warning(f"Could not move imported file: {downloaded_file.filename}")
            except Exception as error:
                logger.error(
                    f"Failed to import file {downloaded_file.filename}: {error}"
                )
                if archive_bucket or not archive_files:
                    deleted = downloaded_file.delete_from_disk()
                    if deleted:
                        logger.info(f"Deleted local file after import failure: {downloaded_file.filepath}")
                    else:
                        logger.warning(
                            f"Could not delete local file after import failure: {downloaded_file.filepath}"
                        )

        logger.info('Finished importing files.')

    # Run in dependency order.
    for label, path in [
        ('handicap', HANDICAPS_SQL_PATH),
        ('adjusted scores', ADJUSTED_SCORES_SQL_PATH),
        ('season event summary', SEASON_EVENT_SUMMARY_SQL_PATH),
        ('players', PLAYERS_SQL_PATH),
        ('season log', SEASON_LOG_SQL_PATH),
        #('views', VIEWS_SQL_PATH),
    ]:
        try:
            logger.info(f"Running {label} script: {path}")
            database.execute_sql_script(path)
            logger.info(f"Finished {label} update.")
        except Exception as error:
            logger.error(f"Failed running {label} script ({path}): {error}")


if __name__ == "__main__":
    try:
        main()
    finally:
        maybe_upload_log_to_gcs()