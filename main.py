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
from file import File
from logger import setup_logging

logger = setup_logging()
VIEWS_SQL_PATH = 'sql/create_views.sql'
HANDICAPS_SQL_PATH = 'sql/drop_create_handicaps_table.sql'
FINAL_SCORES_SQL_PATH = 'sql/merge_into_final_scores.sql'


def main():
    
    database.run_create_script()
    if not database.payouts_table_exists():
        logger.info('Payouts table missing. Creating payouts table.')
        database.create_payout_table()
    
    logger.info('Starting league event download run')

    # Track already downloaded events so we only download new files.
    imported_urls = database.fetch_imported_event_urls()
    downloaded_files = []
    imported_dir = Path(__file__).parent / 'exports' / 'Imported'

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
                        result = scrape_udisc.download_event_data(export_url)
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
                            
    logger.info(f"Finished downloading files. Starting import of {len(downloaded_files)} new files.")

    for league_id, downloaded_file in downloaded_files:
        try:
            event_id = database.import_downloaded_file(league_id, downloaded_file)
            logger.info(
                f"Imported file into events/raw_scores/hole_scores (event_id={event_id}): {downloaded_file.filename}"
            )

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

    logger.info('Finished importing files.')

    # Run in dependency order: handicaps first, then final scores (which joins
    # handicaps), then views (which select from final_scores).
    for label, path in [
        ('handicap', HANDICAPS_SQL_PATH),
        ('final scores', FINAL_SCORES_SQL_PATH),
        ('views', VIEWS_SQL_PATH),
    ]:
        try:
            logger.info(f"Running {label} script: {path}")
            database.execute_sql_script(path)
            logger.info(f"Finished {label} update.")
        except Exception as error:
            logger.error(f"Failed running {label} script ({path}): {error}")


if __name__ == "__main__":
    main()