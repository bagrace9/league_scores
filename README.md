# League Scores Pipeline

Automated Python + BigQuery pipeline for collecting league event data from UDisc exports, importing raw scoring data, calculating handicaps and payouts, and producing query-ready season standings views.

This project is designed as a practical, production-style ETL workflow with idempotent imports, SQL-driven scoring logic, and deployment-ready scheduling.

## Why This Project

I built this for the leagues I play in so I could automate weekly standings, payouts, and handicap updates without manual spreadsheet work:

- Python application orchestration
- SQL schema design and transformation logic
- Idempotent data ingestion patterns
- Operational concerns (logging, scheduling, deployment)

## Core Features

- Scrapes league event links from UDisc schedule pages
- Discovers leaderboard export URLs and downloads spreadsheets concurrently
- Prevents duplicate imports using persisted `export_url` history
- Synchronizes working table schemas with `_template` definitions automatically
- Imports event, player, and hole-level data into normalized tables
- Automatically prunes orphaned scores if parent events are removed
- Applies per-event overrides (handicap exclusions, points multipliers, buy-in overrides)
- Rebuilds handicaps using configurable league rules
- Rebuilds adjusted scores with place, points, and payout calculations
- Creates reporting tables for current-season analytics
- Archives imported files to GCS or local disk, or deletes them after import
- Logs each run to stdout

## Tech Stack

- Python 3
- BigQuery
- pandas + openpyxl for spreadsheet parsing
- requests + BeautifulSoup for scraping
- google-cloud-bigquery for BigQuery access
- google-cloud-storage for optional GCS file archiving and remote config
- pyarrow + pandas-gbq for BigQuery data transfer
- pytest for automated testing

## Project Structure

- `main.py`: pipeline entry point and orchestration
- `database.py`: database access and write/read operations
- `scrape_udisc.py`: web scraping and file download logic
- `file.py`: file metadata parsing and file system/GCS operations
- `utils.py`: URL parsing and formatting helpers
- `logger.py`: logging configuration
- `league_bootstrap.py`: optional league seeding from config file
- `config.py`: configuration loading (env vars, local file, or GCS)
- `sql/`: schema and transformation scripts
- `config/`: database config templates and example league configs
- `tests/`: pytest unit test suite
- `logs/`: runtime logs (if redirected from stdout)
- `exports/`: downloaded and imported spreadsheet files

## Data Pipeline Flow

1. Ensure template tables exist and synchronize permanent table schemas with templates (drop/recreate if schema has changed)
2. Drop template tables after sync
3. Prune orphaned scores from child tables
4. Ensure payout lookup table exists
5. Read configured leagues from DB
6. If no leagues exist, optionally bootstrap from `config/league_configs.json`
7. Scrape event links from each league's schedule pages (paginated)
8. For each event link, find leaderboard export URLs concurrently (ThreadPoolExecutor)
9. Skip export URLs already imported
10. Download new exports
11. Parse and prepare event, raw score, and hole score rows
12. Bulk import into `events`, `raw_scores`, and `hole_scores`
13. Archive, move, or delete imported files based on storage config
14. Apply per-event overrides from the `event_updates` table
15. Rebuild `handicaps`
16. Rebuild `adjusted_scores`
17. Recreate season reporting tables (`season_event_summary`, `season_players_summary`, `season_log`, `season_hole_scores_summary`)

## Database Configuration

The app resolves configuration in the following priority order (last wins):

1. GCS config file — if `DB_CONFIG_GCS_URI` env var is set, or `config_path` starts with `gs://`
2. Local config file — defaults to `config/db_config.txt`
3. Individual environment variables — override any file-sourced values

### Required settings

- `GCP_PROJECT_ID`
- `BIGQUERY_DATASET`

### Optional settings

- `BIGQUERY_LOCATION` — BigQuery dataset location, defaults to `US`
- `GOOGLE_APPLICATION_CREDENTIALS` — path to service account JSON key file
- `GCS_BUCKET` — GCS bucket for remote config and file archiving
- `ARCHIVE_IMPORTED_FILES` — whether to archive files after import (`true`/`false`, defaults to `true`)
- `LEAGUES_BOOTSTRAP_PATH` — path to league bootstrap JSON (local or `gs://` URI)
- `DB_CONFIG_GCS_URI` — GCS URI of a remote `db_config.txt` to load at startup

### Option 1: Environment Variables (recommended for production)

Set settings in your runtime environment (or via systemd `EnvironmentFile`).

### Option 2: Local Config File

Copy and edit:

- `config/example_db_config.txt` -> `config/db_config.txt`

### Option 3: GCS Config File

Set `DB_CONFIG_GCS_URI=gs://your-bucket/config/db_config.txt` in the environment. The app will download and parse the config file from GCS on startup.

## Optional League Bootstrap Config

If the `leagues` table is empty, the app will look for a league bootstrap JSON file. The default location is `config/league_configs.json`, which can be overridden with `LEAGUES_BOOTSTRAP_PATH`.

If a `GCS_BUCKET` is configured and `LEAGUES_BOOTSTRAP_PATH` is a relative path, the file will be read from GCS at `gs://{GCS_BUCKET}/{LEAGUES_BOOTSTRAP_PATH}`.

If the file exists and contains valid league entries, those leagues are inserted before scraping starts. You can use `config/example_league_configs.json` as a template.

This file is optional and only used when there are no leagues in the DB.

## Running Tests

```bash
pytest tests/
```

The test suite covers configuration loading, URL parsing, file domain logic, scraping utilities, and database helper functions. BigQuery-dependent functions are covered by integration tests (not included here) and require a live GCP connection.

## Local Development Setup

```bash
python -m venv .venv
# Windows PowerShell
.\.venv\Scripts\Activate.ps1
# Linux/macOS
source .venv/bin/activate

pip install --upgrade pip
pip install -r requirements.txt
python main.py
```

## Running in Production (LXC / VM)

Recommended model:

- Unprivileged Debian LXC
- Dedicated app user
- Python virtual environment
- systemd service + timer (daily or hourly)
- Environment file for DB credentials

High-level run command:

```bash
/opt/league-scores/app/.venv/bin/python /opt/league-scores/app/main.py
```

## Operational Notes

- Imports are idempotent by `export_url`
- Unfinished events (end date in the future) have their metadata recorded but scores are not imported until the event is complete
- SQL scripts are executed in dependency order: handicaps → adjusted_scores → summary tables
- Logs are written to stdout; redirect to a file if persistent log storage is needed
- Concurrent downloads use a `ThreadPoolExecutor` with up to 10 workers

## Example Use Cases

- League standings dashboards
- Weekly payout calculations
- Historical score analysis by division/player
- Handicap trend tracking

## Engineering Highlights

- Designed and implemented a repeatable ETL-style pipeline from web source to relational tables
- Built idempotent ingestion logic to avoid duplicate event processing
- Implemented concurrent event scraping and downloading with `ThreadPoolExecutor`
- Implemented an automated schema evolution strategy using DDL comparison of template tables
- Implemented SQL-based analytics transformations (rank/place/points/payout) with window functions
- Added environment-aware configuration handling with GCS, local file, and env var resolution
- Structured code into domain-focused modules with operational logging and deployment readiness
- Unit test suite with 145 tests covering parsing, config, scraping, and data transformation logic

## Security Notes

- Do not commit real credentials
- Use environment variables or secret-managed files in deployment
- Use least-privilege DB roles for runtime access

## Future Improvements

- Add migration tooling for schema versioning (for example, Alembic)
- Add CI pipeline checks (lint, tests, compile, packaging)
- Add monitoring/alerts for failed scheduled runs
- Add integration tests for BigQuery-dependent functions

## License

This project is provided as-is for personal and community league operations.
