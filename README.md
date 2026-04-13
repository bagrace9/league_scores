# League Scores Pipeline

Automated Python + PostgreSQL pipeline for collecting league event data from UDisc exports, importing raw scoring data, calculating handicaps and payouts, and producing query-ready season standings views.

This project is designed as a practical, production-style ETL workflow with idempotent imports, SQL-driven scoring logic, and deployment-ready scheduling.

## Why This Project

I built this for the leagues I play in so I could automate weekly standings, payouts, and handicap updates without manual spreadsheet work:

- Python application orchestration
- PostgreSQL schema design and transformation SQL
- Idempotent data ingestion patterns
- Operational concerns (logging, scheduling, deployment)

## Core Features

- Scrapes league event links from UDisc schedule pages
- Discovers leaderboard export URLs and downloads spreadsheets
- Prevents duplicate imports using persisted `export_url` history
- Imports event, player, and hole-level data into normalized tables
- Rebuilds handicaps using configurable league rules
- Rebuilds final scores with place, points, and payout calculations
- Creates reporting views for current-season analytics
- Logs each run to both console and timestamped log files

## Tech Stack

- Python 3
- PostgreSQL
- pandas + openpyxl for spreadsheet parsing
- requests + BeautifulSoup for scraping
- psycopg2 for DB access

## Project Structure

- `main.py`: pipeline entry point and orchestration
- `database.py`: database access and write/read operations
- `scrape_udisc.py`: web scraping and file download logic
- `file.py`: file metadata parsing and file operations
- `sql/`: schema and transformation scripts
- `config/`: database config templates
- `logs/`: runtime logs
- `exports/`: downloaded and imported spreadsheet files

## Data Pipeline Flow

1. Ensure core tables exist
2. Ensure payout lookup table exists
3. Read configured leagues from DB
4. Scrape event/leaderboard URLs
5. Skip URLs already imported
6. Download new exports
7. Import rows into `events`, `raw_scores`, and `hole_scores`
8. Rebuild `handicaps`
9. Rebuild `final_scores`
10. Recreate reporting views

## Database Configuration

The app supports **environment variables first**, with fallback to a local config file.

Required settings:

- `DB_HOST`
- `DB_PORT`
- `DB_NAME`
- `DB_USER`
- `DB_PASSWORD`

### Option 1: Environment Variables (recommended)

Set DB settings in your runtime environment (or via systemd `EnvironmentFile`).

### Option 2: Local Config File

Copy and edit:

- `config/example_db_config.txt` -> `config/db_config.txt`

## Local Development Setup

```bash
python -m venv .venv
# Windows PowerShell
.\.venv\Scripts\Activate.ps1
# Linux/macOS
# source .venv/bin/activate

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
- SQL scripts are executed in dependency order: handicaps -> final_scores -> views
- Logs are written to `logs/league_scores_YYYYMMDD_HHMMSS.log`

## Example Use Cases

- League standings dashboards
- Weekly payout calculations
- Historical score analysis by division/player
- Handicap trend tracking

## Engineering Highlights

- Designed and implemented a repeatable ETL-style pipeline from web source to relational tables
- Built idempotent ingestion logic to avoid duplicate event processing
- Implemented SQL-based analytics transformations (rank/place/points/payout) with window functions
- Added environment-aware configuration handling suitable for local and production deployments
- Structured code into domain-focused modules with operational logging and deployment readiness

## Security Notes

- Do not commit real credentials
- Use environment variables or secret-managed files in deployment
- Use least-privilege DB roles for runtime access

## Future Improvements

- Add automated test coverage for parsing and SQL transform validation
- Add migration tooling for schema versioning (for example, Alembic)
- Add CI pipeline checks (lint, tests, compile, packaging)
- Add monitoring/alerts for failed scheduled runs

## License

This project is provided as-is for personal and community league operations.
