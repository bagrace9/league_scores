import configparser
import json
import discord_utils
from calculate_scores import calculate_league_scores
import scrape_udisc
from write_to_excel import save_to_excel

def load_leagues(leagues_file):
    # Load league configurations from a JSON file
    with open(leagues_file, 'r') as f:
        leagues = json.load(f)['leagues']
        for league in leagues:
            league['url'] = f"https://udisc.com/leagues/{league['id']}/schedule"
    return leagues

def scrape_and_upload_league_scores(league_url, file_name, channel_id, handicap_enabled):
    # Scrape and upload league scores
    year = scrape_udisc.find_first_year(league_url)
    if not year:
        return
    
    df_raw_scores = scrape_udisc.scrape(league_url, year, handicap_enabled)

    df_full_scores = calculate_league_scores(df_raw_scores, handicap_enabled)

    # Filter scores from the current year
    df_season_scores = df_full_scores[df_full_scores['start_date'].dt.year == int(year)]
    results_path = 'results/' + file_name
    save_to_excel(df_season_scores, results_path, handicap_enabled)
    
    if ENABLE_DISCORD_NOTIFICATIONS:
        discord_utils.upload_scores_to_discord(results_path, channel_id)

if __name__ == "__main__":
    # Read configuration
    config = configparser.ConfigParser()
    config.read('config.ini')
    DISCORD_TOKEN = config['discord']['DISCORD_TOKEN']
    ENABLE_DISCORD_NOTIFICATIONS = config['discord'].getboolean('ENABLE_DISCORD_NOTIFICATIONS')
    LEAGUES_FILE = config['files']['LEAGUES_FILE']
    
    # Load leagues and process scores
    leagues = load_leagues(LEAGUES_FILE)
    
    for league in leagues:
        league_url = league['url']
        file_name = league['file_name']
        channel_id = league['discord_channel_id']
        handicap_enabled = league['handicap_enabled']
        scrape_and_upload_league_scores(league_url, file_name, channel_id, handicap_enabled)