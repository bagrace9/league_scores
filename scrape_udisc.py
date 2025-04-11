import requests
from bs4 import BeautifulSoup
import pandas as pd
import sqlite3
import database


# Fetch page content
def fetch_page_content(url):
    response = requests.get(url)
    response.raise_for_status()
    return BeautifulSoup(response.content, 'html.parser')

# Parse event details
def parse_event_details(soup):
    headings = soup.find_all(['h1', 'h2', 'h3', 'h4', 'h5', 'h6'])
    if len(headings) < 5:
        return None, None
    event = headings[0].text.strip()
    divisions = [heading.text.strip() for heading in headings[3:-1]]
    return event, divisions

# Parse league dates
def parse_league_dates(soup):
    league_date = soup.find('div', class_='text-subtle text-sm md:text-base').text.strip()
    if ' - ' in league_date:
        start_date_str, end_date_str = league_date.split(' - ')
        start_date_str = start_date_str + ', ' + end_date_str[-4:]
    else:
        start_date_str = league_date
        end_date_str = league_date
    return start_date_str, end_date_str

# Parse scores from the soup object and return a DataFrame
def parse_scores(soup, event, divisions, start_date_str, end_date_str):
    df_raw_scores = pd.DataFrame(columns=['start_date_str', 'end_date_str', 'event', 'division', 'player', 'score'])
    column_data = soup.find_all('tr')
    for row in column_data:
        row_data = row.find_all('td')
        player_score = [data.text.strip() for data in row_data]
        if player_score == []:
            div = divisions.pop(0)
        else:
            curr_score = pd.DataFrame({'start_date_str': [start_date_str], 'end_date_str': [end_date_str], 'event': [event], 'division': [div], 'player': [player_score[1]], 'score': [player_score[-1]]})
            df_raw_scores = pd.concat([df_raw_scores, curr_score], ignore_index=True)
    return df_raw_scores

# Get scores from multiple URLs and combine them into a single DataFrame
def get_scores(weeks):
    df_raw_scores = pd.DataFrame(columns=['start_date_str', 'end_date_str', 'event', 'division', 'player', 'score'])
    for _, week in weeks.iterrows():
        url = week['url']  # Assuming the DataFrame has a column named 'url'
        
        soup = fetch_page_content(url)
        if not soup:
            continue
        event, divisions = parse_event_details(soup)
        if event is None:
            continue
        start_date_str, end_date_str = parse_league_dates(soup)
        if not start_date_str or not end_date_str:
            continue

        df_week_scores = parse_scores(soup, event, divisions, start_date_str, end_date_str)
        df_week_scores['points_multiplyer'] = week['points_multiplyer']
        df_week_scores['handicap_excluded'] = week['handicap_excluded']

        df_raw_scores = pd.concat([df_raw_scores, df_week_scores], ignore_index=True)

    return df_raw_scores

# Get event links by iterating through pages and filtering relevant links
def get_event_links(url,lookback_year):
    links = []
    last_len = -1
    page = 1
    while len(links) > last_len:
        page_url = url + '/schedule?page=' + str(page)
        last_len = len(links)
        soup = fetch_page_content(page_url)
        if not soup:
            break
        for a_tag in soup.find_all('a', href=True):
            year_span = a_tag.find_next('span', class_='text-subtle ml-2 text-sm font-normal')
            if year_span:
                link_year = year_span.text.strip()
            link = a_tag['href']
            if link.startswith('/events') and link.endswith('/leaderboard') and int(link_year) >= int(lookback_year):
                links.append('https://udisc.com' + link)
        page += 1
    return links



def save_to_database(df_scores, league_id, db_path="league_scores.db"):
  
    # Connect to SQLite database (or create it if it doesn't exist)
    conn = sqlite3.connect(db_path)

    # Remove rows where the player ends with "DNF" (case-sensitive)
    df_scores = df_scores[~df_scores['player'].str.endswith('DNF', na=False)]
    
    df_scores['league_id'] = league_id
    df_scores['start_date'] = pd.to_datetime(df_scores['start_date_str'], errors='coerce')
    df_scores['end_date'] = pd.to_datetime(df_scores['end_date_str'], errors='coerce')

    df_scores.to_sql('impt_raw_scores', conn, if_exists='append', index=False)
    
    # Close the connection
    conn.close()


def scrape(weeks,league_id):   

    database.execute_sql("DELETE FROM impt_raw_scores")
    
    df_scores = get_scores(weeks)

    # Save the results to a SQLite database
    save_to_database(df_scores,league_id)

    return df_scores

