import requests
from bs4 import BeautifulSoup
import pandas as pd

# Fetch page content
def fetch_page_content(url):
    response = requests.get(url)
    response.raise_for_status()
    return BeautifulSoup(response.content, 'html.parser')

# Extract year from the page content
def extract_year(soup):
    league_year = soup.find('span', class_='text-subtle ml-2 text-sm font-normal').text.strip()
    return league_year

# Find the first year from the URL
def find_first_year(url):
    soup = fetch_page_content(url)
    if soup:
        return extract_year(soup)
    return None

# Parse event details
def parse_event_details(soup):
    headings = soup.find_all(['h1', 'h2', 'h3', 'h4', 'h5', 'h6'])
    if len(headings) < 5:
        return None, None
    event = headings[0].text.strip()
    divisions = [heading.text.strip() for heading in headings[3:-1]]
    return event, divisions

# Parse league dates
def parse_league_dates(soup, year):
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
def get_scores(urls, year):
    df_raw_scores = pd.DataFrame(columns=['start_date_str', 'end_date_str', 'event', 'division', 'player', 'score'])
    for url in urls:
        if 'final' in url.lower() or 'finale' in url.lower():
            type = "final"
        elif 'week' in url.lower() or 'flex' in url.lower():
            type = 'week'
        else:
            continue
        
        soup = fetch_page_content(url)
        if not soup:
            continue
        event, divisions = parse_event_details(soup)
        if event is None:
            continue
        start_date_str, end_date_str = parse_league_dates(soup, year)
        if not start_date_str or not end_date_str:
            continue

        df_week_scores = parse_scores(soup, event, divisions, start_date_str, end_date_str )

        df_week_scores['type'] = type

        df_raw_scores = pd.concat([df_raw_scores, df_week_scores], ignore_index=True)

    return df_raw_scores

# Get event links by iterating through pages and filtering relevant links
def get_event_links(url, year, handicap_enabled):
    if handicap_enabled:
        lookback_year = str(int(year) - 1)
    else:
        lookback_year = year
    links = []
    last_len = -1
    page = 1
    while len(links) > last_len:
        page_url = url + '?page=' + str(page)
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

def scrape(league_url, year, handicap_enabled):    
    event_links = get_event_links(league_url, year, handicap_enabled)
    df_scores = get_scores(event_links, year)
    return df_scores

