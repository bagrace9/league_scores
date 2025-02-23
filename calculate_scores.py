import pandas as pd
from scrape_udisc import get_event_links, get_scores

def calculate_league_scores(df_scores, handicap_enabled):
    # Convert date strings to datetime objects
    df_scores['start_date'] = pd.to_datetime(df_scores['start_date_str'], format='%b %d, %Y')
    df_scores['end_date'] = pd.to_datetime(df_scores['end_date_str'], format='%b %d, %Y')

    # Calculate handicap if enabled
    if handicap_enabled:
        df_scores = calculate_handicap(df_scores)
    else:
        df_scores['handicap'] = 0

    # Filter scores for the most recent year
    max_year = df_scores['start_date'].dt.year.max()
    df_scores = df_scores[df_scores['start_date'].dt.year == max_year]

    # Calculate adjusted scores
    df_scores['score'] = df_scores['score'].astype(int)
    df_scores['adjusted_score'] = df_scores['score'] - df_scores['handicap']

    # Calculate weekly points
    df_scores['week_points'] = df_scores.groupby(['start_date', 'division'])['adjusted_score'].rank(ascending=False, method='max')

    # Double points for final events
    df_scores.loc[df_scores['type'] == 'final', 'week_points'] *= 2

    # Calculate weekly place and season points
    df_scores['week_place'] = df_scores.groupby(['start_date', 'division'])['adjusted_score'].rank(ascending=True, method='min')
    df_scores['season_points'] = df_scores.apply(
        lambda row: df_scores[(df_scores['start_date'] <= row['start_date']) & 
                              (df_scores['player'] == row['player']) & 
                              (df_scores['division'] == row['division'])]['week_points'].sum(), axis=1)
    return df_scores

def calculate_handicap(df_scores):
    # Sort scores by player and start date
    df_scores = df_scores.sort_values(by=['player', 'start_date'])
    df_scores['handicap_avg'] = df_scores.groupby('player')['score'].rolling(window=3, min_periods=1).mean().reset_index(level=0, drop=True)
    df_scores['handicap_score_cnt'] = df_scores.apply(
        lambda row: df_scores[(df_scores['player'] == row['player']) & 
                              (df_scores['start_date'] < row['start_date'])].shape[0], axis=1)

    # Calculate handicap based on average scores
    df_scores['handicap'] = 0.0
    df_scores.loc[df_scores['handicap_score_cnt'] >= 3, 'handicap'] = (df_scores['handicap_avg'] - 48) * 0.8

    # Ensure handicap is non-negative and rounded
    df_scores['handicap'] = df_scores['handicap'].apply(lambda x: max(x, 0))
    df_scores['handicap'] = df_scores['handicap'].round()
    return df_scores

