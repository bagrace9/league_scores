import pandas as pd
import os
import sqlite3
from datetime import datetime
import calc_payouts

def write_division_to_sheet(writer, df_start_date, league_id, handicap_enabled=False):
    # Ensure start_date is stripped of time
    df_start_date['start_date'] = pd.to_datetime(df_start_date['start_date']).dt.date
    # Format the worksheet name as mm/dd/yyyy
    worksheet_name = df_start_date['start_date'].max().strftime('%m.%d.%Y')
    worksheet = writer.book.add_worksheet(worksheet_name)
    worksheet.write(0, 0, max(df_start_date['event']))
    startrow = 1
    divisions = df_start_date['division'].unique().tolist()
    for division in divisions:
        worksheet.write(startrow, 0, division)
        startrow += 1
        df_division = df_start_date[df_start_date['division'] == division]

        conn = connect_to_sqlite()
        if conn is None:
            raise Exception("Failed to connect to the database.")

        league_cash_percentage = 0
        try:
            with conn:
                cursor = conn.execute("SELECT league_cash_percentage FROM leagues WHERE id = ?", (league_id,))
                result = cursor.fetchone()
                if result:
                    league_cash_percentage = result[0]
        finally:
            conn.close()

        payouts = calc_payouts.weighted_payouts(len(df_division), 4, league_cash_percentage)
        df_division['payout'] = 0.0
        
        for place, group in df_division.groupby('place'):
            payout_value = sum(payouts[:len(group)]) / len(group)
            df_division.loc[group.index, 'payout'] = payout_value
            payouts = payouts[len(group):]
        
        df_division['payout'] = df_division['payout'].round(0)

        if handicap_enabled:
            df_division = df_division[['player', 'raw_score', 'handicap', 'adjusted_score', 'place', 'points', 'season_points', 'payout']]
            df_division.columns = ['Player', 'Score', 'Handicap', 'Adjusted Score', 'Place', 'Week Points', 'Season Points', 'Payout']
        else:
            df_division = df_division[['player', 'raw_score', 'place', 'points', 'season_points', 'payout']]
            df_division.columns = ['Player', 'Score', 'Place', 'Week Points', 'Season Points', 'Payout']
            print(df_division)
        df_division = df_division.sort_values(by=['Week Points'], ascending=False)
        df_division.to_excel(writer, startrow=startrow, startcol=0, sheet_name=worksheet_name, index=False)
        startrow += len(df_division) + 2

def write_total_points_to_sheet(writer, df_season_scores):
    # Write total points to a worksheet
    worksheet = writer.book.add_worksheet('Total Points')
    startcol = 0
    divisions = df_season_scores['division'].unique().tolist()
    for division in divisions:
        worksheet.write(1, startcol, division)
        df_div_total_points = df_season_scores[df_season_scores['division'] == division]
        df_div_total_points = df_div_total_points.sort_values(by=['start_date'], ascending=False).drop_duplicates(subset=['player'], keep='first')
        df_div_total_points = df_div_total_points.copy()
        df_div_total_points.loc[:, 'place'] = df_div_total_points['season_points'].rank(ascending=False, method='min')
        df_div_total_points = df_div_total_points[['place', 'player', 'season_points']]
        df_div_total_points.columns = ['Place', 'Player', 'Points']
        df_div_total_points = df_div_total_points.sort_values(by=['Points'], ascending=False)
        df_div_total_points.to_excel(writer, startrow=2, startcol=startcol, sheet_name='Total Points', index=False)
        startcol += 5

def save_to_excel(df_season_scores, full_path, league_id, handicap_enabled):
    # Ensure the results directory exists
    results_dir = os.path.dirname(full_path)
    os.makedirs(results_dir, exist_ok=True)        
    
    # Write scores to an Excel file
    distinct_start_dates = df_season_scores['start_date'].unique().tolist()
    distinct_start_dates.sort(reverse=True)
    with pd.ExcelWriter(full_path, engine="xlsxwriter") as writer:
        write_total_points_to_sheet(writer, df_season_scores)
        for start_date in distinct_start_dates:
            df_start_date = df_season_scores[df_season_scores['start_date'] == start_date]
            write_division_to_sheet(writer, df_start_date, league_id, handicap_enabled)

def connect_to_sqlite():
    """Establish a connection to the SQLite database."""
    try:
        conn = sqlite3.connect('league_scores.db')
        return conn
    except sqlite3.Error as e:
        print(f"Error connecting to SQLite database: {e}")
        return None

def get_spreadsheet_data(league_id):
    # Connect to the database
    conn = connect_to_sqlite()

    # Fetch league name and handicap_enabled value from the database
    query_league = "SELECT league_name, league_is_handicap FROM leagues WHERE id = ?"
    league_data = pd.read_sql_query(query_league, conn, params=(league_id,)).iloc[0]
    league_name = league_data['league_name'].replace(" ", "_")
    handicap_enabled = league_data['league_is_handicap']

    # Query the league data
    query = """
    SELECT *    
    FROM scores s
    JOIN leagues l
        ON s.league_id = l.id
    WHERE s.league_id = ?
     and year = strftime('%Y', datetime('now'))
    """
    df_season_scores = pd.read_sql_query(query, conn, params=(league_id,))

    # Close the database connection
    conn.close()

    # Define the output file path using league name and current date
    current_date = datetime.now().strftime('%Y_%m_%d')
    output_file = f"results/{league_name}_{current_date}_scores.xlsx"

    # Call save_to_excel
    save_to_excel(df_season_scores, output_file, league_id, handicap_enabled)
