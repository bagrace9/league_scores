import pandas as pd
import os

def write_division_to_sheet(writer, df_start_date, handicap_enabled=False):
    # Write division scores to a worksheet
    worksheet_name = df_start_date['start_date_str'].max()
    worksheet = writer.book.add_worksheet(worksheet_name)
    worksheet.write(0, 0, max(df_start_date['event']))
    startrow = 1
    divisions = df_start_date['division'].unique().tolist()
    for division in divisions:
        worksheet.write(startrow, 0, division)
        startrow += 1
        df_division = df_start_date[df_start_date['division'] == division]
        if handicap_enabled:
            df_division = df_division[['player', 'score', 'handicap' , 'adjusted_score', 'week_place', 'week_points', 'season_points']]
            df_division.columns = ['Player', 'Score', 'Handicap', 'Adjusted Score', 'Place', 'Week Points', 'Season Points']
        else:
            df_division = df_division[['player', 'score', 'week_place', 'week_points', 'season_points']]
            df_division.columns = ['Player', 'Score', 'Place', 'Week Points', 'Season Points']
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

def save_to_excel(df_season_scores, full_path, handicap_enabled):
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
            write_division_to_sheet(writer, df_start_date, handicap_enabled)

