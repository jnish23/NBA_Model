import pandas as pd
from selenium import webdriver
from time import sleep

def avg_data_for_model(df, window_size=10, min_periods=5):
    """Computes rolling average for each team's game stats over last n games where n is the window_size (default=10)"""
    df = df.copy()

    df = df.drop(columns = ['SEASON_YEAR_opp', 'SEASON_ID_opp', 
                            'TEAM_ID_opp', 'TEAM_ABBREVIATION_opp',
                            'TEAM_NAME_opp', 'GAME_DATE_opp', 
                            'MATCHUP_opp', 'WL_opp', 'HOME_GAME_opp',
                           'point_diff_opp'])

    team_dfs = []
    for season in df['SEASON_YEAR_team'].unique():
        season_df = df.loc[df['SEASON_YEAR_team'] == season]
        for team in df['TEAM_ABBREVIATION_team'].unique():
            team_df = season_df.loc[season_df['TEAM_ABBREVIATION_team'] == team].sort_values('GAME_DATE_team')
            team_df.iloc[:, 12:] = team_df.iloc[:, 12:].rolling(10, min_periods=min_periods).mean()
            team_dfs.append(team_df)

    new_df = pd.concat(team_dfs)
    new_df = new_df.reset_index(drop=True)
        
    return new_df




def get_days_spreads(date):
    """INPUTS
    date: "yyyy-mm-dd"
    OUPUTS: dataframe with game spreads
    """
    gm_date = []
    away_teams = []
    home_teams = []
    away_spreads = []
    home_spreads = []
    
    web = 'https://www.sportsbookreview.com/betting-odds/nba-basketball/pointspread/?date={}'.format(date)
    path = '../chromedriver.exe'
    driver = webdriver.Chrome(path)
    driver.get(web)
    sleep(2)


    single_row_events = driver.find_elements_by_class_name('eventMarketGridContainer-3QipG')

    num_postponed_events = len(driver.find_elements_by_class_name('eventStatus-3EHqw'))

    num_listed_events = len(single_row_events)
    cutoff = num_listed_events - num_postponed_events

    for event in single_row_events[:cutoff]:

        away_team = event.find_elements_by_class_name('participantBox-3ar9Y')[0].text
        home_team = event.find_elements_by_class_name('participantBox-3ar9Y')[1].text
        away_teams.append(away_team)
        home_teams.append(home_team)
        gm_date.append(date)

        spreads = event.find_elements_by_class_name('pointer-2j4Dk')
        away_lines = []
        home_lines = []
        for i in range(len(spreads)):    
            if i % 2 == 0:
                away_lines.append(spreads[i].text)
            else:
                home_lines.append(spreads[i].text)
        away_spreads.append(away_lines)
        home_spreads.append(home_lines)

    driver.quit()

    todays_spreads = pd.DataFrame({'away_team':away_teams,
                      'home_team':home_teams,
                       'game_date':gm_date,
                      'away_spread':away_spreads,
                      'home_spread':home_spreads})

    for col in todays_spreads[['away_spread', 'home_spread']].columns:
        todays_spreads[col] = todays_spreads[col].astype(str)
        todays_spreads[col] = todays_spreads[col].str.replace("[", "")
        todays_spreads[col] = todays_spreads[col].str.replace("]", "")
        todays_spreads[col] = todays_spreads[col].str.strip()
        
    return todays_spreads