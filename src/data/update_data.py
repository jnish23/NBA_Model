import pandas as pd
from tqdm import tqdm
from nba_api.stats.endpoints import leaguegamelog
from nba_api.stats.endpoints import playergamelogs
from nba_api.stats.endpoints import boxscoreadvancedv2
from nba_api.stats.endpoints import boxscorescoringv2
from nba_api.stats.endpoints import boxscoreplayertrackv2
from nba_api.stats.endpoints import teamgamelogs
from IPython.display import clear_output

import random
from selenium import webdriver
from time import sleep

## Update basic team gamelogs and player gamelogs

def update_team_basic_boxscores(conn, season):
    """
    Updates the basic team gamelogs csv file for the given season
    INPUTS:
    season (str): current season which you will update ie: '2020-21'
    
    """
    table_name = 'team_basic_boxscores'
    dfs = []
    for season_type in ['Regular Season', 'Playoffs']:
        team_gamelogs = leaguegamelog.LeagueGameLog(season=season, season_type_all_star=season_type).get_data_frames()[0]
        dfs.append(team_gamelogs)

    team_gamelogs_full = pd.concat(dfs)    
    team_gamelogs_full['SEASON'] = team_gamelogs_full['SEASON_ID'].str[-4:].astype(int).apply(season_string)
    team_gamelogs_full = team_gamelogs_full.drop(columns=['SEASON_ID', 'VIDEO_AVAILABLE'])
    
    
    team_gamelogs_full.to_sql(table_name, conn, if_exists='append', index=False)
    
    cur = conn.cursor()
    cur.execute(f'''DELETE FROM {table_name} 
                    WHERE rowid NOT IN (SELECT MAX(rowid) FROM {table_name} 
                                        GROUP BY TEAM_NAME, GAME_ID, GAME_DATE, MATCHUP)''')
    conn.commit()
    
    return None


def update_team_advanced_boxscores(conn, season, dates=[]):
    table_name = 'team_advanced_boxscores'
    
    season_str = season_string(season)
    
    game_ids_not_added = []
    
    # Pull the GAME_IDs from my data
    game_ids_in_db = pd.read_sql('''SELECT DISTINCT team_basic_boxscores.GAME_ID FROM team_basic_boxscores
                INNER JOIN team_advanced_boxscores 
                ON team_basic_boxscores.GAME_ID = team_advanced_boxscores.GAME_ID
                AND team_basic_boxscores.TEAM_ID = team_advanced_boxscores.TEAM_ID
                WHERE SEASON = "{}" '''.format(season_str), conn)

    game_ids_in_db = game_ids_in_db['GAME_ID'].tolist()
    
    missing_game_ids = []
    if len(dates) != 0:
        for date in dates:
            gamelogs = leaguegamelog.LeagueGameLog(
                season=season_str, date_from_nullable=date, date_to_nullable=date).get_data_frames()[0]
            missing_game_ids.extend(gamelogs['GAME_ID'].unique())
            
    else:        
        # get up to date GAME_IDs
        to_date_game_ids = []
        for season_type in ['Regular Season', 'Playoffs']:
            to_date_gamelogs = leaguegamelog.LeagueGameLog(season=season_str, season_type_all_star=season_type).get_data_frames()[0]
            to_date_game_ids.extend(to_date_gamelogs['GAME_ID'].unique())

        # See which game_ids are missing
        missing_game_ids = set(to_date_game_ids) - set(game_ids_in_db)
        
    num_games_updated = len(missing_game_ids)
    print("num_games_updated:", num_games_updated)
    
    if num_games_updated == 0:
        print("All team advanced boxscores up to date in season {}".format(season_str))
        return None
        
    for game_id in tqdm(missing_game_ids, desc='progress'):
        try:
            boxscores = boxscoreadvancedv2.BoxScoreAdvancedV2(game_id).get_data_frames()[1]
            boxscores.to_sql(table_name, conn, if_exists='append', index=False)
            sleep(2)
        except:
            game_ids_not_added.append(game_id)  
    
    cur = conn.cursor()
    cur.execute('DELETE FROM {} WHERE rowid NOT IN (SELECT max(rowid) FROM {} GROUP BY TEAM_ID, GAME_ID)'.format(table_name, table_name))
    conn.commit()
    
    return game_ids_not_added


def update_team_scoring_boxscores(conn, season, dates=[]):
    table_name = 'team_scoring_boxscores'

    season_str = season_string(season)

    game_ids_not_added = []

    # Pull the GAME_IDs from my data
    game_ids_in_db = pd.read_sql(f'''SELECT DISTINCT team_scoring_boxscores.GAME_ID FROM team_basic_boxscores
                INNER JOIN team_scoring_boxscores 
                ON team_basic_boxscores.GAME_ID = team_scoring_boxscores.GAME_ID
                AND team_basic_boxscores.TEAM_ID = team_scoring_boxscores.TEAM_ID
                WHERE SEASON = "{season_str}" ''', conn)

    game_ids_in_db = game_ids_in_db['GAME_ID'].tolist()

    missing_game_ids = []
    if len(dates) != 0:
        for date in dates:
            gamelogs = leaguegamelog.LeagueGameLog(
                season=season_str, date_from_nullable=date, date_to_nullable=date).get_data_frames()[0]
            missing_game_ids.extend(gamelogs['GAME_ID'].unique())

    else:
        # get up to date GAME_IDs
        to_date_game_ids = []
        for season_type in ['Regular Season', 'Playoffs']:
            to_date_gamelogs = leaguegamelog.LeagueGameLog(
                season=season_str, season_type_all_star=season_type).get_data_frames()[0]
            to_date_game_ids.extend(to_date_gamelogs['GAME_ID'].unique())

        # See which game_ids are missing
        missing_game_ids = set(to_date_game_ids) - set(game_ids_in_db)
        
    num_games_updated = len(missing_game_ids)
    print("num_games_updated:", num_games_updated)

    if num_games_updated == 0:
        print("All team advanced boxscores up to date in season {}".format(season_str))
        return None

    for game_id in tqdm(missing_game_ids, desc='progress'):
        try:
            boxscores = boxscorescoringv2.BoxScoreScoringV2(
                game_id).get_data_frames()[1]
            boxscores.to_sql(table_name, conn,
                             if_exists='append', index=False)
            sleep(2)
        except:
            game_ids_not_added.append(game_id)

    cur = conn.cursor()
    cur.execute('DELETE FROM {} WHERE rowid NOT IN (SELECT max(rowid) FROM {} GROUP BY TEAM_ID, GAME_ID)'.format(
        table_name, table_name))
    conn.commit()

    return game_ids_not_added


def update_moneylines(conn, season=2021, custom_dates=[]):
    table_name = 'moneylines'
    # Get current moneyline data

    if len(custom_dates) == 0:
        current_ml_data = pd.read_sql_query(
            "SELECT * FROM moneylines", conn)

        abbr_mapping = {'Boston': 'BOS', 'Portland': 'POR',
                        'L.A. Lakers': 'LAL', 'Brooklyn': 'BKN',
                        'Cleveland': 'CLE', 'Toronto': 'TOR',
                        'Philadelphia': 'PHI', 'Memphis': 'MEM',
                        'Minnesota': 'MIN', 'New Orleans': 'NOP',
                        'Oklahoma City': 'OKC', 'Dallas': 'DAL',
                        'San Antonio': 'SAS', 'Denver': 'DEN',
                        'Golden State': 'GSW', 'L.A. Clippers': 'LAC',
                        'Orlando': 'ORL', 'Utah': 'UTA',
                        'Charlotte': 'CHA', 'Detroit': 'DET',
                        'Miami': 'MIA', 'Phoenix': 'PHX',
                        'Atlanta': 'ATL', 'New York': 'NYK',
                        'Indiana': 'IND', 'Chicago': 'CHI',
                        'Houston': 'HOU', 'Milwaukee': 'MIL',
                        'Sacramento': 'SAC', 'Washington': 'WAS'}

        current_ml_data['HOME_TEAM'] = current_ml_data['HOME_TEAM'].replace(
            abbr_mapping)
        current_ml_data['AWAY_TEAM'] = current_ml_data['AWAY_TEAM'].replace(
            abbr_mapping)

        up_to_date_games = get_season_games(season)

        merged_df = pd.merge(up_to_date_games, current_ml_data, how='left', left_on=[
                             'HOME_TEAM', 'AWAY_TEAM', 'GAME_DATE'], right_on=['HOME_TEAM', 'AWAY_TEAM', 'GM_DATE'])
        
        
        missing_dates = merged_df.loc[merged_df['AWAY_ML'].isnull(), 'GAME_DATE'].unique().tolist()
        
    else:
        missing_dates = custom_dates

    if len(missing_dates) == 0:
        print("moneyline table is up to date")
        return None
        
    seasons = []
    gm_dates = []
    away_teams = []
    home_teams = []
    away_mls = []
    home_mls = []

    for date in tqdm(missing_dates, desc='progress'):
        web = 'https://www.sportsbookreview.com/betting-odds/nba-basketball/money-line/?date={}'.format(
            date)
        path = '../chromedriver.exe'
        driver = webdriver.Chrome(path)
        driver.get(web)
        sleep(random.randint(1, 2))

        try:
            single_row_events = driver.find_elements_by_class_name(
                'eventMarketGridContainer-3QipG')

        except:
            print("No Data for {}".format(date))
            dates_with_no_data.append(date)
            continue

        num_postponed_events = len(
            driver.find_elements_by_class_name('eventStatus-3EHqw'))

        num_listed_events = len(single_row_events)
        cutoff = num_listed_events - num_postponed_events

        for event in single_row_events[:cutoff]:
            seasons.append(season_string(season))

            away_team = event.find_elements_by_class_name(
                'participantBox-3ar9Y')[0].text
            home_team = event.find_elements_by_class_name(
                'participantBox-3ar9Y')[1].text

            away_teams.append(away_team)
            home_teams.append(home_team)

            gm_dates.append(date)

            mls = event.find_elements_by_class_name('pointer-2j4Dk')

            away_moneyline = []
            home_moneyline = []

            for i, ml in enumerate(mls):
                if i % 2 == 0:
                    away_moneyline.append(ml.text)
                else:
                    home_moneyline.append(ml.text)

            away_moneyline = ",".join(away_moneyline)
            home_moneyline = ",".join(home_moneyline)

            away_mls.append(away_moneyline)
            home_mls.append(home_moneyline)

        driver.quit()
        sleep(random.randint(2, 3))

    clear_output(wait=True)

    df = pd.DataFrame({'SEASON': seasons,
                       'GM_DATE': gm_dates,
                       'AWAY_TEAM': away_teams,
                      'HOME_TEAM': home_teams,
                       'AWAY_ML': away_mls,
                       'HOME_ML': home_mls,
                       })

    df = df.sort_values(['GM_DATE']).reset_index(drop=True)

    df.to_sql(table_name, conn, if_exists='append', index=False)

    cur = conn.cursor()
    cur.execute('''DELETE FROM moneylines 
                    WHERE rowid NOT IN (SELECT MAX(rowid) FROM moneylines
                                        GROUP BY GM_DATE, AWAY_TEAM, HOME_TEAM, AWAY_ML, HOME_ML)''')
    conn.commit()

    return None


def update_spreads(conn, season, custom_dates=[]):
    table_name = 'spreads'
    # Get current spread data
    
    if len(custom_dates) == 0:
        current_spread_data = pd.read_sql_query(
            "SELECT * FROM spreads", conn)

        abbr_mapping = {'Boston': 'BOS', 'Portland': 'POR',
                        'L.A. Lakers': 'LAL', 'Brooklyn': 'BKN',
                        'Cleveland': 'CLE', 'Toronto': 'TOR',
                        'Philadelphia': 'PHI', 'Memphis': 'MEM',
                        'Minnesota': 'MIN', 'New Orleans': 'NOP',
                        'Oklahoma City': 'OKC', 'Dallas': 'DAL',
                        'San Antonio': 'SAS', 'Denver': 'DEN',
                        'Golden State': 'GSW', 'L.A. Clippers': 'LAC',
                        'Orlando': 'ORL', 'Utah': 'UTA',
                        'Charlotte': 'CHA', 'Detroit': 'DET',
                        'Miami': 'MIA', 'Phoenix': 'PHX',
                        'Atlanta': 'ATL', 'New York': 'NYK',
                        'Indiana': 'IND', 'Chicago': 'CHI',
                        'Houston': 'HOU', 'Milwaukee': 'MIL',
                        'Sacramento': 'SAC', 'Washington': 'WAS'}

        current_spread_data['HOME_TEAM'] = current_spread_data['HOME_TEAM'].replace(
            abbr_mapping)
        current_spread_data['AWAY_TEAM'] = current_spread_data['AWAY_TEAM'].replace(
            abbr_mapping)

        up_to_date_games = get_season_games(season)

        merged_df = pd.merge(up_to_date_games, current_spread_data, how='left', left_on=[
                             'HOME_TEAM', 'AWAY_TEAM', 'GAME_DATE'], right_on=['HOME_TEAM', 'AWAY_TEAM', 'GM_DATE'])
        
        
        missing_dates = merged_df.loc[merged_df['AWAY_SPREAD'].isnull(), 'GAME_DATE'].unique().tolist()

        print("Updating spreads for {} days".format(len(missing_dates)))

    else:
        missing_dates = custom_dates

        
    if len(missing_dates) == 0:
        print("spreads table is up to date")
        return None 
    
    seasons = []
    gm_dates = []
    away_teams = []
    home_teams = []
    away_scoreboards = []
    home_scoreboards = []
    away_spreads = []
    home_spreads = []
    
    
    for date in tqdm(missing_dates, desc='progress'):
            web = 'https://www.sportsbookreview.com/betting-odds/nba-basketball/?date={}'.format(date)
            path = '../chromedriver.exe'
            driver = webdriver.Chrome(path)
            driver.get(web)
            sleep(random.randint(2,3))

            try:
                single_row_events = driver.find_elements_by_class_name('eventMarketGridContainer-3QipG')
                
            except:
                print("No Data for {}".format(date))
                dates_with_no_data.append(date)
                continue
                
            num_postponed_events = len(driver.find_elements_by_class_name('eventStatus-3EHqw'))

            num_listed_events = len(single_row_events)
            cutoff = num_listed_events - num_postponed_events

            for event in single_row_events[:cutoff]:

                away_team = event.find_elements_by_class_name('participantBox-3ar9Y')[0].text
                home_team = event.find_elements_by_class_name('participantBox-3ar9Y')[1].text
                away_teams.append(away_team)
                home_teams.append(home_team)
                gm_dates.append(date)

                seasons.append(season_string(season))
                
                scoreboard = event.find_elements_by_class_name('scoreboard-1TXQV')

                home_score = []
                away_score = []

                for score in scoreboard:
                    quarters = score.find_elements_by_class_name('scoreboardColumn-2OtpR')
                    for i in range(len(quarters)):
                        scores = quarters[i].text.split('\n')
                        away_score.append(scores[0])
                        home_score.append(scores[1])
                        
                    home_score = ",".join(home_score)
                    away_score = ",".join(away_score)
                    
                    away_scoreboards.append(away_score)
                    home_scoreboards.append(home_score)


                if len(away_scoreboards) != len(away_teams):
                    num_to_add = len(away_teams) - len(away_scoreboards)
                    for i in range(num_to_add):
                        away_scoreboards.append('')
                        home_scoreboards.append('')

                spreads = event.find_elements_by_class_name('pointer-2j4Dk')
                away_lines = []
                home_lines = []
                for i in range(len(spreads)):    
                    if i % 2 == 0:
                        away_lines.append(spreads[i].text)
                    else:
                        home_lines.append(spreads[i].text)
                
                away_lines = ",".join(away_lines)
                home_lines = ",".join(home_lines)
                
                away_spreads.append(away_lines)
                home_spreads.append(home_lines)

                if len(away_spreads) != len(away_teams):
                    num_to_add = len(away_teams) - len(away_spreads)
                    for i in range(num_to_add):
                        away_scoreboards.append('')
                        home_scoreboards.append('')

            driver.quit()
            clear_output(wait=True)

    df = pd.DataFrame({'SEASON':seasons, 
                      'GM_DATE':gm_dates,
                      'AWAY_TEAM':away_teams,
                      'HOME_TEAM':home_teams,
                      'AWAY_SCOREBOARD':away_scoreboards,
                      'HOME_SCOREBOARD':home_scoreboards,
                      'AWAY_SPREAD':away_spreads,
                      'HOME_SPREAD':home_spreads})


    df.to_sql(table_name, conn, if_exists='append', index=False)
    
    cur = conn.cursor()
    cur.execute('''DELETE FROM spreads 
                    WHERE rowid NOT IN (SELECT MAX(rowid) FROM spreads 
                                        GROUP BY GM_DATE, AWAY_TEAM, HOME_TEAM, AWAY_SPREAD, HOME_SPREAD)''')
    conn.commit()
    
    return None

def update_all_data(conn, season):
    """Combines all the update functions above into one function that updates all my data"""
    print("updating basic team boxscores")
    update_team_basic_boxscores(conn = conn, season=season)
    print("updating advanced team boxscores")
    update_team_advanced_boxscores(conn = conn, season=season, dates=[])
    print("updating scoring boxscores")
    update_team_scoring_boxscores(conn = conn, season=season, dates=[])
    print("updating moneyline data")
    update_moneylines(conn, season, custom_dates=[])
    print("updating spreads data")
    update_spreads(conn, season, custom_dates=[])
    

def season_string(season):
    return str(season) + '-' + str(season+1)[-2:]


def get_season_games(season):
    season_str = season_string(season)
    gamelogs = []
    for season_type in ['Regular Season', 'Playoffs']:
        games = leaguegamelog.LeagueGameLog(season=season_str, season_type_all_star=season_type).get_data_frames()[0]
        gamelogs.append(games)
        
    df = pd.concat(gamelogs)
    
    df['HOME_TEAM'] = df['MATCHUP'].apply(
    lambda x: x[:3] if 'vs' in x else x[-3:])
    
    df['AWAY_TEAM'] = df['MATCHUP'].apply(
    lambda x: x[:3] if '@' in x else x[-3:])
    
    return df
