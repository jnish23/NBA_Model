import pandas as pd
from tqdm import tqdm
import sqlite3

def add_basic_boxscores_to_db(conn, start_season, end_season, if_exists='append'):
    """This function pulls basic team boxscores from the NBA_API package 
    and appends (or creates a new table if not exists) it to the table team_basic_boxscores in the sqlite db"""
    
    table_name = 'team_basic_boxscores'
    
    if if_exists == 'replace':
        conn.execute('DROP TABLE IF EXISTS ' + table_name)
        conn.execute('VACUUM')
        
    conn.execute("""CREATE TABLE IF NOT EXISTS {} (SEASON TEXT, TEAM_ID INTEGER, TEAM_ABBREVIATION TEXT, 
        TEAM_NAME TEXT, GAME_ID TEXT, GAME_DATE DATE, MATCHUP TEXT, WL TEXT, MIN INTEGER, FGM INTEGER, FGA INTEGER, 
        FG_PCT FLOAT, FG3M INTEGER, FG3A  INTEGER, FG3_PCT FLOAT, FTM INTEGER, FTA INTEGER, FT_PCT FLOAT, OREB INTEGER,
        DREB INTEGER, REB INTEGER, AST INTEGER, STL INTEGER, BLK INTEGER, TOV INTEGER, PF INTEGER, PTS INTEGER, 
        PLUS_MINUS INTEGER)""".format(table_name))    
    
    for season in range(start_season, end_season+1):
        season_str = season_string(season)

        for season_type in ['Regular Season', 'Playoffs']:
            boxscores = leaguegamelog.LeagueGameLog(season=season_str, season_type_all_star=season_type).get_data_frames()[0]
            season_boxscores.append(boxscores)
            sleep(2)
        season_df = pd.concat(season_boxscores)
        season_df['SEASON'] = season_str
        season_df.drop(columns = ['SEASON_ID', 'VIDEO_AVAILABLE'], inplace=True)
        
        season_df.to_sql(table_name, conn, if_exists='append', index=False)
        
        sleep(3)
        
    cur = conn.cursor()
    cur.execute('DELETE FROM {} WHERE rowid NOT IN (SELECT max(rowid) FROM {} GROUP BY TEAM_ID, GAME_ID)'.format(table_name, table_name))
    conn.commit()
    
    return None


def add_advanced_boxscores_to_db(conn, start_season, end_season, if_exists='append'):
    """
    This function pulls advanced team boxscores from the NBA_API package 
    and appends (or creates a new table if not exists) it to the table team_advanced_boxscores in the sqlite db
    
    Note: Because of timeout errors and that we have to pull each game's individually, each season takes 1-2 hours.
    If some games were not pulled in certain seasons, you can use the update functions to gather those individual games.
    """
    
    table_name = 'team_advanced_boxscores'
    game_ids_not_added = []
    
    if if_exists == 'replace':
        conn.execute('DROP TABLE IF EXISTS ' + table_name)
        conn.execute('VACUUM')
    
    conn.execute('''CREATE TABLE IF NOT EXISTS {} (GAME_ID TEXT, TEAM_ID INTEGER, TEAM_NAME TEXT, 
        TEAM_ABBREVIATION TEXT, TEAM_CITY TEXT, MIN TEXT, E_OFF_RATING FLOAT, OFF_RATING FLOAT, E_DEF_RATING FLOAT, 
        DEF_RATING FLOAT, E_NET_RATING FLOAT, NET_RATING FLOAT, AST_PCT FLOAT, AST_TOV FLOAT, 
        AST_RATIO FLOAT, OREB_PCT FLOAT, DREB_PCT FLOAT, REB_PCT FLOAT, E_TM_TOV_PCT FLOAT, 
        TM_TOV_PCT FLOAT, EFG_PCT FLOAT, TS_PCT FLOAT, USG_PCT FLOAT, E_USG_PCT FLOAT, E_PACE FLOAT, 
        PACE FLOAT, PACE_PER40 FLOAT, POSS FLOAT, PIE FLOAT)'''.format(table_name))
    
    
    for season in range(start_season, end_season+1):
        season_str = season_string(season)
        season_team_boxscores = []

        for season_type in ['Regular Season', 'Playoffs']:
            logs = leaguegamelog.LeagueGameLog(season=season, season_type_all_star=season_type).get_data_frames()[0]
            game_ids = logs['GAME_ID'].unique()

            for i in range(0, len(game_ids), 100):
                print('games {} to {}'.format(i, i+100))
                for game_id in tqdm(game_ids[i:i+100], desc='progress'):
                    try:
                        team_boxscores = boxscoreadvancedv2.BoxScoreAdvancedV2(game_id).get_data_frames()[1]                    
                        team_boxscores.to_sql(table_name, conn, if_exists='append', index=False)
                    except:
                        game_ids_not_added.append(game_id)
                    sleep(2)
                sleep(120)
                clear_output(wait=True)

        sleep(120)
        
    cur = conn.cursor()
    cur.execute('DELETE FROM {} WHERE rowid NOT IN (SELECT max(rowid) FROM {} GROUP BY TEAM_ID, GAME_ID)'.format(table_name, table_name))
    conn.commit()
    
    return None


def add_scoring_boxscores_to_db(conn, start_season, end_season, if_exists='append'):
    """
    This function pulls scoring team boxscores from the NBA_API package 
    and appends (or creates a new table if not exists) it to the table team_scoring_boxscores in the sqlite db.
    
    Note: Because of timeout errors and that we have to pull each game's individually, each season takes 1-2 hours.
    If some games were not pulled in certain seasons, you can use the update functions to gather those individual games.
    """
    
    table_name = 'team_scoring_boxscores'
    game_ids_not_added = []

    if if_exists == 'replace':
        conn.execute('DROP TABLE IF EXISTS ' + table_name)
        conn.execute('VACUUM')
    
    conn.execute('''GAME_ID TEXT, TEAM_ID INTEGER, TEAM_NAME TEXT, TEAM_ABBREVIATION TEXT, TEAM_CITY TEXT,
       MIN TEXT, PCT_FGA_2PT FLOAT, PCT_FGA_3PT FLOAT, PCT_PTS_2PT FLOAT, PCT_PTS_2PT_MR FLOAT,
       PCT_PTS_3PT FLOAT, PCT_PTS_FB FLOAT, PCT_PTS_FT FLOAT, PCT_PTS_OFF_TOV FLOAT,
       PCT_PTS_PAINT FLOAT, PCT_AST_2PM FLOAT, PCT_UAST_2PM FLOAT, PCT_AST_3PM FLOAT,
       PCT_UAST_3PM FLOAT, PCT_AST_FGM FLOAT, PCT_UAST_FGM FLOAT)'''.format(table_name))
    
    
    for season in range(start_season, end_season+1):
        season_str = season_string(season)
        season_team_boxscores = []

        for season_type in ['Regular Season', 'Playoffs']:
            logs = leaguegamelog.LeagueGameLog(season=season, season_type_all_star=season_type).get_data_frames()[0]
            game_ids = logs['GAME_ID'].unique()

            for i in range(0, len(game_ids), 100):
                print('games {} to {}'.format(i, i+100))
                for game_id in tqdm(game_ids[i:i+100], desc='progress'):
                    try:
                        scoring_boxscores = boxscorescoringv2.BoxScoreScoringV2(game_id).get_data_frames()[1]
                        scoring_boxscores.to_sql(table_name, conn, if_exists='append', index=False)
                    except:
                        game_ids_not_added.append(game_id)
                    sleep(2)
                sleep(120)
                clear_output(wait=True)

        sleep(120)
        
    cur = conn.cursor()
    cur.execute('DELETE FROM {} WHERE rowid NOT IN (SELECT max(rowid) FROM {} GROUP BY TEAM_ID, GAME_ID)'.format(table_name, table_name))
    conn.commit()
    
    return game_ids_not_added


def add_moneylines_to_db(conn, start_season, end_season, if_exists='append'):
    
    table_name = 'moneylines'

    if if_exists == 'replace':
        conn.execute('DROP TABLE IF EXISTS ' + table_name)
        
    conn.execute("""CREATE TABLE IF NOT EXISTS {} (SEASON TEXT, GM_DATE DATE, HOME_TEAM TEXT,
            AWAY_TEAM TEXT, AWAY_ML TEXT, HOME_ML TEXT)""".format(table_name))
    
    dates_with_no_data = []
    
    seasons = []
    gm_dates = []
    away_teams = []
    home_teams = []
    away_mls = []
    home_mls = []

    for season in range(start_season, end_season+1):
        print("scraping season: {}".format(season_string(season)))
        dates = get_game_dates(season)
        
        for date in tqdm(dates, desc='progress'):
            web = 'https://www.sportsbookreview.com/betting-odds/nba-basketball/money-line/?date={}'.format(date)
            path = '../chromedriver.exe'
            driver = webdriver.Chrome(path)
            driver.get(web)
            sleep(random.randint(1,2))

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
                seasons.append(season_string(season))

                away_team = event.find_elements_by_class_name('participantBox-3ar9Y')[0].text
                home_team = event.find_elements_by_class_name('participantBox-3ar9Y')[1].text

                away_teams.append(away_team)
                home_teams.append(home_team)

                gm_dates.append(date)

                mls = event.find_elements_by_class_name('pointer-2j4Dk')
                
                away_moneyline = []
                home_moneyline = []
                
                
                for i, ml in enumerate(mls):
                    if i%2==0:
                        away_moneyline.append(ml.text)
                    else:
                        home_moneyline.append(ml.text)
                
                away_moneyline = ",".join(away_moneyline)
                home_moneyline = ",".join(home_moneyline)

                away_mls.append(away_moneyline)
                home_mls.append(home_moneyline)
                
            driver.quit()
            
        clear_output(wait=True)
        
    df = pd.DataFrame({'SEASON':seasons,
                       'GM_DATE':gm_dates,
                       'AWAY_TEAM':away_teams, 
                      'HOME_TEAM':home_teams,
                      'AWAY_ML':away_mls,
                      'HOME_ML':home_mls,
                                         })
    
    df = df.sort_values(['GM_DATE']).reset_index(drop=True)
    
    df.to_sql(table_name, conn, if_exists='append', index=False)

    cur = connection.cursor()
    cur.execute('''DELETE FROM moneylines 
                    WHERE rowid NOT IN (SELECT MIN(rowid) FROM moneylines
                                        GROUP BY GM_DATE, AWAY_TEAM, HOME_TEAM)''')
    conn.commit()
    
    return df


def add_spreads_to_db(conn, start_season, end_season, if_exists='append'):
    
    table_name = 'spreads'

    if if_exists == 'replace':
        conn.execute('DROP TABLE IF EXISTS ' + table_name)
        
    conn.execute("""CREATE TABLE IF NOT EXISTS {} (SEASON TEXT, GM_DATE DATE, HOME_TEAM TEXT,
            AWAY_TEAM TEXT, AWAY_SCOREBOARD TEXT, HOME_SCOREBOARD TEXT, AWAY_SPREAD TEXT,
            HOME_SPREAD TEXT)""".format(table_name))
    
    dates_with_no_data = []
    
    seasons = []
    gm_dates = []
    away_teams = []
    home_teams = []
    away_scoreboards = []
    home_scoreboards = []
    away_spreads = []
    home_spreads = []
    
    for season in range(start_season, end_season+1):
        print("scraping season: {}".format(season_string(season)))
        dates = get_game_dates(season)    
        
        for date in tqdm(dates, desc='progress'):
            web = 'https://www.sportsbookreview.com/betting-odds/nba-basketball/?date={}'.format(date)
            path = '../chromedriver.exe'
            driver = webdriver.Chrome(path)
            driver.get(web)
            sleep(random.randint(1,2))

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

    df = df.sort_values(['GM_DATE']).reset_index(drop=True)
    
    df.to_sql(table_name, conn, if_exists='append', index=False)
    
    cur = connection.cursor()
    cur.execute('''DELETE FROM spreads 
                    WHERE rowid NOT IN (SELECT MIN(rowid) FROM spreads 
                                        GROUP BY GM_DATE, AWAY_TEAM, HOME_TEAM)''')
    conn.commit()
    
    return df

    
def season_string(season):
    return str(season) + '-' + str(season+1)[-2:]


def get_game_dates(season):
    season_str = season_string(season)
    dates = []
    for season_type in ['Regular Season', 'Playoffs']:
        games = leaguegamelog.LeagueGameLog(season=season_str, season_type_all_star=season_type).get_data_frames()[0]
        dates.extend(games['GAME_DATE'].unique())
        sleep(1)
    return dates
