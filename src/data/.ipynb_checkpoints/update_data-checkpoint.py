import pandas as pd
from tqdm import tqdm
from nba_api.stats.endpoints import leaguegamelog
from nba_api.stats.endpoints import playergamelogs
from nba_api.stats.endpoints import boxscoreadvancedv2
from nba_api.stats.endpoints import boxscorescoringv2
from nba_api.stats.endpoints import boxscoreplayertrackv2
from selenium import webdriver
from time import sleep

## Update basic team gamelogs and player gamelogs

def update_team_gamelogs(season= '2020-21'):
    """
    Updates the basic team gamelogs csv file for the given season
    INPUTS:
    season (str): current season which you will update ie: '2020-21'
    
    """
    dfs = []
    for season_type in ['Regular Season', 'Playoffs']:
        team_gamelogs = leaguegamelog.LeagueGameLog(season=season, season_type_all_star=season_type).get_data_frames()[0]
        dfs.append(team_gamelogs)
        
    path = '../data/current_season_data/team_gamelogs_{}.csv'.format(season)
    team_gamelogs_full = pd.concat(dfs)
    team_gamelogs_full.to_csv(path, index=False)

    return None


def update_player_gamelogs(season= '2020-21'):
    """
    Updates the basic player gamelogs csv file for the given season
    INPUTS:
    season (str): current season which you will update ie: '2020-21'
    """
    dfs = []
    for season_type in ['Regular Season', 'Playoffs']:
        player_gamelogs = playergamelogs.PlayerGameLogs(season_nullable=season, season_type_nullable = season_type).get_data_frames()[0]
        dfs.append(player_gamelogs)
    
    player_gamelogs_full = pd.concat(dfs)
    path = '../data/current_season_data/player_gamelogs_{}.csv'.format(season)    
    player_gamelogs_full.to_csv(path, index=False)
        
    return None


def update_advanced_boxscores(season = '2020-21'):
    """
    Updates the advanced team and player boxscores csv file for the given season
    INPUTS:
    season (str): current season which you will update ie: '2020-21'
    """
    # Load the data I currently have
    current_player_boxscores = pd.read_csv('../data/current_season_data/player_advanced_boxscores_{}.csv'.format(season), dtype={'GAME_ID':'object'})
    current_team_boxscores = pd.read_csv('../data/current_season_data/team_advanced_boxscores_{}.csv'.format(season), dtype={'GAME_ID':'object'})

    current_game_ids = current_team_boxscores['GAME_ID'].unique()
#     ids_with_nans = current_team_boxscores.loc[current_team_boxscores.isna().any(axis=1), 'GAME_ID'].unique()

    # get all GAME_IDs that are currently in my data
    
    # Get GAME_IDs for all games that have been played in season so far
    to_date_game_ids = []
    for season_type in ['Regular Season', 'Playoffs']:
        to_date_gamelogs = leaguegamelog.LeagueGameLog(season=season, season_type_all_star=season_type).get_data_frames()[0]
        to_date_game_ids.extend(to_date_gamelogs['GAME_ID'].unique())
        
    # Determine which GAME_IDs are missing in my data
    missing_game_ids = set(to_date_game_ids) - set(current_game_ids)
#     missing_game_ids.update(ids_with_nans)
    
    num_games_updated = len(missing_game_ids)
    if num_games_updated == 0:
        print("Data is all up to date")
        return None
    
    print("Updating {} Games".format(num_games_updated))
    
    season_player_boxscores = []
    season_team_boxscores = []
    
    for game_id in tqdm(missing_game_ids, desc='progress'):
        boxscores = boxscoreadvancedv2.BoxScoreAdvancedV2(game_id).get_data_frames()
        season_player_boxscores.append(boxscores[0])
        season_team_boxscores.append(boxscores[1])    
        sleep(2)
        
    new_player_boxscores = pd.concat(season_player_boxscores)
    new_team_boxscores = pd.concat(season_team_boxscores)
    
    updated_player_boxscores = pd.concat([current_player_boxscores, new_player_boxscores])
    updated_team_boxscores = pd.concat([current_team_boxscores, new_team_boxscores])
   
    updated_player_boxscores.to_csv('../data/current_season_data/player_advanced_boxscores_{}.csv'.format(season), index=False)
    updated_team_boxscores.to_csv('../data/current_season_data/team_advanced_boxscores_{}.csv'.format(season), index=False)

    return None


def update_scoring_boxscores(season='2020-21'):
    """
    Updates the scoring team and player boxscores csv file for the given season
    INPUTS:
    season (str): current season which you will update ie: '2020-21'
    """
    # Load the data I currently have
    current_player_boxscores = pd.read_csv('../data/current_season_data/player_scoring_boxscores_{}.csv'.format(season), dtype={'GAME_ID':'object'})
    current_team_boxscores = pd.read_csv('../data/current_season_data/team_scoring_boxscores_{}.csv'.format(season), dtype={'GAME_ID':'object'})
  
    # get all GAME_IDs that are currently in my data
    current_game_ids = current_team_boxscores['GAME_ID'].unique()
#     ids_with_nans = current_team_boxscores.loc[current_team_boxscores.isna().any(axis=1), 'GAME_ID'].unique()

    to_date_game_ids = []
    for season_type in ['Regular Season', 'Playoffs']:
        to_date_gamelogs = leaguegamelog.LeagueGameLog(season=season, season_type_all_star=season_type).get_data_frames()[0]
        to_date_game_ids.extend(to_date_gamelogs['GAME_ID'].unique())
    
    # Determine which GAME_IDs are missing in my data
    missing_game_ids = set(to_date_game_ids) - set(current_game_ids)
#     missing_game_ids.update(ids_with_nans)
    
    num_games_updated = len(missing_game_ids)
    if num_games_updated == 0:
        print("Data is all up to date")
        return None
    
    print("Updating {} Games".format(num_games_updated))
    
    
    season_player_boxscores = []
    season_team_boxscores = []
    for game_id in tqdm(missing_game_ids, desc='progress'):
        boxscores = boxscorescoringv2.BoxScoreScoringV2(game_id).get_data_frames()
        season_player_boxscores.append(boxscores[0])
        season_team_boxscores.append(boxscores[1])    
        sleep(2)
        
    new_player_boxscores = pd.concat(season_player_boxscores)
    new_team_boxscores = pd.concat(season_team_boxscores)
    
    updated_player_boxscores = pd.concat([current_player_boxscores, new_player_boxscores])
    updated_team_boxscores = pd.concat([current_team_boxscores, new_team_boxscores])
   
    updated_player_boxscores.to_csv('../data/current_season_data/player_scoring_boxscores_{}.csv'.format(season), index=False)
    updated_team_boxscores.to_csv('../data/current_season_data/team_scoring_boxscores_{}.csv'.format(season), index=False)
        
    return None


def update_tracking_boxscores(season='2020-21'):
    # Load the data I currently have
    current_player_boxscores = pd.read_csv('../data/current_season_data/player_tracking_boxscores_{}.csv'.format(season), dtype={'GAME_ID':'object'})
    current_team_boxscores = pd.read_csv('../data/current_season_data/team_tracking_boxscores_{}.csv'.format(season), dtype={'GAME_ID':'object'})

    current_game_ids = current_team_boxscores['GAME_ID'].unique()
#     ids_with_nans = current_team_boxscores.loc[current_team_boxscores.isna().any(axis=1), 'GAME_ID'].unique()

    # get all GAME_IDs that are currently in my data
    to_date_game_ids = []
    for season_type in ['Regular Season', 'Playoffs']:
        to_date_gamelogs = leaguegamelog.LeagueGameLog(season=season, season_type_all_star=season_type).get_data_frames()[0]
        to_date_game_ids.extend(to_date_gamelogs['GAME_ID'].unique())
        
    # Determine which GAME_IDs are missing in my data
    missing_game_ids = set(to_date_game_ids) - set(current_game_ids)
#     missing_game_ids.update(ids_with_nans)
    
    num_games_updated = len(missing_game_ids)
    
    
    if num_games_updated == 0:
        print("Data is all up to date")
        return None
    
    print("Updating {} Games".format(num_games_updated))
    
    season_player_boxscores = []
    season_team_boxscores = []
    
    for game_id in tqdm(missing_game_ids, desc='progress'):
        boxscores = boxscoreplayertrackv2.BoxScorePlayerTrackV2(game_id).get_data_frames()
        season_player_boxscores.append(boxscores[0])
        season_team_boxscores.append(boxscores[1])    
        sleep(2)
        
    new_player_boxscores = pd.concat(season_player_boxscores)
    new_team_boxscores = pd.concat(season_team_boxscores)
    
    updated_player_boxscores = pd.concat([current_player_boxscores, new_player_boxscores])
    updated_team_boxscores = pd.concat([current_team_boxscores, new_team_boxscores])
   
    updated_player_boxscores.to_csv('../data/current_season_data/player_tracking_boxscores_{}.csv'.format(season), index=False)
    updated_team_boxscores.to_csv('../data/current_season_data/team_tracking_boxscores_{}.csv'.format(season), index=False)
    
    
    return None


def update_all_data(season='2020-21'):
    """Combines all the update functions above into one function that updates all my data"""
    print("updating basic team boxscores")
    update_team_gamelogs(season=season)
    print("updating basic player boxscores")
    update_player_gamelogs(season=season)
    print("updating advanced boxscores")
    update_advanced_boxscores(season=season)
    print("updating scoring boxscores")
    update_scoring_boxscores(season=season)
    print("updating tracking boxscores")
    update_tracking_boxscores(season=season)

    return None


def update_moneyline_data(season='2020-21'):
    
    current_moneylines_df = pd.read_csv('../data/all_moneylines_sbr.csv')
    
    current_dates = current_moneylines_df['game_date'].unique()
    
    up_to_date_dates = []
    
    for season_type in ['Regular Season', 'Playoffs']:
        to_date_gamelogs = leaguegamelog.LeagueGameLog(season=season, season_type_all_star=season_type).get_data_frames()[0]
        up_to_date_dates.extend(to_date_gamelogs['GAME_DATE'].unique())
    
    missing_dates = set(up_to_date_dates) - set(current_dates)    
    
    print("Updating lines for {} days".format(len(missing_dates)))
    # Get Moneylines

    gm_date = []
    away_teams = []
    home_teams = []
    away_moneylines = []
    home_moneylines = []

    for date in tqdm(missing_dates, desc='progress'):
        web = 'https://www.sportsbookreview.com/betting-odds/nba-basketball/money-line/?date={}'.format(date)
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

            mls = event.find_elements_by_class_name('pointer-2j4Dk')
            away_lines = []
            home_lines = []
            for i in range(len(mls)):    
                if i % 2 == 0:
                    away_lines.append(mls[i].text)
                else:
                    home_lines.append(mls[i].text)
            away_moneylines.append(away_lines)
            home_moneylines.append(home_lines)
            
        driver.quit()

        
    moneylines_to_add_df = pd.DataFrame({'away_team':away_teams,
                 'home_team':home_teams,
                 'game_date':gm_date,
                 'away_moneyline':away_moneylines,
                 'home_moneyline':home_moneylines})
    
    updated_moneylines_df = pd.concat([current_moneylines_df, moneylines_to_add_df])
    updated_moneylines_df.sort_values('game_date', inplace=True)
    
    for col in ['away_moneyline', 'home_moneyline']:
        updated_moneylines_df[col] = updated_moneylines_df[col].astype(str)
        updated_moneylines_df[col] = updated_moneylines_df[col].str.replace("[", "")
        updated_moneylines_df[col] = updated_moneylines_df[col].str.replace("]", "")
        updated_moneylines_df[col] = updated_moneylines_df[col].str.strip()
        
    updated_moneylines_df.to_csv('../data/all_moneylines_sbr.csv', index=False)
    
    return None


def update_spread_data(season='2020-21'):
    # Get Spreads
    current_spreads_df = pd.read_csv('../data/all_spreads_sbr.csv')
    
    dates_with_null_scores = current_spreads_df.loc[current_spreads_df['home_scoreboard'].isnull(), 'game_date'].tolist()
    current_dates = current_spreads_df['game_date'].unique()
    
    up_to_date_dates = []
    for season_type in ['Regular Season', 'Playoffs']:
        to_date_gamelogs = leaguegamelog.LeagueGameLog(season=season, season_type_all_star=season_type).get_data_frames()[0]
        up_to_date_dates.extend(to_date_gamelogs['GAME_DATE'].unique())
    
    missing_dates = set(up_to_date_dates) - set(current_dates)
    missing_dates.update(dates_with_null_scores)
    print("Updating lines for {} days".format(len(missing_dates)))


    gm_date = []
    away_teams = []
    home_teams = []
    away_scoreboards = []
    home_scoreboards = []
    away_spreads = []
    home_spreads = []
    
    

    for date in tqdm(missing_dates, desc='progress'):
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


            scoreboard = event.find_elements_by_class_name('scoreboard-1TXQV')

            home_score = []
            away_score = []

            for score in scoreboard:
                quarters = score.find_elements_by_class_name('scoreboardColumn-2OtpR')
                for i in range(len(quarters)):
                    scores = quarters[i].text.split('\n')
                    away_score.append(scores[0])
                    home_score.append(scores[1])
                away_scoreboards.append(away_score)
                home_scoreboards.append(home_score)


            if len(away_scoreboards) != len(away_teams):
                num_to_add = len(away_teams) - len(away_scoreboards)
                for i in range(num_to_add):
                    away_scoreboards.append([])
                    home_scoreboards.append([])


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

            if len(away_spreads) != len(away_teams):
                num_to_add = len(away_teams) - len(away_spreads)
                for i in range(num_to_add):
                    away_scoreboards.append([])
                    home_scoreboards.append([])


        driver.quit()

    spreads_to_add_df = pd.DataFrame({'away_team':away_teams,
                      'home_team':home_teams,
                       'game_date':gm_date,
                      'away_scoreboard':away_scoreboards,
                      'home_scoreboard':home_scoreboards,
                      'away_spread':away_spreads,
                      'home_spread':home_spreads})

    updated_spreads = pd.concat([current_spreads_df, spreads_to_add_df])
    
    updated_spreads = updated_spreads.drop_duplicates(subset=['away_team', 'home_team', 'game_date'], keep='last')
    updated_spreads.sort_values('game_date', inplace=True)
    
    for col in updated_spreads.columns[3:]:
        updated_spreads[col] = updated_spreads[col].astype(str)
        updated_spreads[col] = updated_spreads[col].str.replace("[", "")
        updated_spreads[col] = updated_spreads[col].str.replace("]", "")
        updated_spreads[col] = updated_spreads[col].str.strip()

    updated_spreads.to_csv('../data/all_spreads_sbr.csv', index=False)
    
    return None

