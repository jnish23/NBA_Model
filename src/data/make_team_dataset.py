import pandas as pd

def load_team_data():
    """Loads basic, advanced, and scoring boxscores from
    seasons 2000-2019 and merges them into one dataframe
    """
    seasons = ['200{}-0{}'.format(x, x+1) if x != 9 else '200{}-{}'.format(x, x+1) for x in range(0,10)]
    seasons2 = ['20{}-{}'.format(x, x+1) for x in range(10, 20)]
    seasons.extend(seasons2)

    basic_gls_all, adv_gls_all, scoring_gls_all = [], [], []
    for season in seasons:
        basic_gls = pd.read_csv('../data/basic_team_boxscores/team_gamelogs_{}.csv'.format(season))
        basic_gls_all.append(basic_gls)

        adv_gls = pd.read_csv('../data/advanced_team_boxscores/team_advanced_boxscores_{}.csv'.format(season))
        adv_gls_all.append(adv_gls)

        scoring_gls = pd.read_csv('../data/scoring_team_boxscores/team_scoring_boxscores_{}.csv'.format(season))
        scoring_gls_all.append(scoring_gls)

    basic_gls_all_df = pd.concat(basic_gls_all)
    adv_gls_all_df = pd.concat(adv_gls_all)
    scoring_gls_all_df = pd.concat(scoring_gls_all)

    temp = pd.merge(basic_gls_all_df, adv_gls_all_df, how='left', on=['GAME_ID', 'TEAM_ABBREVIATION'], suffixes=['', '_y'])
    full_df = pd.merge(temp, scoring_gls_all_df, how='left', on=['GAME_ID', 'TEAM_ABBREVIATION'], suffixes=['', '_y'])

    full_df = full_df.drop(columns = ['VIDEO_AVAILABLE', 'TEAM_ID_y', 
                                      'TEAM_NAME_y', 'TEAM_CITY', 
                                      'MIN_y', 'TEAM_ID_y',
                                      'TEAM_NAME_y', 'TEAM_CITY_y',
                                      'MIN_y'])
    return full_df


def clean_team_data(df):
    """This function cleans the team_data
    1) Changes W/L to 1/0 
    2) Changes franchise abbreviations to their most 
    recent abbreviation for consistency
    3) Converts GAME_DATE to datetime object
    4) Creates a binary column 'HOME_GAME'
    5) Removes 3 games where advanced stats were not collected
    """
    df = df.copy()
    df['WL'] = (df['WL'] == 'W').astype(int)
    
    abbr_mapping = {'NJN':'BKN',
                   'CHH':'CHA',
                   'VAN':'MEM',
                   'NOH':'NOP',
                   'NOK':'NOP',
                   'SEA':'OKC'}
    
    df['TEAM_ABBREVIATION'] = df['TEAM_ABBREVIATION'].replace(abbr_mapping)
    df['MATCHUP'] = df['MATCHUP'].str.replace('NJN', 'BKN')
    df['MATCHUP'] = df['MATCHUP'].str.replace('CHH', 'CHA')
    df['MATCHUP'] = df['MATCHUP'].str.replace('VAN', 'MEM')
    df['MATCHUP'] = df['MATCHUP'].str.replace('NOH', 'NOP')
    df['MATCHUP'] = df['MATCHUP'].str.replace('NOK', 'NOP')
    df['MATCHUP'] = df['MATCHUP'].str.replace('SEA', 'OKC')


    df['GAME_DATE'] = pd.to_datetime(df['GAME_DATE'])
    
    df['HOME_GAME'] = df['MATCHUP'].str.contains('vs').astype(int)
    df = df.dropna(subset = ['E_OFF_RATING'])
    
    return df


def prep_for_aggregation(df):
    """This function...
    1) Removes categories that are percentages,
    as we will be averaging them and do not want to average 
    percentages. 
    2) Converts shooting percentage stats into raw values"""
    df = df.copy()
    
    df = df.drop(columns=['FT_PCT', 'FG_PCT', 'FG3_PCT', 'DREB_PCT',
                         'OREB_PCT', 'REB_PCT', 'AST_PCT', 'AST_TOV',
                          'AST_RATIO', 'E_TM_TOV_PCT', 'TM_TOV_PCT',
                          'EFG_PCT', 'TS_PCT', 'USG_PCT', 'E_USG_PCT',
                          'E_PACE', 'PACE', 'PACE_PER40'])
    
    df['FG2M'] = df['FGM'] - df['FG3M']
    df['FG2A'] = df['FGA'] - df['FG3A']
    df['PTS_2PT_MR'] = (df['PTS'] * df['PCT_PTS_2PT_MR']).astype('int8')
    df['PTS_FB'] = (df['PTS'] * df['PCT_PTS_FB']).astype('int8')
    df['PTS_OFF_TOV'] = (df['PTS'] * df['PCT_PTS_OFF_TOV']).astype('int8')
    df['PTS_PAINT'] = (df['PTS'] * df['PCT_PTS_PAINT']).astype('int8')
    df['AST_2PM'] = (df['FG2M'] * df['PCT_AST_2PM']).astype('int8')
    df['AST_3PM'] = (df['FG3M'] * df['PCT_AST_3PM']).astype('int8')
    df['UAST_2PM'] = (df['FG2M'] * df['PCT_UAST_2PM']).astype('int8')
    df['UAST_3PM'] = (df['FG3M'] * df['PCT_UAST_3PM']).astype('int8')    

    df = df.drop(columns = ['PCT_PTS_2PT', 'PCT_PTS_2PT_MR', 
                            'PCT_PTS_3PT', 'PCT_PTS_FB',
                            'PCT_PTS_OFF_TOV', 'PCT_PTS_PAINT',
                            'PCT_AST_2PM', 'PCT_UAST_2PM', 
                            'PCT_AST_3PM', 'PCT_UAST_3PM', 
                            'PCT_AST_FGM', 'PCT_UAST_FGM', 
                            'FGM', 'FGA'])

    df = df[['SEASON_YEAR', 'SEASON_ID', 'TEAM_ID',
      'TEAM_ABBREVIATION', 'TEAM_NAME', 'GAME_ID',
      'GAME_DATE', 'MATCHUP', 'HOME_GAME', 'WL', 'MIN',
      'FG2M', 'FG2A', 'FG3M', 'FG3A', 'FTM', 'FTA',
      'OREB', 'DREB', 'REB', 'AST', 'STL', 'BLK', 'TOV', 'PF',
      'PTS', 'PLUS_MINUS', 'E_OFF_RATING', 'OFF_RATING', 
      'E_DEF_RATING', 'DEF_RATING', 'E_NET_RATING', 'NET_RATING', 
      'POSS', 'PIE', 'PTS_2PT_MR', 'PTS_FB', 'PTS_OFF_TOV',
      'PTS_PAINT', 'AST_2PM', 'UAST_2PM', 'AST_3PM', 'UAST_3PM']]

    
    return df