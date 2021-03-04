import numpy as np
import pandas as pd

def clean_spread_data(df):
    df['date'] = pd.to_datetime(df['date'])
    df['home_team_abbr'] = df['home_team_abbr'].replace({'NY':'NYK',
                                                                            'GS':'GSW',
                                                                            'SA':'SAS',
                                                                            'BK':'BKN',
                                                                            'NO':'NOP',
                                                                            'PHO':'PHX'
                                                                                        }
                                                                             )
    df['away_team_abbr'] = df['away_team_abbr'].replace({'NY':'NYK',
                                                                            'GS':'GSW',
                                                                            'SA':'SAS',
                                                                            'BK':'BKN',
                                                                            'NO':'NOP',
                                                                            'PHO':'PHX'}
                                                                           )

    teams = df['home_team_abbr'].unique()

    df = df.sort_values(['date'])
        
    relevant_betting = df[['date', 'home_team_abbr',  'away_team_abbr',
                           'home_score', 'away_score', 'spread', 'total']]
                          
    return relevant_betting


def clean_moneyline_data(df):
    abbr_mapping = {'Boston':'BOS', 'Portland':'POR', 
                    'L.A. Lakers':'LAL', 'Brooklyn':'BKN', 
                    'Cleveland':'CLE', 'Toronto':'TOR',
                    'Philadelphia':'PHI', 'Memphis':'MEM',
                    'Minnesota':'MIN', 'New Orleans':'NOP',
                   'Oklahoma City':'OKC', 'Dallas':'DAL', 
                    'San Antonio':'SAS', 'Denver':'DEN', 
                    'Golden State':'GSW', 'L.A. Clippers':'LAC', 
                    'Orlando':'ORL', 'Utah':'UTA', 
                    'Charlotte':'CHA', 'Detroit':'DET',
                   'Miami':'MIA', 'Phoenix':'PHX',
                    'Atlanta':'ATL', 'New York':'NYK', 
                    'Indiana':'IND', 'Chicago':'CHI',
                   'Houston':'HOU', 'Milwaukee':'MIL',
                    'Sacramento':'SAC', 'Washington':'WAS'}

    df['home_team'] = df['home_team'].replace(abbr_mapping)
    df['away_team'] = df['away_team'].replace(abbr_mapping)

    # moneyline_df['away_moneyline1'] = moneyline_df['away_moneyline'].apply(lambda x: x[0])
    away_mls = df['away_moneyline'].str.split(',', expand=True)
    away_mls.columns = ['away_ml1', 'away_ml2', 'away_ml3', 'away_ml4']
    home_mls = df['home_moneyline'].str.split(',', expand=True)
    home_mls.columns = ['home_ml1', 'home_ml2', 'home_ml3', 'home_ml4']

    ml_df = pd.concat([df, away_mls, home_mls], axis=1)

    ml_df = ml_df.drop(columns=['away_moneyline', 'home_moneyline'])

    for col in ml_df.columns[3:]:
        ml_df[col] = ml_df[col].str.replace('[', '')
        ml_df[col] = ml_df[col].str.replace(']', '')
        ml_df[col] = ml_df[col].str.replace("'", '')
        ml_df[col] = ml_df[col].str.strip()

    for col in ml_df.columns[3:]:
        ml_df.loc[ml_df[col] == ' -', col] = np.nan
        ml_df.loc[ml_df[col] == '-', col] = np.nan
        ml_df.loc[ml_df[col] == '', col] = np.nan
    
    return ml_df