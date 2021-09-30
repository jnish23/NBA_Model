import numpy as np
import pandas as pd

def convert_to_float(x):
    if x[0] == '+':
        return x[1:]
    elif x[0] == '-':
        return int(x)
    else:
        return np.nan


def clean_spread_data(df):

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
    
    df['away_spread'] = df['away_spread'].str.replace("'", "")
    df['away_spread'] = df['away_spread'].str.replace("½", ".5")
    df['away_spread'] = df['away_spread'].str.replace("PK", "+0")
    df['away_spread'] = df['away_spread'].str.replace("+", "")

    
    df['home_spread'] = df['home_spread'].str.replace("'", "")
    df['home_spread'] = df['home_spread'].str.replace("½", ".5")
    df['home_spread'] = df['home_spread'].str.replace("PK", "+0")
    df['home_spread'] = df['home_spread'].str.replace("+", "")


    away_spreads = df['away_spread'].str.split(',', expand=True)
    home_spreads = df['home_spread'].str.split(',', expand=True)
    away_spreads.columns = ['away_spread1', 'away_spread2', 'away_spread3', 'away_spread4']
    home_spreads.columns = ['home_spread1', 'home_spread2', 'home_spread3', 'home_spread4']
    
    full_df = pd.concat([df, away_spreads, home_spreads], axis=1)
    full_df = full_df.drop(columns=['away_spread', 'home_spread'])
    
    columns = full_df.columns[5:]
    for col in columns:
        full_df[col] = full_df[col].str.strip()
        full_df[col] = full_df[col].str[:-4]
        full_df.loc[full_df[col] == '', col] = np.nan
        full_df.loc[full_df[col] == '-', col] = np.nan
        full_df.loc[full_df[col] == '-.', col] = 0
        
    for col in full_df.columns[3:]:
        full_df[col] = full_df[col].str.replace('[', '')
        full_df[col] = full_df[col].str.replace(']', '')
        full_df[col] = full_df[col].str.replace("'", '')
        full_df[col] = full_df[col].str.strip()

        
    for col in columns:
        full_df[col] = full_df[col].apply(lambda x: str(x)[:-1] if str(x)[-1] == '-' else x)
        full_df[col] = full_df[col].astype(float)
        
    full_df['away_spread_mode'] = full_df[['away_spread1', 'away_spread2', 'away_spread3', 'away_spread4']].mode(axis=1)[0]
    full_df['home_spread_mode'] = full_df[['home_spread1', 'home_spread2', 'home_spread3', 'home_spread4']].mode(axis=1)[0]

    full_df['game_date'] = pd.to_datetime(full_df['game_date'])
    
    
    return full_df


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
        
    for col in ml_df.columns[3:11]:
        ml_df[col] = ml_df[col].astype(str).apply(convert_to_float)
        ml_df[col] = ml_df[col].astype(float)

    ml_df['away_ml_mode'] = ml_df.iloc[:, 3:7].mode(axis=1)[0]
    ml_df['home_ml_mode'] = ml_df.iloc[:, 7:11].mode(axis=1)[0]
    
    return ml_df