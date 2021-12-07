# NBA_Model

## Objective
The purpose of this project is to use historical NBA boxscore data and historical betting odds to build a model that can be profitable against vegas betting lines.
To do this, I will use team level statistics and stack multiple models. 


## The Data

All of the data is stored in a sqlite database nba.db

### Team Data - Team level boxscores for every game from 2000-present 

- team_basic_boxscores
- team_advanced_boxscores
- team_scoring_boxscores

### Betting Data - Spreads and Totals for every game from 2006-present
- betting_data_2010_present


## Results
This project is still in progress. This will be updated as I continue to work on it.

## Tools

This project is built using a number of different Python libraries, all in the [Jupyter Notebook](https://jupyter.org/) format.
These include:
- Pandas
- NBA_api
- Plotly
- Matplotlib
- SciKit-Learn
- Optuna
- Sqlite3