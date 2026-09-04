import pandas as pd
import requests
import numpy as np
import json, os, time, pdb
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches

_UTILS_DIR = os.path.dirname(os.path.abspath(__file__))
_DATA_DIR = os.path.normpath(os.path.join(_UTILS_DIR, '..', 'formed_data'))


def gather_dataframes(league):
    # print(_DATA_DIR)
    # file_dir = os.listdir(os.path.join(_DATA_DIR, 'game_data'))
    file_game_dir_v2 = os.listdir(os.path.join(_DATA_DIR, 'game_data_v2'))
    player_file_dir = os.listdir(os.path.join(_DATA_DIR, 'player_data_mk2'))
    team_file_dir = os.listdir(os.path.join(_DATA_DIR, 'team_data'))

    league_dict = {
        'URC': [
            [os.path.join(_DATA_DIR, 'game_data_v2', i) for i in file_game_dir_v2 if 'URC_game' in i],
            [os.path.join(_DATA_DIR, 'player_data_mk2', i) for i in player_file_dir if 'URC_player' in i],
            [os.path.join(_DATA_DIR, 'team_data', i) for i in team_file_dir if 'URC_team' in i]
        ],
        'ChampCup': [
            [os.path.join(_DATA_DIR, 'game_data_v2', i) for i in file_game_dir_v2 if 'ChampCup_game' in i],
            [os.path.join(_DATA_DIR, 'player_data_mk2', i) for i in player_file_dir if 'ChampCup_player' in i],
            # [os.path.join(_DATA_DIR, 'team_data', i) for i in team_file_dir if 'ChampCup_team' in i]
        ],
        'Prem': [
            [os.path.join(_DATA_DIR, 'game_data_v2', i) for i in file_game_dir_v2 if 'Prem_game' in i],
            [os.path.join(_DATA_DIR, 'player_data_mk2', i) for i in player_file_dir if 'Prem_player' in i],
            [os.path.join(_DATA_DIR, 'team_data', i) for i in team_file_dir if 'Prem_team' in i]
        ],
        'T14': [
            [os.path.join(_DATA_DIR, 'game_data_v2', i) for i in file_game_dir_v2 if 'T14_game' in i],
            [os.path.join(_DATA_DIR, 'player_data_mk2', i) for i in player_file_dir if 'T14_player' in i],
            [os.path.join(_DATA_DIR, 'team_data', i) for i in team_file_dir if 'T14_team' in i]
        ],
        'RWC': [],
        'RChamp': [
            [os.path.join(_DATA_DIR, 'game_data_v2', i) for i in file_game_dir_v2 if 'RChamp_game' in i],
            [os.path.join(_DATA_DIR, 'player_data_mk2', i) for i in player_file_dir if 'RChamp_player' in i],
            [os.path.join(_DATA_DIR, 'team_data', i) for i in team_file_dir if 'RChamp_team' in i]
        ],
        'SixNat': [
            [os.path.join(_DATA_DIR, 'game_data_v2', i) for i in file_game_dir_v2 if 'SixNat_game' in i],
            [os.path.join(_DATA_DIR, 'player_data_mk2', i) for i in player_file_dir if 'SixNat_player' in i],
            [os.path.join(_DATA_DIR, 'team_data', i) for i in team_file_dir if 'SixNat_team' in i]
        ],
        'SR': [
            [os.path.join(_DATA_DIR, 'game_data_v2', i) for i in file_game_dir_v2 if 'SR_game' in i],
            [os.path.join(_DATA_DIR, 'player_data_mk2', i) for i in player_file_dir if 'SR_player' in i],
            [os.path.join(_DATA_DIR, 'team_data', i) for i in team_file_dir if 'SR_team' in i]
        ],
        'PNCup': []
    }

    grab_files = lambda league, order: pd.concat(list(map(pd.read_csv, league_dict[league][order])))
    if league in ['SixNat', 'RChamp', 'RWC']:
        game_df, player_df, team_df = data_combine_temp_inter(
            grab_files(league, 0),
            grab_files(league, 1),
            grab_files(league, 2)
        )
    elif league in ['ChampCup']:
        game_df, player_df = data_combine(
            grab_files(league, 0),
            grab_files(league, 1), 
            team_df = None, 
            alt_version=True
        )
        return game_df, player_df 

    else:
        game_df, player_df, team_df = data_combine(
            grab_files(league, 0),
            grab_files(league, 1),
            grab_files(league, 2)
        )

    return game_df, player_df, team_df



def data_combine(game_df, player_df, team_df, alt_version = False):
    player_df = player_df.merge(game_df[['game_id', 'season']], how='left', on='game_id')

    if not alt_version:
        team_df = team_df[~team_df['team_id'].isnull()]
        team_df['team_id'] = team_df['team_id'].astype(int)
        team_df['season'] = team_df['season'].astype(int)

        player_df = player_df.merge(
            team_df[['team_name', 'team_id', 'season', 'rank']],
            how='left',
            left_on=['team', 'season'],
            right_on=['team_id', 'season']
        )

        return game_df, player_df, team_df
    else:
        return game_df, player_df


def data_combine_temp_inter(game_df,player_df, team_df):
    player_df = player_df.merge(game_df[['game_id', 'season']], how='left', on='game_id')
    team_df = team_df[~team_df['team_id'].isnull()]
    team_df['team_id'] = team_df['team_id'].astype(int)
    team_df['season'] = team_df['season'].astype(int)

    player_df = player_df.merge(
        team_df[['team_name', 'team_id', 'season']],
        how='left',
        left_on=['team', 'season'],
        right_on=['team_id', 'season']
    )

    return game_df, player_df, team_df


def fill_pos(player_df, league):
    pos_df = player_df.groupby(['p_id', 'position'])['game_id'].count().reset_index()
    non_reserves = pos_df[pos_df['position'] != 'R'].sort_values(by=['p_id', 'game_id'], ascending=False)
    max_pos = non_reserves.groupby('p_id').first().reset_index().rename(columns={'position': 'alt_position'})
    
    player_df = player_df.merge(max_pos[['p_id', 'alt_position']], how='left', on='p_id')
    player_df['off_position'] = np.where(
        player_df['position'] != 'R', player_df['position'], 
        np.where(
            player_df['alt_position'].isnull(), 
            player_df['position'], 
            player_df['alt_position']
        )
    )
    
    back_pos = ['FB', 'W', 'C', 'FH', 'SH']
    for_pos = ['P', 'H', 'L', 'N8', 'FL']
    player_df['pos_place'] = np.where(
        player_df['off_position'].isin(back_pos),
        'back',
        np.where(
            player_df['off_position'].isin(for_pos),
            'forward',
            'reserve'
        )
    )
    player_df['league'] = league
    
    return player_df


def get_n_top_not(row, n=5):
    if row['rank'] <= n:
        place = 'top_5'
    elif row['rank'] > (row['max_teams']-n):
        place = 'bot_5'
    else:
        place = 'mid_table'
        
    return place 


def get_min_max_lims(input_col):
    if type(input_col) == type(list()):
        input_col = pd.concat(input_col, axis=0)
        
    max_lim = input_col.max() + input_col.std()
    min_lim = input_col.min() - input_col.std()

    return (min_lim, max_lim)


def calc_custom_metrics(df):
    df['meters_per_passes'] = np.where(
        df['passes'] > 0,
        df['meters_run'] / df['passes'],
        df['meters_run']
    )
    
    df['adj_meters_per_passes'] = np.where(
        df['passes'] > 0,
        df['meters_run'] / (df['passes']*df['runs']),
        df['meters_run'] / df['runs']
    )
    
    df['passes_per_runs'] = np.where(
        df['runs'] > 0,
        df['passes'] / df['runs'],
        df['passes']
    )
    
    df['passes_per_runs'].fillna(0.0, inplace=True)
    df['meters_per_passes'].fillna(0.0, inplace=True)
    df['adj_meters_per_passes'].fillna(0.0, inplace=True)
    
    return df 

    