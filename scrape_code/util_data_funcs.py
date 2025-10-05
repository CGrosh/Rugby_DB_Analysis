import pandas as pd 
import requests 
import numpy as np 
import json, os, time, pdb 
import matplotlib.pyplot as plt 
import matplotlib.patches as mpatches


def data_combine(game_df, player_df, team_df):
    player_df = player_df.merge(game_df[['game_id', 'season']], how='left', on='game_id')
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

    