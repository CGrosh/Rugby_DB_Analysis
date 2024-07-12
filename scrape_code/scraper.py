import pandas as pd 
import requests 
import numpy as np 
from bs4 import BeautifulSoup
import json, os, time, pdb 
import sys 
import warnings, logging 
import itertools 
from tqdm import tqdm
from argparse import ArgumentParser
from util_funcs import * 
from selenium import webdriver 
from selenium.webdriver.chrome.options import Options 
from selenium.webdriver.common.by import By 
from selenium.common.exceptions import NoSuchElementException, WebDriverException
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


class scrape_model:
    
    def __init__(self, league_set, season_set, date_set, update_type: str):
        
        self.league_set = league_set
        self.season_set = season_set 
        self.update_type = update_type
        
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64)' + \
            'AppleWebKit/537.36 (KHTML, like Gecko) ' + \
           'Chrome/119.0.0.0 Safari/537.36'
        }
        
        
    def start_up_driver(self):
        options = webdriver.ChromeOptions()
        options.add_argument('--headless')
        self.driver = webdriver.Chrome(options=options)
        
    
    def check_driver(self):
        try:
            url_status = self.driver.current_url
            status = True 
        except WebDriverException as e: 
            status = False 
        finally:
            status = False 
        return status 
        
       
    def close_out_driver(self):
        self.driver.close()
        self.driver.quit()
        
    
    # Util function used in schedule pulls 
    def label_table_parse(self, labels, table):
        row_step = []
        for row in table:
            val_text = row.text
            if val_text not in labels:
                row_step.append(val_text)

        row_ordered = [[row_step[i-1], row_step[i]] \
                       for i in range(1, len(row_step), 2)]

        return row_ordered 
    
    
    def slice_prop_func(self, x, char):
        x = x.replace(" ", "")
        if char == '/':
            return int(x[:x.find(char)])
        elif char == '(':
             return int(x[x.find('/')+1:x.find('(')])  
        elif char == ')':
            return int(x[x.find('(')+1:x.find('%')]) * 0.01


    def slice_poss_terr_func(self, x):
        x = x.replace(" ", "")
        two_h_val = int(x[x.find('/')+1:].replace('%', '')) * 0.01
        one_h_val = int(x[:x.find('/')].replace('%', '')) * 0.01
        return one_h_val, two_h_val

    
    def clean_match_stats(self, df):
        
        perc_fix = lambda x: 0.0 if x == 'N/A' else int(x.replace("%", ""))*0.01
        
        prop_variable_names = ['rucks_won', 'mauls_won', 'scrum', 'lineout']
        for side in ['home', 'away']:
            try:
                df['{}_kick_percent'.format(side)] = df[
                    '{}_kick_percent'.format(side)].apply(perc_fix)

                df['{}_1h_poss'.format(side)] = df[
                    '{}_possession_1h_2h'.format(side)].apply(lambda x: self.slice_poss_terr_func(x)[0])
                df['{}_2h_poss'.format(side)] = df[
                    '{}_possession_1h_2h'.format(side)].apply(lambda x: self.slice_poss_terr_func(x)[1])

                df['{}_1h_terr'.format(side)] = df[
                    '{}_territory_1h_2h'.format(side)].apply(lambda x: self.slice_poss_terr_func(x)[0])
                df['{}_2h_terr'.format(side)] = df[
                    '{}_territory_1h_2h'.format(side)].apply(lambda x: self.slice_poss_terr_func(x)[1])
            except:
                print('annoying error')
                pdb.set_trace()
            
            for var_group in prop_variable_names:
                
                if len(df[df['{}_{}'.format(side, var_group)].str.contains('NaN')]) > 0:
                    print('stopped on annoying rows')
                    pdb.set_trace()

                if 'won' in var_group:
                    str_attach = ''
                else:
                    str_attach = 'won_'

                df['{}_{}_{}'.format(side, var_group, 'num')] = \
                        df['{}_{}'.format(side, var_group)].apply(self.slice_prop_func, args=('/', ))
                df['{}_{}_{}'.format(side, var_group, 'total_num')] = \
                        df['{}_{}'.format(side, var_group)].apply(self.slice_prop_func, args=('(', ))
                df['{}_{}_{}'.format(side, var_group, str_attach+'percent')] = \
                        df['{}_{}'.format(side, var_group)].apply(self.slice_prop_func, args=(')', ))
                
        return df 
    
    
    # Update the driver on the player data pafe to each group of player data tabs 
    def update_driver_to_page(self, tab_labels, label):
        try:
            labels = tab_labels.find_elements(by=By.TAG_NAME, value='span')
            labels[label].click()
        except:
            logging.error("Driver not set to page for player stat scraping")
            return None
        
    
    # Internally used by get_player_stats function below 
    def get_player_page_data(self, table, fields, team):
        
        group_df = [] 
        for player in range(1, len(table)):

            player_tag = table[player].find_elements(by=By.TAG_NAME, value='a')
            p_name, href_link = player_tag[0].text, player_tag[0].get_attribute('href')

            p_id = href_link[href_link.find('player/')+7:href_link.find('.html')]
            pos = table[player].find_elements(by=By.TAG_NAME, value='span')[0].text

            stats = [
                float(i.text) for i in table[player].find_elements(
                    by=By.TAG_NAME, value='td')[1:]
            ]

            group_df.append([p_name, pos, p_id, team]+stats)
            
        titles = ['p_name', 'position', 'p_id', 'team'] + fields
        test_df = pd.DataFrame(group_df, columns=titles)
        
        return test_df 
    
    
    # @staticmethod
    def get_player_stats(self, game_id, league_id):
        
        fields = [
            ['tries', 'try_assists', 'conversion_goals',
             'penalty_goals', 'drop_goals_converted', 'points'],
            ['-', 'passes', 'runs', 'meters_run',
             'clean_breaks', 'defenders_beaten', 'offloads', '-'],
            ['to_conceded', 'tackles', 'missed_tackles', 'lineouts_won'],
            ['penalties_conceded', 'yellows', 'reds']
         ]
        
        page_url = "https://www.espn.co.uk/rugby/"+\
                "playerstats?gameId={}&league={}".format(game_id, league_id)
        self.driver.get(page_url)
        
        tab_labels = self.driver.find_element(
            by=By.CLASS_NAME, value='col-b').find_elements(
            by=By.TAG_NAME, value='div')[3]
        
        grouped_dfs = []
        for label in range(4):
            
            self.update_driver_to_page(tab_labels, label)
            
            group_table = self.driver.find_elements(by=By.TAG_NAME, value='table')
            home_group = group_table[0].find_elements(by=By.TAG_NAME, value='tr')
            away_group = group_table[1].find_elements(by=By.TAG_NAME, value='tr')
            
            comb_df = pd.concat(
                [
                    self.get_player_page_data(home_group, fields[label], 'home'),
                    self.get_player_page_data(away_group, fields[label], 'away')
                ], axis=0
            )
            grouped_dfs.append(comb_df)
        # pdb.set_trace()
        final_df = pd.concat(grouped_dfs, axis=1)
        final_df['game_id'], final_df['league_df'] = game_id, league_id
        final_df = final_df.loc[:, ~final_df.columns.duplicated()]
        
        return final_df 
        
    
    # Takes the game and league to parse the match stats 
    def get_match_stats(self, game_id, league_id): 

        # URL declaration, scraping, and initial parsing to get the tables on the page 
        url = 'https://www.espn.com/rugby/matchstats?gameId={}&league={}'.format(
            game_id, league_id
        )
        response = requests.get(url, headers=self.headers).content
        soup = BeautifulSoup(response, 'html.parser')
        tables = soup.findAll('table')

        # Group labels 
        group_2_labels = ['Tries', 'Conversion Goals', 
                          'Penalty Goals', 'Kick Percent Success'
        ]
        group_3_labels = ['Kicks From Hand', 'Passes', 'Runs']
        group_4_labels = [
            'Possession 1H/2H', 'Territory 1H/2H', 'Clean Breaks', 
            'Defenders Beaten', 'Offload', 'Rucks Won', 
            'Mauls Won', 'Turnovers Conceded'
        ]
        group_8_labels = ['Red Cards', 'Yellow Cards', 'Total Free Kicks Conceded']

        # Parsed data lists used by multiple groups 
        four_tables = soup.findAll(
            class_='sub-module equal-height countChartList height-reset'
        )
        check_top_largeLabels = soup.findAll(
            class_="stat-graph compareLineGraph twoTeam largeLabels"
        )
        stacked_rls = soup.findAll(class_='stacked-rl')

        # GROUP 1: Home and Away team parsing 
        top_bar = soup.find(class_='competitors')

        h_a_team_bar = [
            top_bar.find(class_='team team-a'),
            top_bar.find(class_='team team-b')
        ]

        # GROUP 2: Match Events 
        match_event = four_tables[0].find('tbody')
        match_event_rows = match_event.findAll('td')

        group_2_ordered = self.label_table_parse(group_2_labels, match_event_rows)

        # GROUP 3: Kick/Pass/Run 
        home_away_total_meters = [
            int(i.text) for i in check_top_largeLabels[0].findAll(class_='chartValue')
        ]

        meter_rows = four_tables[1].find('tbody').findAll('td')
        group_3_ordered = self.label_table_parse(group_3_labels, meter_rows)

        # GROUP 4: Attacking 
        attack_rows = stacked_rls[0].find('tbody').findAll('td')
        group_4_ordered = self.label_table_parse(group_4_labels, attack_rows)

        # GROUP 5: Possession and Territory 
        terr_vals = soup.findAll(
            class_="stat-graph compareLineGraph twoTeam largeLabels large"
        )[0].findAll(class_='chartValue')

        poss_vals = check_top_largeLabels[1].findAll(class_='chartValue')

        # GROUP 6: Set Pieces 
        sp_charts = four_tables[2].findAll(class_='countChart')

        # list of scrums and then lineouts. 00 is home scrums, 10 is home lineouts 
        h_a_set_pieces = [
            [
                sp_charts[0].findAll(class_='countLabel')[0].text,
                sp_charts[0].findAll(class_='countLabel')[1].text
            ],
            [
                sp_charts[1].findAll(class_='countLabel')[0].text,
                sp_charts[1].findAll(class_='countLabel')[1].text
            ]
        ]

        # GROUP 7: Defending 
        # list of lists, raw tackles and then home tackles 
        tackles = [
            four_tables[3].findAll(class_='home-team'),
            four_tables[3].findAll(class_='away-team')
        ]

        # GROUP 8: Discipline and Penalties 
        disc_rows = tables[3].find('tbody').findAll('td')
        penalty = stacked_rls[1].find(class_='countChart').findAll(
            class_='countLabel'
        )

        group_8_ordered = self.label_table_parse(group_8_labels, disc_rows)

        # Combine all group variables 
        top_data_dict = {
            'game_id': game_id,
            'league_id': league_id,

            # GROUP 1 metrics 
            'home_team': h_a_team_bar[0].find(class_='short-name').text,
            'home_team_score': int(h_a_team_bar[0].find(class_='score-container').text), 
            'away_team': h_a_team_bar[1].find(class_='short-name').text,
            'away_team_score': int(h_a_team_bar[1].find(class_='score-container').text),

            # GROUP 2 metrics 
            'home_tries': group_2_ordered[0][0],
            'away_tries': group_2_ordered[0][1],
            'home_conversions': group_2_ordered[1][0],
            'away_conversions': group_2_ordered[1][1],
            'home_penalty_goals': group_2_ordered[2][0],
            'away_penalty_goals': group_2_ordered[2][1],
            'home_kick_percent': group_2_ordered[3][0],
            'away_kick_percent': group_2_ordered[3][1],

            # GROUP 3 metrics 
            'home_total_meters': home_away_total_meters[0],
            'away_total_meters': home_away_total_meters[1],
            'home_kfh': group_3_ordered[0][0],
            'away_kfh': group_3_ordered[0][1],
            'home_pass_meters': group_3_ordered[1][0],
            'away_pass_meters': group_3_ordered[1][1],
            'home_runs': group_3_ordered[2][0],
            'away_runs': group_3_ordered[2][1],

            # GROUP 4 metrics 
            'home_possession_1h_2h': group_4_ordered[0][0],
            'home_territory_1h_2h': group_4_ordered[1][0],
            'home_clean_breaks': group_4_ordered[2][0],
            'home_defenders_beaten': group_4_ordered[3][0],
            'home_offloads': group_4_ordered[4][0],
            'home_rucks_won': group_4_ordered[5][0],
            'home_mauls_won': group_4_ordered[6][0],
            'home_turnovers_conceeded': group_4_ordered[7][0],
            'away_possession_1h_2h': group_4_ordered[0][1],
            'away_territory_1h_2h': group_4_ordered[1][1],
            'away_clean_breaks': group_4_ordered[2][1],
            'away_defenders_beaten': group_4_ordered[3][1],
            'away_offloads': group_4_ordered[4][1],
            'away_rucks_won': group_4_ordered[5][1],
            'away_mauls_won': group_4_ordered[6][1],
            'away_turnovers_conceeded': group_4_ordered[7][1],

            # GROUP 5 metrics 
            'home_total_possession': poss_vals[0].text,
            'home_total_territory': terr_vals[0].text,
            'away_total_possesion': poss_vals[1].text,
            'away_total_territory': terr_vals[1].text,

            # GROUP 6 metrics 
            'home_scrum': h_a_set_pieces[0][0],
            'home_lineout': h_a_set_pieces[1][0],
            'away_scrum': h_a_set_pieces[0][1],
            'away_lineout': h_a_set_pieces[1][1],

            # GROUP 7 metrics 
            'home_tackles': tackles[0][0].text, 
            'home_tackle_perc': tackles[1][0].text, 
            'away_tackles': tackles[0][1].text, 
            'away_tackle_perc': tackles[1][1].text,

            # GROUP 8 metrics 
            'home_red_cards': group_8_ordered[0][0],
            'home_yellow_cards': group_8_ordered[1][0],
            'home_free_kicks_con': group_8_ordered[2][0], 
            'away_red_cards': group_8_ordered[0][1],
            'away_yellow_cards': group_8_ordered[1][1],
            'away_free_kicks_con': group_8_ordered[2][1],
            'home_penalties': int(penalty[0].text),
            'away_penalties': int(penalty[1].text)

        }

        df = pd.DataFrame(top_data_dict, index=[0])
        
        return df 
    
    
    # Functions below are used for gathering team and league data based on the inputs to the class 
    def get_teams_list(self, season, league):
        
        test_url = 'https://www.espn.co.uk' + \
            '/rugby/table/_/league/{}/season/{}'.format(league, season)
        response = requests.get(test_url, headers=self.headers).content
        soup_team = BeautifulSoup(response, 'html.parser')

        get_index = lambda x, char: x.find(char)

        tbodies = soup_team.findAll('tbody')

        team_name_link = {}
        for tbody in tbodies:

            row_trs = tbody.findAll('tr')

            for tr in row_trs:

                td_start = tr.findAll('td')[0]
                try:
                    team_link = td_start.findAll('a')[0]['href']
                    
                    team_name_link[td_start.findAll('a')[1].find('span').text] = [
                        team_link,
                        team_link[team_link.find('id/')+3:team_link.find('id/')+3+\
                                  get_index(team_link[team_link.find('id/')+3:], '/')]
                    ]
                except:
                    team_link = None 
                    team_name_link[td_start.findAll('span')[3].findAll('span')[0].text] = [
                        team_link,
                        None
                    ]
    
        return team_name_link 
        
        
    def get_schedule(self, teams_list, league, season, team_input = None):
        
        if len(teams_list.keys()) == 0:
            sched_dict = {}
         
        elif team_input is not None:
            results_base_url = "https://www.espn.co.uk/rugby/results/_/team/"
            team_id = teams_list[team][1]

            team_page_resp = requests.get(
                results_base_url+team_id+'/league/{}/season/{}'.format(league, season), \
                headers=self.headers).content
            team_soup = BeautifulSoup(team_page_resp, 'html.parser')
            
            full_sched = team_soup.find(id='sched-container')
            match_months = full_sched.findAll('tbody')
        
        else:
            sched_dict = {}

            results_base_url = "https://www.espn.co.uk/rugby/results/_/team/"
            for team in teams_list.keys():
                team_id = teams_list[team][1]
                if team_id is not None:

                    team_page_resp = requests.get(
                        results_base_url+team_id+'/league/{}/season/{}'.format(league, season),
                        headers=self.headers).content
                    team_soup = BeautifulSoup(team_page_resp, 'html.parser')

                    full_sched = team_soup.find(id='sched-container')
                    match_months = full_sched.findAll('tbody')

                    sched_dict[team] = match_months 

        return sched_dict
        
    
    def parse_scheds(self, sched):
        
        totals = []
        for mon in sched:

            rows = mon.findAll('tr')

            for row in rows:

                first_row = row.findAll('td')
                date = first_row[0].text

                home_base, away_base = first_row[1].findAll('a')[0], \
                        first_row[2].findAll('a')[0]

                home_team, away_team = home_base.find('span').text, \
                        away_base.find('span').text

                home_team_abbr, away_team_abbr = home_base.find('abbr').text, \
                        away_base.find('abbr').text

                try:
                    game_link = first_row[1].findAll('span')[-1].find('a')['href']
                    game_id, league_id = game_link[
                        game_link.find('Id/')+3:game_link.find('/league')], \
                    game_link[game_link.find('league/')+7:]
                    score = first_row[1].findAll('span')[-1].find('a').text

                except TypeError:
                    game_link, game_id, league_id = np.nan, np.nan, np.nan 
                    score = first_row[1].findAll('span')[-1].text
        
                competition, stadium = first_row[4].text, first_row[5].text
                home_score, away_score = score.split()[0], score.split()[-1]

                totals.append([
                    date, home_team, away_team, home_team_abbr,
                    away_team_abbr, game_link, score, home_score,
                    away_score, competition, stadium, game_id, league_id
                ])

        comb_df = pd.DataFrame(
            totals,
            columns=[
                'date', 'home_team', 'away_team', 
                'home_team_abbr', 'away_team_abbr', 'game_link', 
                'score', 'home_score', 'away_score', 
                'competition', 'stadium', 'game_id', 'league_id']
        )
    
        return comb_df 
    
    
    def gather_season_teams(self, league, season):
        
        team_links = self.get_teams_list(season, league)
        team_sched = self.get_schedule(
            teams_list=team_links,
            league=league,
            season=season
        )
        
        comb_df = []
        for team in team_sched.keys():
            parsed_scheds = self.parse_scheds(team_sched[team])
            parsed_scheds['season'] = season
            comb_df.append(parsed_scheds)

        team_dfs = pd.concat(comb_df, axis=0, ignore_index=True).drop_duplicates()
        
        return team_dfs
    
    
    # def process_season_player_data(self, sched_df, limit=0):
        
    
    
if __name__ == '__main__':
    
    
    parser = ArgumentParser(
        description="Test Executions of Rugby Scraper Code Non-Production Version"
    )
    
    parser.add_argument(
        "--league", 
        "-l", 
        help="Specificy the league in which the scraper should pull from",
        type=str
    )
    
    parser.add_argument(
        "--season", 
        "-s", 
        help="Specificy the season in which the scraper should pull from",
        type=int
    )
    
    parser.add_argument(
        "--local_run", 
        "-r", 
        help="runtype to either pull from a local file or scrape from a ",
        default=False, 
        type=bool
    )
    
    parser.add_argument(
        "--data_pull_type", 
        "-dtt", 
        help="The groups of data that should be included: Game, Season, Player",
        type=str
    )
    
    parser.add_argument(
        "--filepath", 
        "-f", 
        help="filepath for where a local runtype should pull from",
        type=str
    )
    
    settings = parser.parse_args()
    
    league_dict = {
        'URC': 270557, 
        'Prem': 267979,
        'T14': 270559,
        'RWC': 164205,
        'RChamp': 244293,
        # 'ChampCup': 271937,
        # 'ChallCup': 272073,  
        'SR': 242041, 
        'PNCup': 256449
    }

    seasons = [
        2018, 2019, 2020, 
        2021, 2022, 2024
    ]

    league_pull, season_pull = league_dict[settings.league], \
                    settings.season
    
    match_scrape = scrape_model(
        league_set=[league_pull], 
        season_set=[season_pull], 
        update_type='single team',
        date_set=None
    )
    
    
    try:
        # pdb.set_trace()
        if settings.local_run is False:

            test_data_pull = match_scrape.gather_season_teams(
                league_pull,
                season=season_pull
            )

            print("team data for season pulled")
            team_data_join_back = test_data_pull[
                ['game_id', 'date', 'competition', 'season', 'stadium']
            ]

            game_dfs = []
            for game in tqdm(range(len(test_data_pull))):
                try:
                    if type(test_data_pull['game_id'].iloc[game]) == type('tester'):
                        game_dfs.append(match_scrape.get_match_stats(
                            game_id=test_data_pull['game_id'].iloc[game],
                            league_id=test_data_pull['league_id'].iloc[game],
                        ))
                except Exception as e: 
                    print(e)
                    pdb.set_trace()

            print('completed the data pull portion')
            all_teams_df = pd.concat(game_dfs, axis=0)
            all_teams_df = all_teams_df.merge(team_data_join_back, how='left', on='game_id')
            all_teams_df = match_scrape.clean_match_stats(all_teams_df)

            pdb.set_trace()
            
            print("pulling player data from game dataframes")
            match_scrape.start_up_driver()
            
            player_dfs = []
            for game in tqdm(range(len(all_teams_df.iloc[:5]))):
                try:
                    player_dfs.append(
                        match_scrape.get_player_stats(
                            game_id=all_teams_df.iloc[game]['game_id'],
                            league_id=all_teams_df.iloc[game]['league_id'])
                    )
                except:
                    print("Skipping game {}".format(all_teams_df.iloc[game]['game_id']))
            player_data = pd.concat(player_dfs, axis=0)
            
            pdb.set_trace()
            
            all_teams_df.to_csv('formed_data/game_data/{}_game_stats_{}.csv'.format(settings.league, season_pull), index=False)

        else:

            if settings.filepath is not None:
                test_data_pull = pd.read_csv(settings.filepath)

                print("team data for season pulled")
                team_data_join_back = test_data_pull[
                    ['game_id', 'date', 'competition', 'season', 'stadium']
                ]

                game_dfs = []
                for game in range(len(test_data_pull)):
                    try:
                        if type(test_data_pull['game_id'].iloc[game]) == type('tester'):
                            game_dfs.append(match_scrape.get_match_stats(
                                game_id=test_data_pull['game_id'].iloc[game],
                                league_id=test_data_pull['league_id'].iloc[game],
                            ))
                    except Exception as e: 
                        print(e)
                        pdb.set_trace()

                print('completed the data pull portion')
                all_teams_df = pd.concat(game_dfs, axis=0)
                all_teams_df = all_teams_df.merge(team_data_join_back, how='left', on='game_id')
                all_teams_df.to_csv('formed_data/{}_game_stats_{}.csv'.format(league_dict[settings.league], season_pull), index=False)

            else:
                print("Local run selected but no filepath provided")
    except KeyboardInterrupt: 
        if match_scrape.check_driver == True:
            match_scrape.close_out_driver()
    finally:
        if match_scrape.check_driver == True:
            match_scrape.close_out_driver()
        
        

        

    
    
    
