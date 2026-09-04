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
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'utils'))
from util_funcs import *
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException, WebDriverException
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import datetime


_SCRAPERS_DIR = os.path.dirname(os.path.abspath(__file__))
_DATA_DIR = os.path.join(_SCRAPERS_DIR, '..', 'formed_data')


class scrape_model:
    """
    ESPN rugby statistics scraper covering schedule, match, and player data.

    Uses requests + BeautifulSoup for static match stats pages and Selenium
    ChromeDriver for JS-rendered player stats tabs. Supports multiple leagues
    and seasons via ESPN's public rugby URLs.

    League IDs and season years are passed at init; the typical scraping flow is:
        gather_season_teams()  →  get_match_stats()  →  get_player_stats()
    """

    def __init__(self, league_set, season_set, date_set, update_type: str, driver_type='backend'):
        """
        Parameters
        ----------
        league_set : list[int]
            ESPN numeric league IDs (e.g., 270557 for URC).
        season_set : list[int]
            Season years to scrape (e.g., [2023, 2024]).
        date_set : any
            Date filter for incremental updates (currently unused).
        update_type : str
            Describes the scope of the run (e.g., 'single team', 'full').
        driver_type : str
            'backend' launches headless Chrome; 'testing' opens a visible browser.
        """
        self.league_set = league_set
        self.season_set = season_set
        self.update_type = update_type
        self.driver_type = driver_type

        self.headers = {
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64)' + \
            'AppleWebKit/537.36 (KHTML, like Gecko) ' + \
           'Chrome/119.0.0.0 Safari/537.36',
           'User-Agent-Chrome-Windows': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) ' + \
            'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36'
        }


    def start_up_driver(self):
        """
        Initialize the Selenium ChromeDriver.

        In 'testing' mode opens a visible browser window for inspection.
        In 'backend' mode runs headless Chrome with a spoofed user-agent to
        reduce the chance of ESPN's bot detection blocking the request.
        """
        if self.driver_type == 'testing':
            self.driver = webdriver.Chrome()
        else:
            options = webdriver.ChromeOptions()
            options.add_argument('--headless=new')
            options.add_argument("--window-size=1920,1080")
            options.add_argument("--disable-gpu")
            options.add_argument('--disable-dev-shm-usage')
            options.add_argument("user-agent={}".format(self.headers['User-Agent']))
            self.driver = webdriver.Chrome(options=options)


    def check_driver(self):
        """
        Check whether the WebDriver session is still alive.

        Returns True if the driver responds, False if it has crashed or quit.
        """
        try:
            url_status = self.driver.current_url
            status = True
        except WebDriverException as e:
            status = False
        return status


    def close_out_driver(self):
        """Close the browser window and terminate the ChromeDriver process."""
        self.driver.close()
        self.driver.quit()


    def label_table_parse(self, labels, table):
        """
        Parse a flat list of ESPN table cells into [home, away] value pairs.

        ESPN match stat tables interleave label cells with home/away value cells.
        This filters out known label strings and pairs the remaining consecutive
        values as [home_val, away_val].

        Parameters
        ----------
        labels : list[str]
            Label strings to exclude (e.g., ['Tries', 'Conversion Goals']).
        table : list[Tag]
            Flat list of BeautifulSoup <td> elements from the stat table.

        Returns
        -------
        list[list[str, str]]
            List of [home_value, away_value] pairs in label order.
        """
        row_step = []
        for row in table:
            val_text = row.text
            if val_text not in labels:
                row_step.append(val_text)

        row_ordered = [[row_step[i-1], row_step[i]] \
                       for i in range(1, len(row_step), 2)]

        return row_ordered


    def get_game_date(self, game_id, league_id):
        """
        Fetch and parse the game date for a single match from ESPN.

        Hits the matchstats page for the given game, extracts the
        'game-date-time' element, and returns a datetime.date object.

        Parameters
        ----------
        game_id : str
            ESPN game identifier.
        league_id : str or int
            ESPN league identifier.

        Returns
        -------
        datetime.date
        """
        url = 'https://www.espn.com/rugby/matchstats/_/gameId/{}/league/{}'.format(
            game_id, league_id
        )
        response = requests.get(url, headers=self.headers).content
        soup = BeautifulSoup(response, 'html.parser')
        date_parse = soup.find(class_='game-date-time').text

        hour = date_parse[:date_parse.find(':')]
        minute = date_parse[date_parse.find(':')+1:date_parse.find(' ')]
        am_pm = date_parse[date_parse.find(' ')+1:date_parse.find(',')]

        month_day = date_parse[date_parse.find(',')+2:-6]
        month, day = datetime.datetime.strptime(month_day.split(' ')[0], '%b').month, month_day.split(' ')[1]
        year = date_parse[-4:]

        return datetime.date(int(year), int(month), int(day))


    def parse_game_date(self, date_parse):
        """
        Parse a date string from ESPN's game-date-time element into a datetime.date.

        Expected input format: "HH:MM AM/PM, Mon DD, YYYY"
        (e.g., "2:30 PM, Jan 15, 2024")

        Parameters
        ----------
        date_parse : str
            Raw date string scraped from the ESPN page.

        Returns
        -------
        datetime.date
        """
        hour = date_parse[:date_parse.find(':')]
        minute = date_parse[date_parse.find(':')+1:date_parse.find(' ')]
        am_pm = date_parse[date_parse.find(' ')+1:date_parse.find(',')]

        month_day = date_parse[date_parse.find(',')+2:-6]
        month, day = datetime.datetime.strptime(month_day.split(' ')[0], '%b').month, month_day.split(' ')[1]
        year = date_parse[-4:]

        return datetime.date(int(year), int(month), int(day))



    def slice_prop_func(self, x, char):
        """
        Extract a numeric component from ESPN's proportion string format "X/Y (Z%)".

        Parameters
        ----------
        x : str
            Proportion string, e.g., "14/18 (78%)".
        char : str
            Which component to extract:
            '/'  → X, the successful count (numerator)
            '('  → Y, the total attempts (denominator)
            ')'  → Z as a decimal, the success rate (e.g., 0.78 for 78%)

        Returns
        -------
        int or float
        """
        x = x.replace(" ", "")
        if char == '/':
            return int(x[:x.find(char)])
        elif char == '(':
             return int(x[x.find('/')+1:x.find('(')])
        elif char == ')':
            return int(x[x.find('(')+1:x.find('%')]) * 0.01


    def slice_poss_terr_func(self, x):
        """
        Parse ESPN's first-half/second-half possession or territory string.

        Input format: "X%/Y%" (e.g., "52%/48%")

        Parameters
        ----------
        x : str
            Combined H1/H2 string from the ESPN page.

        Returns
        -------
        tuple[float, float]
            (first_half_value, second_half_value) as decimals (e.g., 0.52, 0.48).
        """
        x = x.replace(" ", "")
        two_h_val = int(x[x.find('/')+1:].replace('%', '')) * 0.01
        one_h_val = int(x[:x.find('/')].replace('%', '')) * 0.01
        return one_h_val, two_h_val


    def clean_match_stats(self, df):
        """
        Expand raw proportion/percentage strings in a match stats DataFrame into
        numeric columns.

        For each side (home, away), this method:
        - Converts kick_percent strings to decimals
        - Splits 1H/2H possession and territory strings into separate _1h/_2h columns
        - Expands rucks_won, mauls_won, scrum, and lineout proportion strings into
          three columns each: _num (successes), _total_num (attempts), _percent (rate)

        NaN and N/A values in proportion columns are replaced with '0/0 (0%)' before
        parsing to avoid errors on incomplete data.

        Parameters
        ----------
        df : pd.DataFrame
            Raw match stats DataFrame as returned by get_match_stats().

        Returns
        -------
        pd.DataFrame
            The same DataFrame with additional cleaned numeric columns appended.
        """
        perc_fix = lambda x: 0.0 if x == 'N/A' else (0.0 if x == '' else int(x.replace("%", ""))*0.01)

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
            except Exception as e:
                print('annoying error')
                print(e)
                print(side)
                pdb.set_trace()

            for var_group in prop_variable_names:

                # Replace NaN placeholder strings before parsing proportion values
                if len(df[df['{}_{}'.format(side, var_group)].str.contains('NaN')]) > 0:

                    df.loc[
                        df['{}_{}'.format(side, var_group)].str.contains('NaN'),
                        '{}_{}'.format(side, var_group)] = '0/0 (0%)'

                # scrum and lineout columns don't carry 'won' in their raw name
                if 'won' in var_group:
                    str_attach = ''
                else:
                    str_attach = 'won_'
                try:

                    df.loc[
                        df['{}_{}'.format(side, var_group)].str.contains('N/A'),
                        '{}_{}'.format(side, var_group)] = '0/0 (0%)'

                    df.loc[
                        df['{}_{}'.format(side, var_group)] == '',
                        '{}_{}'.format(side, var_group)] = '0/0 (0%)'

                    df['{}_{}_{}'.format(side, var_group, 'num')] = \
                            df['{}_{}'.format(side, var_group)].apply(self.slice_prop_func, args=('/', ))
                    df['{}_{}_{}'.format(side, var_group, 'total_num')] = \
                            df['{}_{}'.format(side, var_group)].apply(self.slice_prop_func, args=('(', ))
                    df['{}_{}_{}'.format(side, var_group, str_attach+'percent')] = \
                            df['{}_{}'.format(side, var_group)].apply(self.slice_prop_func, args=(')', ))
                except Exception as e:
                    print("new annoying error")
                    print(e)
                    pdb.set_trace()

        return df


    def update_driver_to_page(self, pass_tab_labels, pass_label):
        """
        Click a tab in the player stats tab bar to load a specific stat group.

        Used internally by get_player_stats to cycle through ESPN's four stat tabs
        (Scoring, Attack, Defense, Discipline).

        Parameters
        ----------
        pass_tab_labels : WebElement
            The parent element containing the tab <li> items.
        pass_label : int
            Zero-based index of the tab to click.
        """
        labels = pass_tab_labels.find_elements(by=By.TAG_NAME, value='li')
        labels[pass_label].click()


    def get_player_page_data(self, table, fields, team):
        """
        Parse one stat tab's player table into a DataFrame.

        Extracts player name, player ID (from the href link), position, team side,
        and the numeric stat values for each player row.

        Parameters
        ----------
        table : list[WebElement]
            List of <tr> row elements from the stat table (index 0 is the header).
        fields : list[str]
            Column names matching the stat columns on this tab.
        team : str
            'home' or 'away', used to tag each player row.

        Returns
        -------
        pd.DataFrame
            Columns: ['p_name', 'position', 'p_id', 'team'] + fields
        """
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


    def get_player_page_data_v2(self, table, fields, team):
        """
        Alternate player page parser for ESPN layouts that omit player profile links.

        Functionally equivalent to get_player_page_data but falls back to NaN for
        player IDs when href links are not present in the table. This is the version
        currently used in production within get_player_stats.

        Parameters
        ----------
        table : list[WebElement]
            List of <tr> row elements (index 0 is the header row).
        fields : list[str]
            Column names matching the stat columns on this tab.
        team : str
            'home' or 'away'.

        Returns
        -------
        pd.DataFrame
            Columns: ['p_name', 'position', 'p_id', 'team'] + fields
            p_id will be NaN for all rows.
        """
        group_df = []
        for player in range(1, len(table)):

            player_tag = table[player].find_elements(by=By.TAG_NAME, value='td')
            p_name = table[player].find_elements(by=By.TAG_NAME, value='span')[0].text
            href_link = None
            p_id = np.nan

            pos = table[player].find_elements(by=By.TAG_NAME, value='span')[1].text

            stats = [
                float(i.text) for i in table[player].find_elements(
                    by=By.TAG_NAME, value='td')[1:]
            ]

            group_df.append([p_name, pos, p_id, team]+stats)
        titles = ['p_name', 'position', 'p_id', 'team'] + fields
        test_df = pd.DataFrame(group_df, columns=titles)

        return test_df


    def get_player_stats(self, game_id, league_id):
        """
        Scrape per-player statistics for a single match from ESPN's player stats page.

        Iterates through ESPN's four stat tabs using Selenium, parses each table for
        both home and away players, then merges all tabs into a single wide-format
        DataFrame. Requires the WebDriver to already be running (call start_up_driver first).

        Stat tabs and the fields collected from each:
            Tab 0 - Scoring:    tries, try_assists, conversion_goals,
                                penalty_goals, drop_goals_converted, points
            Tab 1 - Attack:     passes, runs, meters_run,
                                clean_breaks, defenders_beaten, offloads
            Tab 2 - Defense:    to_conceded, tackles, missed_tackles, lineouts_won
            Tab 3 - Discipline: penalties_conceded, yellows, reds

        Parameters
        ----------
        game_id : str
            ESPN game identifier.
        league_id : str or int
            ESPN league identifier.

        Returns
        -------
        pd.DataFrame
            One row per player with all stat columns merged, plus game_id and league_df.
        """
        fields = [
            ['tries', 'try_assists', 'conversion_goals',
             'penalty_goals', 'drop_goals_converted', 'points'],
            ['-', 'passes', 'runs', 'meters_run',
             'clean_breaks', 'defenders_beaten', 'offloads', '-'],
            ['to_conceded', 'tackles', 'missed_tackles', 'lineouts_won'],
            ['penalties_conceded', 'yellows', 'reds']
         ]

        page_url = "https://www.espn.co.uk/rugby/"+\
                "playerstats/_/gameId/{}/league/{}".format(game_id, league_id)
        self.driver.get(page_url)

        # Locate the stat tab bar — it's the 4th div inside the col-b container
        tab_labels = self.driver.find_element(
            by=By.CLASS_NAME, value='col-b').find_elements(
            by=By.TAG_NAME, value='div')[3]

        grouped_dfs = []
        for label in range(4):
            click_val = tab_labels.find_elements(by=By.TAG_NAME, value='li')[label]
            click_val.click()

            # First table is home team, second is away team
            group_table = self.driver.find_elements(by=By.TAG_NAME, value='table')
            home_group = group_table[0].find_elements(by=By.TAG_NAME, value='tr')
            away_group = group_table[1].find_elements(by=By.TAG_NAME, value='tr')

            comb_df = pd.concat(
                [
                    self.get_player_page_data_v2(home_group, fields[label], 'home'),
                    self.get_player_page_data_v2(away_group, fields[label], 'away')
                ], axis=0
            )
            grouped_dfs.append(comb_df)

        # Concatenate all four tab DataFrames horizontally and drop duplicate columns
        final_df = pd.concat(grouped_dfs, axis=1)
        final_df['game_id'], final_df['league_df'] = game_id, league_id
        final_df = final_df.loc[:, ~final_df.columns.duplicated()]

        return final_df


    def get_match_stats(self, game_id, league_id):
        """
        Scrape team-level match statistics for a single game from ESPN.

        Uses requests + BeautifulSoup (no Selenium required). Parses 8 stat groups
        from the ESPN matchstats page and returns them as a single-row DataFrame.

        Stat groups collected:
            1. Teams and final scores
            2. Match events: tries, conversions, penalty goals, kick %
            3. Kick/Pass/Run: total meters, kicks from hand, passes, runs
            4. Attack: clean breaks, defenders beaten, offloads, rucks,
                       mauls, turnovers, possession 1H/2H, territory 1H/2H
            5. Possession and territory totals
            6. Set pieces: scrums won, lineouts won
            7. Defense: tackles made, tackle success %
            8. Discipline: red cards, yellow cards, free kicks conceded, penalties

        Parameters
        ----------
        game_id : str
            ESPN game identifier.
        league_id : str or int
            ESPN league identifier.

        Returns
        -------
        pd.DataFrame
            Single-row DataFrame with all stats as columns, prefixed with 'home_'
            or 'away_' where applicable.
        """
        # URL declaration, scraping, and initial parsing to get the tables on the page
        url = 'https://www.espn.co.uk/rugby/matchstats/_/gameId/{}/league/{}'.format(
            game_id, league_id
        )
        response = requests.get(url, headers=self.headers).content
        soup = BeautifulSoup(response, 'html.parser')
        tables = soup.find_all('table')

        # Group labels used to filter label cells out of each stat table
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

        # Pre-select reusable parsed elements referenced by multiple groups
        four_tables = soup.find_all(
            class_='sub-module equal-height countChartList height-reset'
        )
        check_top_largeLabels = soup.find_all(
            class_="stat-graph compareLineGraph twoTeam largeLabels"
        )
        stacked_rls = soup.find_all(class_='stacked-rl')

        # GROUP 1: Home and Away team names and scores
        top_bar = soup.find(class_='competitors')

        h_a_team_bar = [
            top_bar.find(class_='team team-a'),
            top_bar.find(class_='team team-b')
        ]

        # GROUP 2: Match Events (tries, conversions, penalty goals, kick %)
        match_event = four_tables[0].find('tbody')
        match_event_rows = match_event.find_all('td')

        group_2_ordered = self.label_table_parse(group_2_labels, match_event_rows)

        # GROUP 3: Kick/Pass/Run — total meters comes from a separate chart element
        home_away_total_meters = [
            int(i.text) for i in check_top_largeLabels[0].find_all(class_='chartValue')
        ]

        meter_rows = four_tables[1].find('tbody').find_all('td')
        group_3_ordered = self.label_table_parse(group_3_labels, meter_rows)

        # GROUP 4: Attacking stats
        attack_rows = stacked_rls[0].find('tbody').find_all('td')
        group_4_ordered = self.label_table_parse(group_4_labels, attack_rows)

        # GROUP 5: Possession and territory totals (separate chart elements)
        terr_vals = soup.find_all(
            class_="stat-graph compareLineGraph twoTeam largeLabels large"
        )[0].find_all(class_='chartValue')

        poss_vals = check_top_largeLabels[1].find_all(class_='chartValue')

        # GROUP 6: Set Pieces — index 0 is scrums, index 1 is lineouts
        # Within each, index 0 is home, index 1 is away
        sp_charts = four_tables[2].find_all(class_='countChart')

        h_a_set_pieces = [
            [
                sp_charts[0].find_all(class_='countLabel')[0].text,
                sp_charts[0].find_all(class_='countLabel')[1].text
            ],
            [
                sp_charts[1].find_all(class_='countLabel')[0].text,
                sp_charts[1].find_all(class_='countLabel')[1].text
            ]
        ]

        # GROUP 7: Defense — index 0 is raw tackle counts, index 1 is tackle %
        tackles = [
            four_tables[3].find_all(class_='home-team'),
            four_tables[3].find_all(class_='away-team')
        ]

        # GROUP 8: Discipline and Penalties
        disc_rows = tables[3].find('tbody').find_all('td')
        penalty = stacked_rls[1].find(class_='countChart').find_all(
            class_='countLabel'
        )

        group_8_ordered = self.label_table_parse(group_8_labels, disc_rows)

        # Combine all group variables into a single flat dictionary
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


    def get_teams_list(self, season, league):
        """
        Fetch the list of teams for a given league and season from the ESPN standings page.

        Parameters
        ----------
        season : int
            Season year (e.g., 2024).
        league : int
            ESPN numeric league ID.

        Returns
        -------
        dict
            Maps team name (str) → [team_href (str), team_id (str)].
            team_id is None for teams where the href could not be parsed.
        """
        test_url = 'https://www.espn.co.uk' + \
            '/rugby/table/_/league/{}/season/{}'.format(league, season)
        response = requests.get(test_url, headers=self.headers).content
        soup_team = BeautifulSoup(response, 'html.parser')

        get_index = lambda x, char: x.find(char)

        tbodies = soup_team.find_all('tbody')
        # ESPN standings pages sometimes render two tbody elements per table (header + data);
        # taking every other one skips the duplicate header rows
        if len(tbodies) > 1:
            tbodies = tbodies[::2]

        team_name_link = {}
        for tbody in tbodies:

            row_trs = tbody.find_all('tr')
            for tr in row_trs:

                td_start = tr.find_all('td')[0]
                try:
                    team_link = td_start.find_all('a')[0]['href']

                    team_name_link[td_start.find_all('a')[-1].text] = [
                        team_link,
                        team_link[team_link.find('id/')+3:team_link.find('id/')+3+\
                                    get_index(team_link[team_link.find('id/')+3:], '/')]
                    ]
                except:
                    print(tr)
                    team_link = None
                    team_name_link[td_start.find_all('a')[2].text] = [
                        team_link,
                        None
                    ]

        return team_name_link


    def get_schedule(self, teams_list, league, season, team_input = None):
        """
        Fetch match schedule/results HTML for all teams in a league season.

        Hits ESPN's team results page for each team and returns the raw BeautifulSoup
        tbody elements (grouped by month) for later parsing by parse_scheds().

        Parameters
        ----------
        teams_list : dict
            Team name → [href, team_id] mapping as returned by get_teams_list().
        league : int
            ESPN numeric league ID.
        season : int
            Season year.
        team_input : str, optional
            If provided, fetches schedule for only this team. Currently incomplete —
            the full-league path (team_input=None) is the production code path.

        Returns
        -------
        dict
            Maps team name (str) → list of BeautifulSoup <tbody> elements,
            one element per month of results.
        """
        if len(teams_list.keys()) == 0:
            sched_dict = {}

        elif team_input is not None:
            results_base_url = "https://www.espn.co.uk/rugby/results/_/team/"
            team_id = teams_list[team_input][1]

            team_page_resp = requests.get(
                results_base_url+team_id+'/league/{}/season/{}'.format(league, season), \
                headers=self.headers).content
            team_soup = BeautifulSoup(team_page_resp, 'html.parser')
            # pdb.set_trace()
            full_sched = team_soup.find(id='sched-container')
            match_months = full_sched.find_all('tbody')
            sched_dict = {team_input: match_months}

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
                    match_months = full_sched.find_all('tbody')

                    sched_dict[team] = match_months

        return sched_dict


    def parse_scheds(self, sched):
        """
        Parse raw schedule BeautifulSoup elements for a single team into a DataFrame.

        Iterates month-grouped tbody elements returned by get_schedule() and
        extracts one row per match, including date, teams, score, game ID,
        league ID, competition name, and stadium.

        Parameters
        ----------
        sched : list[Tag]
            List of BeautifulSoup <tbody> elements, one per month of fixtures.

        Returns
        -------
        pd.DataFrame
            Columns: date, home_team, away_team, home_team_abbr, away_team_abbr,
                     game_link, score, home_score, away_score, competition,
                     stadium, game_id, league_id.
            Rows where game_link is unavailable (scheduled but unplayed) will have
            NaN for game_link, game_id, and league_id.
        """
        totals = []
        for mon in sched:

            rows = mon.find_all('tr')

            for row in rows:

                first_row = row.find_all('td')
                date = first_row[0].text

                home_base, away_base = first_row[1].find_all('a')[0], \
                        first_row[2].find_all('a')[0]

                home_team, away_team = home_base.find('span').text, \
                        away_base.find('span').text

                home_team_abbr, away_team_abbr = home_base.find('abbr').text, \
                        away_base.find('abbr').text

                try:
                    game_link = first_row[1].find_all('span')[-1].find('a')['href']
                    game_id, league_id = game_link[
                        game_link.find('Id/')+3:game_link.find('/league')], \
                    game_link[game_link.find('league/')+7:]
                    score = first_row[1].find_all('span')[-1].find('a').text

                # TypeError is raised when the game link element doesn't exist (future fixture)
                except TypeError:
                    game_link, game_id, league_id = np.nan, np.nan, np.nan
                    score = first_row[1].find_all('span')[-1].text

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
        """
        Fetch the full deduplicated schedule DataFrame for an entire league season.

        Convenience method that chains get_teams_list → get_schedule → parse_scheds
        for all teams, deduplicates (since each game appears in two teams' schedules),
        and returns a single combined DataFrame.

        Parameters
        ----------
        league : int
            ESPN numeric league ID.
        season : int
            Season year.

        Returns
        -------
        pd.DataFrame
            Deduplicated schedule for the full season with a 'season' column added.
        """
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
        'ChampCup': 271937,
        'ChallCup': 272073,
        'SR': 242041,
        'PNCup': 256449,
        'SixNat': 180659
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

    # pdb.set_trace()
    try:
        # pdb.set_trace()
        if settings.local_run is False:

            test_data_pull = match_scrape.gather_season_teams(
                league_pull,
                season=season_pull
            )

            print(f'Number of Games for League {league_pull} Season {season_pull}')
            test_data_pull.to_csv(os.path.join(_DATA_DIR, 'game_schedule_data', 'URC_schedule_data_{}.csv'.format(season_pull)), index=False)

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

            all_teams_df.to_csv(os.path.join(_DATA_DIR, 'game_data', '{}_game_stats_{}.csv'.format(settings.league, season_pull)), index=False)


            # pdb.set_trace()

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
            # pdb.set_trace()
            player_data = pd.concat(player_dfs, axis=0)
            player_data.to_csv(os.path.join(_DATA_DIR, 'player_data_mk2', '{}_player_stats_{}.csv'.format(settings.league, season_pull)), index=False)

            # pdb.set_trace()

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
                all_teams_df.to_csv(os.path.join(_DATA_DIR, '{}_game_stats_{}.csv'.format(league_dict[settings.league], season_pull)), index=False)

            else:
                print("Local run selected but no filepath provided")
    except KeyboardInterrupt:
        if match_scrape.check_driver == True:
            match_scrape.close_out_driver()
    finally:
        if match_scrape.check_driver == True:
            match_scrape.close_out_driver()








