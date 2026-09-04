import pandas as pd 
import numpy as np 
from selenium import webdriver
from selenium.webdriver.common.by import By 

fields = [
            ['tries', 'try_assists', 'conversion_goals',
             'penalty_goals', 'drop_goals_converted', 'points'],
            ['-', 'passes', 'runs', 'meters_run',
             'clean_breaks', 'defenders_beaten', 'offloads', '-'],
            ['to_conceded', 'tackles', 'missed_tackles', 'lineouts_won'],
            ['penalties_conceded', 'yellows', 'reds']
         ]

def get_player_page_data_test(table, fields, team):
    group_df = [] 
    for player in range(1, len(table)):

        player_tag = table[player].find_elements(by=By.TAG_NAME, value='td')
        p_name = player_tag[0].text
        href_link = None
        p_id = np.nan
        # p_name, href_link = player_tag[0].text, player_tag[0].get_attribute('href')


        # p_id = href_link[href_link.find('player/')+7:href_link.find('.html')]
        pos = table[player].find_elements(by=By.TAG_NAME, value='span')[1].text

        stats = [
            float(i.text) for i in table[player].find_elements(
                by=By.TAG_NAME, value='td')[1:]
        ]
            # print(stats)

        group_df.append([p_name, pos, p_id, team]+stats)
    print(group_df[0])
    titles = ['p_name', 'position', 'p_id', 'team'] + fields
    print(titles)
    test_df = pd.DataFrame(group_df, columns=titles)
    
    return test_df 

def update_driver_page_test(pass_table, pass_label):
    labels = pass_table.find_elements(by=By.TAG_NAME,
            value='li')[pass_label].click()
