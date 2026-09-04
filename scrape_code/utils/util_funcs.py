import pandas as pd 
import numpy as np 
import json, os, time, pdb 


def get_proportion_values(x, char, break_space=True):
    if break_space == True:
        space = 1
    else:
        space = 0
    
    if char == '/':
        return int(x[:x.find(char)-space])
    elif char == '(':
         return int(x[x.find('/')+space:x.find('(')-space])  
    elif char == ')':
        return int(x[x.find('(')+space:x.find(')')-space]) * 0.01

def slice_poss_terr_func(x):
    two_h_val = int(x[x.find('/')+1:].replace('%', '')) * 0.01
    one_h_val = int(x[:x.find('/')].replace('%', '')) * 0.01
    return one_h_val, two_h_val
