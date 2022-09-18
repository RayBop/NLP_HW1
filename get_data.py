import numpy as np
import pandas as pd

import json
import os
import random
import requests
import string

from bs4 import BeautifulSoup
from datetime import datetime

'''Generate a random email address'''
def _get_random_email():
    letter, number = random.choice(string.ascii_letters), random.randint(1, 1000)
    return f'{letter}{number}@gmail.com'

def _get_json_details(url_end):
    url = f'https://data.sec.gov/submissions/{url_end}'
    headers = {'User-Agent': 'abcdkjf@gmail.com'}
    req = requests.get(url, headers=headers).json()
    return req

'''Get the accession numbers and report dates for 10Q/K'''
def _get_acc_num(cik):
    req = _get_json_details(f'CIK{cik}.json')
    ex = req['filings']['recent']

    for i in range(len(req['filings']['files'])):
        ex_files = _get_json_details(req['filings']['files'][i]['name'])
        ex['accessionNumber'] += ex_files['accessionNumber']
        ex['reportDate'] += ex_files['reportDate']
        ex['form'] += ex_files['form']
        ex['primaryDocument'] += ex_files['primaryDocument']
        ex['filingDate'] += ex_files['filingDate']
    
    assert(len(ex['accessionNumber']) == len(ex['reportDate']))
    assert(len(ex['reportDate']) == len(ex['form']))
    assert(len(ex['primaryDocument']) == len(ex['accessionNumber']))
    assert(len(ex['filingDate']) == len(ex['accessionNumber']))
    
    inds = np.where((np.array(ex['form']) == '10-Q')|(np.array(ex['form']) == '10-K'))[0]
    acc_nums = np.array(ex['accessionNumber'])[inds]
    report_dts = np.array(ex['reportDate'])[inds]
    end_url = np.array(ex['primaryDocument'])[inds]
    file_dts = np.array(ex['filingDate'])[inds]
    
    exchanges = set(req['exchanges'])
    
    return acc_nums, report_dts, end_url, file_dts, exchanges

# '''Generate the url for the 10Q/K'''
# def _get_url(cik, acc_num, email):
#     acc_num_new = ''.join(acc_num.split('-'))
#     ex_url = f'https://www.sec.gov/Archives/edgar/data/{cik.lstrip("0")}/{acc_num_new}/{acc_num}-index.htm'

#     headers = {'User-Agent': email}
#     ex = requests.get(ex_url, headers=headers).content
#     url_end = BeautifulSoup(ex, features='lxml').find_all('div', {'id': 'formDiv'})[1].find_all('td')[2].text.split(' ')[0]
    
#     ret_url = f'https://www.sec.gov/Archives/edgar/data/{cik.lstrip("0")}/{acc_num_new}/{url_end}'
#     return ret_url

'''Get all the 10Q/K'''
def get_10Q(gvk, gvk_cik_map, gvk_dt_map):
    cik = gvk_cik_map[gvk]
    acc_nums, report_dts, url_end, file_dts, exchanges = _get_acc_num(cik)
    print(gvk, len(acc_nums), len(report_dts))
    all_10qs = {}
    
    if (exchanges == set()) or ('NYSE' in exchanges) or ('Nasdaq' in exchanges) or ('AMEX' in exchanges):
        for acc_num, dt, end, f_dt in zip(acc_nums, report_dts, url_end, file_dts):
            if (int(dt[:4]), int(dt[5:7])) in gvk_dt_map[gvk]:
                email = _get_random_email()
                headers = {'User-Agent': email}
                acc_num_new = ''.join(acc_num.split('-'))

                url = f'https://www.sec.gov/Archives/edgar/data/{cik.lstrip("0")}/{acc_num_new}/{end}'
                ex = requests.get(url, headers=headers).content
                
                ten_q = BeautifulSoup(ex, features='lxml')
                for i in ten_q.find_all('table'):
                    i.extract()
                
                for a in ten_q.find_all('a', href=True):
                    a.extract()
                
                ten_q = ten_q.get_text(' ')
                ten_q = ten_q[ten_q.find('UNITED STATES'):]
                ten_q = ten_q.replace('\xa0', ' ').encode('ascii', 'ignore').decode()
                
                all_10qs[f_dt] = ten_q

    return all_10qs

'''Take in the directory, gvkey, and lst of 10Q/K and store them'''
def write_out(out_dir, gvk, ten_lst, gvk_price_dt_map):
    if gvk not in os.listdir(out_dir):
        os.mkdir(f'{out_dir}/{gvk}')
    
    for k, v in ten_lst.items():
        if (int(k[:4]), int(k[5:7])) not in gvk_price_dt_map[gvk]:
            if len(v.split(' ')) >= 2000:
                with open(f'{out_dir}/{gvk}/{k}.txt', 'w') as f:
                    f.write(v)
                f.close()

if __name__ == '__main__':
    # Read the csv file that was given to us
    cols = ['date', 'permno', 'comnam', 'ticker', 'gvkey', 'start', 'ending', 'ret']
    sp_df = pd.read_csv('./data/sp500_w_addl_id.csv', index_col=[0])[cols].copy()
    sp_df['gvkey'] = sp_df.gvkey.astype(str)

    # Read in the compustat data just to get a gvkey o cik mapping
    com_df = pd.read_csv('./data/compustat_stuff.csv')
    gvk_cik_map = {i:j for i, j in zip(com_df.gvkey.values, com_df.cik.values)}
    
    # Bad values that needs to be set manually
    gvk_cik_map[1356] = 4281
    gvk_cik_map[137232] = 1065865
    gvk_cik_map[160256] = 1283699
    gvk_cik_map[179700] = 1418135
    gvk_cik_map = {i:str(int(j)).zfill(10) for i, j in gvk_cik_map.items()}

    # Map gvkey to a set of (year, month) tuples of when the company was in S&P500
    gvkeys = np.unique(sp_df.gvkey.values)
    gvk_dt_map = {}
    for gvk in gvkeys:
        gvk_ex = pd.to_datetime(sp_df[sp_df.gvkey==gvk]['date'].values)
        gvk_dt_map[int(gvk)] = set(zip(gvk_ex.year, gvk_ex.month))
    
    # This maps gvkey to a set of dates where stock was below 3 bucks
    price_df = pd.read_csv('./data/stock daily gvkey.csv')
    price_df['datadate'] = pd.to_datetime(price_df.datadate.values, format='%Y%m%d')

    price_dts = price_df[price_df['prccd'] < 3][['GVKEY', 'datadate']]

    gvk_price_dt_map = {i:set() for i in np.unique(price_df['GVKEY'].values)}
    for i in np.unique(price_dts['GVKEY'].values):
        ex_vals = price_dts[price_dts.GVKEY == i]['datadate'].dt
        gvk_price_dt_map[i] = set(zip(ex_vals.year, ex_vals.month))
    
    # Get the 10Q/K and store them
    ex_set = set(gvk_price_dt_map.keys())
    gvk_cik_key_set = set(gvk_cik_map.keys())
    for gvk in gvkeys.astype(int):
        print('starting', gvk)
        if str(gvk) in os.listdir('/Volumes/TOSHIBA2TB/nlp/the_tens'):
            print(gvk, 'already there')
        elif gvk not in gvk_cik_key_set:
            print(gvk, 'no cik here')
        elif gvk in ex_set:
            ten_lst = get_10Q(gvk, gvk_cik_map, gvk_dt_map)
            write_out('/Volumes/TOSHIBA2TB/nlp/the_tens', gvk, ten_lst, gvk_price_dt_map)
            print(gvk, 'done')
        else:
            print('what the heck', gvk)