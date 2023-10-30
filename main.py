import multiprocessing
from multiprocessing import Pool
from random import random

import requests
from bs4 import BeautifulSoup
import pandas as pd
import json
import time


def fifa2024_players():
    base_page = 'https://www.futwiz.com/en/fc24/players'
    base_page_href = 'https://www.futwiz.com'
    session = requests.Session()
    max_page = 662
    min_page = 0

    columns = ['name', 'href']
    df = pd.DataFrame(columns=columns)

    for i in range(min_page, max_page + 1):
        request = session.get(base_page + '?page=' + str(i))
        soup = BeautifulSoup(request.text, 'html.parser')
        divs = soup.find_all('div', {'class': 'player-name'})
        for div in divs:
            link = div.find('a')['href']
            name = div.find('a').find('b').text
            df = pd.concat([df, pd.DataFrame({'name': [name], 'href': [base_page_href + link]})], ignore_index=True)

    df.to_csv('players_2024.csv', index=False)


def fifa2023_players():
    base_page = 'https://www.futwiz.com/en/fifa23/players'
    base_page_href = 'https://www.futwiz.com'
    session = requests.Session()
    min_page = 0
    max_page = 1007

    columns = ['name', 'href']
    df = pd.DataFrame(columns=columns)

    for i in range(min_page, max_page + 1):
        request = session.get(base_page + '?page=' + str(i))
        soup = BeautifulSoup(request.text, 'html.parser')
        tds = soup.find_all('td', {'class': 'player'})
        for td in tds:
            if td.find('a'):
                firstP = td.find_all('p')[0]
                lastA = firstP.find_all('a')[-1]
                href = lastA['href']
                href = base_page + href
                text = lastA.text
                df = pd.concat([df, pd.DataFrame({'name': [text], 'href': [href]})], ignore_index=True)

    df.to_csv('players_2023.csv', index=False)
    # https://www.futwiz.com/es/app/price_history_player23_multi?p=1775


def fifa2023_history():
    df = pd.read_csv('players_2023.csv')
    session = requests.Session()
    columns = ['id', 'type', 'timestamp', 'price']
    columns_not_found = ['id']
    df_n = pd.DataFrame(columns=columns)
    df_not_found = pd.DataFrame(columns=columns_not_found)
    ids = df['href'].apply(lambda x: x.split('/')[-1].split('-')[0])

    df_n.to_csv('players_2023_history.csv', index=False)
    df_not_found.to_csv('players_2023_history_not_found.csv', index=False)

    for index, id in enumerate(ids):
        try:
            r = session.get(f'https://www.futwiz.com/en/app/price_history_player23_multi?p={id}').text
            r = r.replace('\n', '')
            j = json.loads(r)
            if not j:
                continue
            pc = j['pc']
            console = j['console']
            for p in pc:
                print(p)
                df_n = pd.concat(
                    [df_n, pd.DataFrame({'id': [id], 'type': ['pc'], 'timestamp': [p[0]], 'price': [p[1]]})],
                    ignore_index=True)
            for c in console:
                df_n = pd.concat(
                    [df_n, pd.DataFrame({'id': [id], 'type': ['console'], 'timestamp': [c[0]], 'price': [c[1]]})],
                    ignore_index=True)
        except:
            df_not_found = pd.concat([df_not_found, pd.DataFrame({'id': [id]})], ignore_index=True)
            time.sleep(0.1 + 0.3 * random())
            continue
        if index % 100 == 0:
            print(index, '/', len(ids))
            df_n.to_csv('players_2023_history.csv', index=False, mode='a', header=False)
            df_not_found.to_csv('players_2023_history_not_found.csv', index=False, mode='a', header=False)
            df_n = pd.DataFrame(columns=columns)
            df_not_found = pd.DataFrame(columns=columns_not_found)

    df_not_found.to_csv('players_2023_history_not_found.csv', index=False, mode='a', header=False)
    df_n.to_csv('players_2023_history.csv', index=False, mode='a', header=False)


def fifa2023_stats_player(session, id):
    r = session.get(f'https://www.futwiz.com/en/app/sold23/{id}/console').text
    r = r.replace('\n', '')
    j = json.loads(r)
    return j['player']


def fifa2023_stats():
    df = pd.read_csv('players_2023.csv')
    session = requests.Session()
    df_n = pd.DataFrame()
    ids = df['href'].apply(lambda x: x.split('/')[-1].split('-')[0])
    for i, id in enumerate(ids):
        print(f'{i}/{len(ids)}')
        try:
            time.sleep(1)
            dic = fifa2023_stats_player(session, id)
            df_n = pd.concat([df_n, pd.DataFrame(dic, index=[0])], ignore_index=True)
            if i % 100 == 0:
                df_n.to_csv('players_2023_stats.csv', index=False, header=False, mode='a')
                df_n = pd.DataFrame()
        except Exception as e:
            # print id to error file
            with open('error.txt', 'a') as f:
                f.write(id + '\n')
            df_n.to_csv('players_2023_stats.csv', index=False, header=False, mode='a')
            df_n = pd.DataFrame()

    df_n.to_csv('players_2023_stats.csv', index=False, header=False, mode='a')

def fifa2021_players():
    base_page = 'https://www.futwiz.com/en/fifa21/players'
    base_page_href = 'https://www.futwiz.com'
    session = requests.Session()
    max_page = 908
    min_page = 0

    columns = ['name', 'href']
    df = pd.DataFrame(columns=columns)

    for i in range(min_page, max_page + 1):
        request = session.get(base_page + '?page=' + str(i))
        soup = BeautifulSoup(request.text, 'html.parser')
        divs = soup.find_all('td', {'class': 'player'})
        divs = divs[1:]
        for div in divs:
            link = div.find('a')['href']
            name = div.find('a').find('b').text
            print(base_page_href + link)
            df = pd.concat([df, pd.DataFrame({'name': [name], 'href': [base_page_href + link]})], ignore_index=True)

    df.to_csv('players_2021.csv', index=False)

def fifa2021_history_thread(core_id):
    df = pd.read_csv('players_2021.csv')
    session = requests.Session()
    columns = ['id', 'type', 'timestamp', 'price']
    columns_not_found = ['id']
    df_n = pd.DataFrame(columns=columns)
    df_not_found = pd.DataFrame(columns=columns_not_found)
    df = df.iloc[core_id::4]
    ids = df['href'].apply(lambda x: x.split('/')[-1].split('-')[0])

    for index, id in enumerate(ids):
        try:
            r = session.get(f'https://www.futwiz.com/en/app/price_history_player21_multi?p={id}').text
            r = r.replace('\n', '')
            j = json.loads(r)
            if not j:
                continue
            keys = j.keys()
            for k in keys:
                for p in j[k]:
                    df_n = pd.concat(
                        [df_n, pd.DataFrame({'id': [id], 'type': [k], 'timestamp': [p[0]], 'price': [p[1]]})],
                        ignore_index=True)
        except:
            df_not_found = pd.concat([df_not_found, pd.DataFrame({'id': [id]})], ignore_index=True)
            time.sleep(0.1 + 0.3 * random())
            continue
        if index % 100 == 0:
            df_n.to_csv(f'players_2021_history_{core_id}.csv', index=False, mode='a', header=False)
            df_not_found.to_csv(f'players_2021_history_not_found_{core_id}.csv', index=False, mode='a', header=False)
            df_n = pd.DataFrame(columns=columns)
            df_not_found = pd.DataFrame(columns=columns_not_found)

    df_n.to_csv(f'players_2021_history_{core_id}.csv', index=False, mode='a', header=False)
    df_not_found.to_csv(f'players_2021_history_not_found_{core_id}.csv', index=False, mode='a', header=False)

def fifa_2021_history():
    columns = ['id', 'type', 'timestamp', 'price']
    columns_not_found = ['id']
    df_n = pd.DataFrame(columns=columns)
    df_not_found = pd.DataFrame(columns=columns_not_found)
    n_multiprocessing = 4
    for i in range(n_multiprocessing):
        df_n.to_csv(f'players_2021_history_{i}.csv', index=False)
        df_not_found.to_csv(f'players_2021_history_not_found_{i}.csv', index=False)
    pool = Pool(n_multiprocessing)
    # pass the function the id of the thread
    pool.map(fifa2021_history_thread, range(n_multiprocessing))


if __name__ == '__main__':
    fifa_2021_history()
