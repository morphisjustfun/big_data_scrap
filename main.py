import requests
from bs4 import BeautifulSoup
import pandas as pd
import json


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
    df_n = pd.DataFrame(columns=columns)
    ids = df['href'].apply(lambda x: x.split('/')[-1].split('-')[0])
    for id in ids:
        try:
            r = session.get(f'https://www.futwiz.com/en/app/price_history_player23_multi?p={id}').text
            r = r.replace('\n', '')
            j = json.loads(r)
            if not j:
                continue
            pc = j['pc']
            console = j['console']
            for p in pc:
                df_n = pd.concat([df_n, pd.DataFrame({'id': [id], 'type': ['pc'], 'timestamp': [p[0]], 'price': [p[1]]})],
                                    ignore_index=True)
            for c in console:
                df_n = pd.concat([df_n, pd.DataFrame({'id': [id], 'type': ['console'], 'timestamp': [c[0]], 'price': [c[1]]})],
                                    ignore_index=True)
        except:
            df_n.to_csv('players_2023_history.csv', index=False)
            continue

    df_n.to_csv('players_2023_history.csv', index=False)

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
    for id in ids:
        try:
            dic = fifa2023_stats_player(session, id)
            df_n = pd.concat([df_n, pd.DataFrame(dic, index=[0])], ignore_index=True)
        except:
            df_n.to_csv('players_2023_stats.csv', index=False)
            continue

    df_n.to_csv('players_2023_stats.csv', index=False)



if __name__ == '__main__':
    fifa2023_stats()
    
# json.loads('{"console":[[1686265200000,4547690],[1686351600000,3718818],[1686438000000,3308972],[1686524400000,3355300],[1686610800000,3259754],[1686697200000,3135017],[1686783600000,3177060],[1686870000000,3271231],[1686956400000,3014333],[1687042800000,2672286],[1687129200000,2502000],[1687215600000,2600000],[1687302000000,2811200],[1687388400000,2766333],[1687474800000,2835000],[1687561200000,2835000],[1687647600000,2680000],[1687734000000,2680000],[1687820400000,2803667],[1687906800000,2830000],[1687993200000,2724667],[1688079600000,2580000],[1688166000000,2580000],[1688252400000,2580000],[1688338800000,2580000],[1688425200000,1853600],[1688511600000,2029000],[1688598000000,2029000],[1688684400000,2029000],[1688770800000,1870500],[1688857200000,1870500],[1688943600000,1870500],[1689030000000,1845000],[1689116400000,1845000],[1689202800000,2050000],[1689289200000,2050000],[1689375600000,1833000],[1689462000000,1833000],[1689548400000,1500000],[1689634800000,1500000],[1689721200000,1564500],[1689807600000,1220500],[1689894000000,1219000],[1689980400000,1424250],[1690066800000,1624500],[1690153200000,1442000],[1690239600000,2000000],[1690326000000,1756000],[1690412400000,1688067],[1690498800000,1709667],[1690585200000,1709000],[1690671600000,1729000],[1690758000000,1729000],[1690844400000,1817000],[1690930800000,1892000],[1691017200000,1618222],[1691103600000,1708000],[1691190000000,1524000],[1691276400000,1456667],[1691362800000,1456667],[1691449200000,1335000],[1691535600000,1333000],[1691622000000,1342429],[1691708400000,1290000],[1691794800000,999000],[1691881200000,999000],[1691967600000,815833],[1692054000000,816333],[1692140400000,871800],[1692226800000,900000],[1692313200000,859500],[1692399600000,859500],[1692486000000,859500],[1692572400000,859500],[1692658800000,859500],[1692745200000,859500],[1692831600000,638250],[1692918000000,638250],[1693004400000,638250],[1693090800000,499000],[1693177200000,499000],[1693263600000,512500],[1693350000000,543500],[1693436400000,543500],[1693522800000,543500],[1693609200000,285000],[1693695600000,301448],[1693782000000,276000],[1693868400000,325000],[1693954800000,335000],[1694041200000,335000],[1694127600000,335000],[1694214000000,335000],[1694300400000,335000],[1694386800000,335000],[1694473200000,335000],[1694559600000,679250],[1694646000000,679250],[1694732400000,679250],[1694818800000,679250],[1694905200000,679250],[1694991600000,679250],[1695078000000,679250],[1695164400000,679250],[1695250800000,679250]]}
# ')
# import pandas as pd
# df = pd.read_csv('players_2023.csv')
