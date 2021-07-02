from datetime import datetime
import time

import requests
import pandas as pd

pd.set_option("display.max_rows", None, "display.max_columns", None)

while True:
    nu = datetime.now()
    if nu.minute == 0:

        auth = requests.auth.HTTPBasicAuth('2tGCiuEPOsH1Ug', 'ri8tbdmNIRUHZPJ22PtFs0bXnV4k_g')
        data = {'grant_type': 'password',
                'username': 'arjannes',
                'password': 'Welkom#01'}
        headers = {'User-Agent': 'MyBot/0.0.1'}
        res = requests.post('https://www.reddit.com/api/v1/access_token',
                            auth=auth, data=data, headers=headers)
        TOKEN = res.json()['access_token']
        headers = {**headers, **{'Authorization': f"bearer {TOKEN}"}}
        requests.get('https://oauth.reddit.com/api/v1/me', headers=headers)
        res = requests.get("https://oauth.reddit.com/r/dogecoin/hot",
                           headers=headers,
                           params={'limit': '100'})
        df = pd.DataFrame()
        for post in res.json()['data']['children']:
            # append relevant data to dataframe
            df = df.append({
                'title': post['data']['title'],
                'selftext': post['data']['selftext'],
                'score': post['data']['score'],
                'num_comments': post['data']['num_comments'],
                'created_utc': post['data']['created_utc']

            }, ignore_index=True)
        index = 0
        totaal = 0
        for jannes in df.iterrows():
            totaal += df.iloc[index][1] + df.iloc[index][2]
            # print("Rijtje: ", index, df.iloc[index][1] + df.iloc[index][2])
            index += 1
        print(nu.hour, nu.minute, totaal)
        time.sleep(61)


#print(nu.hour)
#df.to_csv('jannes.csv')