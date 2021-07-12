from datetime import datetime
import time
import requests
import pandas as pd
import mysql.connector

def verbind_met_db():
    # Verbinding maken met de mySQL database (staging)
    try:
        mydb = mysql.connector.connect(
            host="145.97.16.167",       # IP address
            user="python",              # mySQL user
            password="Welkom#01",       # password
            database='staging'          # schema in de database
        )
        print("Connection is gelukt.")
    except:
        # Mocht de connectie niet goed gaan sluiten we het script
        print("Connection is niet gelukt. Code sluiten.")
        exit()
    return mydb

def database_connectie_sluiten(connection, bool):
    if bool:
        connection.commit() # Wijzigingen opslaan als dat moet
    connection.close() # Verbinding sluiten
    exit() # Code sluiten

def sentiment_data_ophalen(conn):

    pd.set_option("display.max_rows", None, "display.max_columns", None)
    dfToCSV = pd.DataFrame()
    while True:
        try:
            nu = datetime.now() # Huidige tijd ophalen
            if True: # Op elke heel uur
                cur = conn.cursor()
                # Authorisatie parameters opslaan
                auth = requests.auth.HTTPBasicAuth('2tGCiuEPOsH1Ug', 'ri8tbdmNIRUHZPJ22PtFs0bXnV4k_g')
                # Login van Reddit voor sentimenten analyse (niet stelen)
                data = {'grant_type': 'password',
                        'username': 'arjannes',
                        'password': 'Welkom#01'}

                # API access token ophalen
                headers = {'User-Agent': 'MyBot/0.0.1'}
                res = requests.post('https://www.reddit.com/api/v1/access_token',
                                    auth=auth, data=data, headers=headers)
                TOKEN = res.json()['access_token']
                headers = {**headers, **{'Authorization': f"bearer {TOKEN}"}}

                # API aanroepen op de dogecoin reddit hot page
                res = requests.get("https://oauth.reddit.com/r/dogecoin/hot",
                                   headers=headers,
                                   params={'limit': '100'})
                df = pd.DataFrame() # Dataframe aanmaken
                for post in res.json()['data']['children']:
                    # Relevante data toevoegen aan dataframe
                    df = df.append({
                        'title': post['data']['title'],
                        'selftext': post['data']['selftext'],
                        'score': post['data']['score'],
                        'num_comments': post['data']['num_comments'],
                        'created_utc': post['data']['created_utc']

                    }, ignore_index=True)
                index = 0
                totaal = 0 # variabel bijhouden om score te berekenen
                for row in df.iterrows():
                    totaal += df.iloc[index][1] + df.iloc[index][2] # Comments en likes bijhouden om score te berekenen
                    index += 1
                print(nu.date(), nu.hour, totaal)
                dfToCSV = dfToCSV.append({'Date': nu.date(), 'Uur': nu.hour, 'Score': totaal}, ignore_index=True)
                print(dfToCSV)
                cur.execute(f"insert into sentiment values({nu.date()}, {nu.hour}, {totaal})")
                conn.commit()
                time.sleep(61)
        except:
            database_connectie_sluiten(conn, False)

def main():
    conn = verbind_met_db()
    sentiment_data_ophalen(conn)

if __name__ == '__main__':
    main()