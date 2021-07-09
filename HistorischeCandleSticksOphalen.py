import uuid
import requests
import pandas as pd
import mysql.connector

def connect_met_database():
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

def tijden_inladen():
    # Het bestand waar de timestamps per maand in staan die we willen ophalen worden hier geladen
    f = open("tijden.txt", "r")
    # We splitsen alle waardes op de komma en gooien het in een lijst
    return f.read().split(", ")

def candlesticks_laden(tijden):
    # Lege lijst maken waar alle candlesticks in komen van de API
    historyDataList = []
    # Index bijhouden voor het itereren
    index = 0
    try:
        # For loop voor alle maanden in de tijden lijst
        for e in tijden:
            index += 1
            # Parameters instellen
            # symbol: de coin
            # interval: de lengte van de candlesticks
            # start/endTime: de periode waar je de candlesticks van wilt
            # limit: Hoeveel candlesticks je wilt ophalen (max 1000)
            parameters = {"symbol": "DOGEUSDT", "interval": "1h", "startTime": e, "endTime": (tijden[index]), "limit": "1000"}
            # De API link van Binance
            base_url = f"https://api.binance.com/api/v3/klines"
            # Verbinding maken met de API en de candlesticks van de opgegeven periode ophalen in een JSON
            r = requests.get(base_url, params=parameters).json()
            for list in r:
                # de lijst aanvullen met de elementen in de json
                historyDataList.append(list)

    except IndexError:
        # Bij het laatste element in de tijd lijst zal er een out of bounds error komen, hier houden we rekening mee.
        print("Index Out of bounds, regel overslaan. Alle maanden gehad.")
    return historyDataList


def list_to_dataframe(historyDataList):
    # De candlestick lijst omzetten naar een dataframe
    df = pd.DataFrame.from_records(historyDataList, columns=['open_timestamp', 'open', 'high', 'low', 'close', 'volume', 'close_timestamp',
                               'quote_asset_volumne', 'number_of_trades', 'taker_buy_base_asset_volume',
                               'taker_buy_quote_asset_volume', 'ignore'])
    return df

def dataframe_opschonen(df):
    # Dataframe opschonen
    # Onnodige kolommen weglaten
    df.drop(['quote_asset_volumne',
                                  'number_of_trades',
                                  'taker_buy_base_asset_volume',
                                  'taker_buy_quote_asset_volume',
                                  'ignore', 'volume'], axis=1, inplace=True)
    schoneDf = df
    # Kijken of er missing values zijn. Zo ja, printen we deze values en de hoeveelheid null values.
    missing_values = schoneDf.isnull().sum().sum()
    if missing_values.any():
        print('ERROR: DATA CONTAINS MISSING VALUES')
        print(missing_values)
    return schoneDf

def schrijf_naar_csv(schoneDf):
    # Deze functie is gemaakt voor als we de data ooit in een CSV nodig hebben.
    # De data wordt opgeslagen onder de naam historydata.csv
    schoneDf.to_csv(r'historydata.csv', index=False)

def schrijf_naar_staging(schoneDf, connection):
    # Alle candlesticks schrijven naar de staging area
    mycursor = connection.cursor()
    # De candlestick tabel leeghalen zodat er geen dubbele data in komt.
    mycursor.execute(f"TRUNCATE candlestick")
    try:
        for index, row in schoneDf.iterrows():
            # Voor elke rij in de dataframe voeren we een query uit
            mycursor.execute(f"INSERT INTO candlestick (open_timestamp, open, high, low, close, close_timestamp) VALUES({row['open_timestamp']}, {row['open']}, {row['high']}, {row['low']}, {row['close']}, {row['close_timestamp']})")
    except:
        # Als er errors zijn tijdens de queries worden de veranderingen niet opgeslagen en wordt de connectie en de code gesloten
        print("Tijdens het schrijven zijn er fouten. De code zal zichzelf nu sluiten, wijzigingen worden niet opgeslagen!")
        mycursor.close() # Cursor sluiten
        database_connectie_sluiten(connection, False)
    mycursor.close() # Cursor sluiten
    print("Wegschrijven naar staging area klaar!")

def database_connectie_sluiten(connection, bool):
    if bool:
        connection.commit() # Wijzigingen opslaan als dat moet
    connection.close() # Verbinding sluiten
    exit() # Code sluiten



def main():
    print("Code: Historische Candlesticks Ophalen en wegschrijven")
    connection = connect_met_database()
    tijden = tijden_inladen()
    historyDataList = candlesticks_laden(tijden)
    df = list_to_dataframe(historyDataList)
    schoneDf = dataframe_opschonen(df)
    # schrijf_naar_csv(schoneDf) # Als de data naar een CSV geexporteerd moet worden
    schrijf_naar_staging(schoneDf, connection)
    database_connectie_sluiten(connection, True)

if __name__ == '__main__':
    main()

