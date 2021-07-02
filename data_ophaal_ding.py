import csv
import requests
import pandas as pd
import utils


def tijden_inladen():
    f = open("tijden.txt", "r")
    return f.read().split(", ")

def candlesticks_laden(tijden):
    historyDataList = []
    index = 0
    try:
        for e in tijden:
            index += 1
            parameters = {"symbol": "DOGEUSDT", "interval": "1h", "startTime": e, "endTime": (tijden[index]), "limit": "1000"}
            base_url = f"https://api.binance.com/api/v3/klines"
            r = requests.get(base_url, params=parameters).json()
            for list in r:
                historyDataList.append(list)

    except IndexError:
        print("Hoi he")

    return historyDataList


def list_to_dataframe(historyDataList):
    df = pd.DataFrame.from_records(historyDataList, columns=['open_timestamp', 'open', 'high', 'low', 'close', 'volume', 'close_timestamp',
                               'quote_asset_volumne', 'number_of_trades', 'taker_buy_base_asset_volume',
                               'taker_buy_quote_asset_volume', 'ignore'])
    return df

def dataframe_opschonen(df):
    df.drop(['quote_asset_volumne',
                                  'number_of_trades',
                                  'taker_buy_base_asset_volume',
                                  'taker_buy_quote_asset_volume',
                                  'ignore', 'volume'], axis=1, inplace=True)
    schoneDf = df
    print(schoneDf)
    missing_values = schoneDf.isnull().sum().sum()
    if missing_values.any():
        print('ERROR: DATA CONTAINS MISSING VALUES')
        print(missing_values)
    return schoneDf

def schrijf_naar_csv(schoneDf):
    schoneDf.to_csv(r'historydata.csv', index=False)


def main():
    print("Welkom!")
    tijden = tijden_inladen()
    historyDataList = candlesticks_laden(tijden)
    df = list_to_dataframe(historyDataList)
    schoneDf = dataframe_opschonen(df)
    schrijf_naar_csv(schoneDf)




if __name__ == '__main__':
    main()

