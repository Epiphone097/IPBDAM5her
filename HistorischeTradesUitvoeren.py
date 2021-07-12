import datetime
import math
import uuid
import talib
import pandas as pd
import mysql.connector


# Alle patterns die talib ondersteund met de ranking erbij. Talib heeft dit nodig om de candlestick te kunnen bepalen.


PATTERN_RANKING = {
        "CDL3LINESTRIKE_Bull": 1,
        "CDL3LINESTRIKE_Bear": 2,
        "CDL3BLACKCROWS_Bull": 3,
        "CDL3BLACKCROWS_Bear": 3,
        "CDLEVENINGSTAR_Bull": 4,
        "CDLEVENINGSTAR_Bear": 4,
        "CDLTASUKIGAP_Bull": 5,
        "CDLTASUKIGAP_Bear": 5,
        "CDLINVERTEDHAMMER_Bull": 6,
        "CDLINVERTEDHAMMER_Bear": 6,
        "CDLMATCHINGLOW_Bull": 7,
        "CDLMATCHINGLOW_Bear": 7,
        "CDLABANDONEDBABY_Bull": 8,
        "CDLABANDONEDBABY_Bear": 8,
        "CDLBREAKAWAY_Bull": 10,
        "CDLBREAKAWAY_Bear": 10,
        "CDLMORNINGSTAR_Bull": 12,
        "CDLMORNINGSTAR_Bear": 12,
        "CDLPIERCING_Bull": 13,
        "CDLPIERCING_Bear": 13,
        "CDLSTICKSANDWICH_Bull": 14,
        "CDLSTICKSANDWICH_Bear": 14,
        "CDLTHRUSTING_Bull": 15,
        "CDLTHRUSTING_Bear": 15,
        "CDLINNECK_Bull": 17,
        "CDLINNECK_Bear": 17,
        "CDL3INSIDE_Bull": 20,
        "CDL3INSIDE_Bear": 56,
        "CDLHOMINGPIGEON_Bull": 21,
        "CDLHOMINGPIGEON_Bear": 21,
        "CDLDARKCLOUDCOVER_Bull": 22,
        "CDLDARKCLOUDCOVER_Bear": 22,
        "CDLIDENTICAL3CROWS_Bull": 24,
        "CDLIDENTICAL3CROWS_Bear": 24,
        "CDLMORNINGDOJISTAR_Bull": 25,
        "CDLMORNINGDOJISTAR_Bear": 25,
        "CDLXSIDEGAP3METHODS_Bull": 27,
        "CDLXSIDEGAP3METHODS_Bear": 26,
        "CDLTRISTAR_Bull": 28,
        "CDLTRISTAR_Bear": 76,
        "CDLGAPSIDESIDEWHITE_Bull": 46,
        "CDLGAPSIDESIDEWHITE_Bear": 29,
        "CDLEVENINGDOJISTAR_Bull": 30,
        "CDLEVENINGDOJISTAR_Bear": 30,
        "CDL3WHITESOLDIERS_Bull": 32,
        "CDL3WHITESOLDIERS_Bear": 32,
        "CDLONNECK_Bull": 33,
        "CDLONNECK_Bear": 33,
        "CDL3OUTSIDE_Bull": 34,
        "CDL3OUTSIDE_Bear": 39,
        "CDLRICKSHAWMAN_Bull": 35,
        "CDLRICKSHAWMAN_Bear": 35,
        "CDLSEPARATINGLINES_Bull": 36,
        "CDLSEPARATINGLINES_Bear": 40,
        "CDLLONGLEGGEDDOJI_Bull": 37,
        "CDLLONGLEGGEDDOJI_Bear": 37,
        "CDLHARAMI_Bull": 38,
        "CDLHARAMI_Bear": 72,
        "CDLLADDERBOTTOM_Bull": 41,
        "CDLLADDERBOTTOM_Bear": 41,
        "CDLCLOSINGMARUBOZU_Bull": 70,
        "CDLCLOSINGMARUBOZU_Bear": 43,
        "CDLTAKURI_Bull": 47,
        "CDLTAKURI_Bear": 47,
        "CDLDOJISTAR_Bull": 49,
        "CDLDOJISTAR_Bear": 51,
        "CDLHARAMICROSS_Bull": 50,
        "CDLHARAMICROSS_Bear": 80,
        "CDLADVANCEBLOCK_Bull": 54,
        "CDLADVANCEBLOCK_Bear": 54,
        "CDLSHOOTINGSTAR_Bull": 55,
        "CDLSHOOTINGSTAR_Bear": 55,
        "CDLMARUBOZU_Bull": 71,
        "CDLMARUBOZU_Bear": 57,
        "CDLUNIQUE3RIVER_Bull": 60,
        "CDLUNIQUE3RIVER_Bear": 60,
        "CDL2CROWS_Bull": 61,
        "CDL2CROWS_Bear": 61,
        "CDLBELTHOLD_Bull": 62,
        "CDLBELTHOLD_Bear": 63,
        "CDLHAMMER_Bull": 65,
        "CDLHAMMER_Bear": 65,
        "CDLHIGHWAVE_Bull": 67,
        "CDLHIGHWAVE_Bear": 67,
        "CDLSPINNINGTOP_Bull": 69,
        "CDLSPINNINGTOP_Bear": 73,
        "CDLUPSIDEGAP2CROWS_Bull": 74,
        "CDLUPSIDEGAP2CROWS_Bear": 74,
        "CDLGRAVESTONEDOJI_Bull": 77,
        "CDLGRAVESTONEDOJI_Bear": 77,
        "CDLHIKKAKEMOD_Bull": 82,
        "CDLHIKKAKEMOD_Bear": 81,
        "CDLHIKKAKE_Bull": 85,
        "CDLHIKKAKE_Bear": 83,
        "CDLENGULFING_Bull": 84,
        "CDLENGULFING_Bear": 91,
        "CDLMATHOLD_Bull": 86,
        "CDLMATHOLD_Bear": 86,
        "CDLHANGINGMAN_Bull": 87,
        "CDLHANGINGMAN_Bear": 87,
        "CDLRISEFALL3METHODS_Bull": 94,
        "CDLRISEFALL3METHODS_Bear": 89,
        "CDLKICKING_Bull": 96,
        "CDLKICKING_Bear": 102,
        "CDLDRAGONFLYDOJI_Bull": 98,
        "CDLDRAGONFLYDOJI_Bear": 98,
        "CDLCONCEALBABYSWALL_Bull": 101,
        "CDLCONCEALBABYSWALL_Bear": 101,
        "CDL3STARSINSOUTH_Bull": 103,
        "CDL3STARSINSOUTH_Bear": 103,
        "CDLDOJI_Bull": 104,
        "CDLDOJI_Bear": 104
    }

# Hier staan alle pattern in die we willen gebruiken voor de trades
patternArray = [
    "CDLBELTHOLD_Bull",
    "CDLHANGINGMAN_Bear",
    "CDLHIKKAKE_Bull"
]

# Functie om de epoch timestamp om te zetten naar een datetime object
def epoch_to_date_time(ts):
    if get_integer_places(ts) == 10:
        return datetime.datetime.fromtimestamp(ts)
    elif get_integer_places(ts) == 13:
        return datetime.datetime.fromtimestamp(ts / 1000)

# Functie die epoch_to_date_time gebruikt om de datetime object te bepalen
def get_integer_places(n):
    if n != 0:
        if abs(n) <= 999999999999997:
            return int(math.log10(abs(n))) + 1
        else:
            return int(math.log10(abs(n)))
    else:
        return 1


def check_signal(candles, candle_names):
    if candles[candle_names].values.sum() == 0:
        # Als candle_names (lijst met patterns die hij checkt) leeg is, return 0
        return 0, None
    else:
        indices = []
        for i in range(len(candles[candle_names].values[0])):
            if candles[candle_names].values[0][i] < 0 or candles[candle_names].values[0][i] > 0:
                # Kijk per pattern of deze herkend wordt in de candle stick die gecheckt wordt in de functie
                indices.append(i) # Voeg deze toe aan de indices list
        container = []
        for i in indices:
            if candles[candle_names].values[0][i] > 0: # Kijk of het een bull of een bear is en voeg dit toe achter de pattern naam
                container.append(candles[candle_names].keys()[i] + '_Bull')
            else:
                container.append(candles[candle_names].keys()[i] + '_Bear')
        rank_list = []
        for p in container:
            if p in PATTERN_RANKING:
                rank_list.append(p)
        if len(rank_list) == len(container):
            rank_index_best = rank_list.index(min(rank_list))
            pattern = container[rank_index_best]
            # Kijk of het een Bull of Bear is en return de pattern
            if 'Bull' in pattern:
                return 1, pattern
            else:
                return 2, pattern
        else:
            if 'Bull' in container[0]:
                return 1, container[0]
            else:
                return 2, container[0]


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


def reset_wallet(con):
    # Wallet resetten
    cur = con.cursor() # Cursortje maken
    cur.execute(f'truncate wallet') # Alle regels in wallet weghalen
    cur.execute(f'insert into wallet values ("1", 10000)') # Maak een wallet aan met een ID en 10k usd
    con.commit() # Verandering committen


def onnodige_tijden_verwijderen(con):
    # Tijden waarin we niet willen traden, halen we uit de historische data
    cur = con.cursor() # cursortje maken
    cur.execute('select * from candlestick') # historische data ophalen
    historydata = cur.fetchall()
    for row in historydata:
        tijd = epoch_to_date_time(int(row[1])) # Timestamp omzetten naar datetime object zodat het leesbaar is
        print(tijd)
        if tijd.hour < 9 or tijd.hour > 21: # Historische data voor 9 uur en na 21 uur worden eruit geknikkerd
            print("delete rij")
            cur.execute(f'delete from candlestick where open_timestamp = {row[1]}') # Knikker de rij eruit
        else:
            print("niet delete") # Niet knikkeren
    con.commit() # Commit de veranderingen

# Functie om alle historische trades uit te voeren
def history_trades(con):
    candle_names = talib.get_function_groups()['Pattern Recognition'] # Alle pattern namen ophalen
    temp = []
    for value in candle_names:
        if "_Bull" in value:
            value = value.replace("_Bull", "")
        elif "_Bear" in value:
            value = value.replace("_Bear", "")
        temp.append(value)
    candle_names = temp
    cur = con.cursor()
    cur.execute('truncate trade')
    con.commit()
    cur.execute('select * from candlestick') # Alle historische candlesticks ophalen
    historydata = cur.fetchall()
    candles = [] # Lege lijst waar we alle candlesticks in gaan zetten
    for row in historydata:
        # Voor elke rij in de historische data dict toevoegen aan de candles lijst
        candles.append({
                'date_time': int(row[1]),
                'open': float(row[2]),
                'close': float(row[5]),
                'high': float(row[3]),
                'low': float(row[4])
            })
    candles = pd.DataFrame(candles) # Dataframe maken van alle historische candlesticks
    # Timestamp omzetten naar leesbare datetime
    candles['date_time'] = candles['date_time'].astype('float64')
    candles['date_time'] = candles['date_time'].apply(epoch_to_date_time)
    # Alle waardes in variabelen zetten
    open = candles['open']
    high = candles['high']
    low = candles['low']
    close = candles['close']
    for candle in candle_names:
        candles[candle] = getattr(talib, candle)(open, high, low, close)
    candlesLen = len(candles.index) # Aantal elementen tellen
    for i in range(0, candlesLen-1):
        signal, pattern = check_signal(candles[i:i+1], candle_names) # Het signaal en pattern ophalen
        # Maak trades tot 20u want om 21u wordt gestopt met traden dus we willen niet kopen om 21u
        if candles['date_time'][i].hour < 21:
            if (signal == 1 and pattern in patternArray): # Bull signal en als de pattern in de patternArray staat
                # Bull signal
                # primary trade voor bull signaal (kopen van de coins)
                trade_id = uuid.uuid4() # random trade id genereren
                cur.execute("select wallet_id from wallet") # Wallet ID ophalen
                # Belangrijke trade info bijhouden in trade variabel
                trade = {'trade_id': trade_id, 'wallet_id': cur.fetchall()[0][0],
                         'timestamp': candles['date_time'][i]}
                cur.execute("select usd_value from wallet") # USD waarde ophalen vanuit wallet
                wallet = [{
                    'usd_value': (cur.fetchall()[0][0]) #-100
                }]
                trade['wallet_usd_value'] = wallet[0]['usd_value'] # Wallet waarde in trade zetten
                trade['taker_side'] = 1 # Taker side is 1 want wij kopen coins
                trade['original'] = 1 # Dit is de original/primary trade
                trade['usd_value'] = 100.00 # Wij kopen voor 100 dollar aan coins
                trade['doge_value'] = (trade['usd_value'] / candles['close'][i]) # Uitrekenen hoeveel dogecoin gekocht kan worden
                trade['doge_value'] = round(trade['doge_value'], 10) # De hoeveelheid gekochte dogecoin afronden op 10 decimalen

                # Coins gekocht dus geld gaat van onze wallet af
                cur.execute(
                    f"UPDATE wallet "
                    f"SET usd_value = usd_value - {trade['usd_value']}"
                )
                con.commit()
                # Alle trade eigenschappen worden opgeslagen in de database
                cur.execute(
                    f"INSERT INTO trade (trade_id, original, pattern,"
                    f"usd_value, doge_value, timestamp, taker_side, wallet_usd_value) "
                    f"VALUES ("
                    f"'{trade_id}', "
                    f"1, "
                    f"'{pattern}', "
                    f"'{trade['usd_value']}',"
                    f"'{trade['doge_value']}', "
                    f"'{trade['timestamp']}', "
                    f"'{trade['taker_side']}', "
                    f"'{trade['wallet_usd_value']}')"
                )
                con.commit()
                # Primary Trade completed (kopen)

                # Secondary trade (verkopen van de coins een uur later)
                trade_id2 = uuid.uuid4()
                cur.execute("select wallet_id from wallet")
                trade2 = {'trade_id': trade_id2, 'wallet_id': cur.fetchall()[0][0],
                         'timestamp': candles['date_time'][i+1]}
                cur.execute("select usd_value from wallet")
                wallet2 = [{
                    'usd_value': cur.fetchall()[0][0]
                }]
                trade2['wallet_usd_value'] = wallet2[0]['usd_value']
                trade2['taker_side'] = 0
                trade2['original'] = 0
                trade2['doge_value'] = trade['doge_value']
                trade2['usd_value'] = trade['doge_value'] * candles['close'][i+1]
                trade['usd_value'] = round(trade['usd_value'], 2)
                trade['doge_value'] = round(trade['doge_value'], 10)

                # Geld toevoegen aan de wallet (coins verkocht)
                cur.execute(
                    f"UPDATE wallet "
                    f"SET usd_value = usd_value + {trade2['usd_value']}"
                )
                con.commit()
                cur.execute("select usd_value from wallet")
                new_wallet_usd = cur.fetchall()[0][0] # Nieuwe wallet value ophalen

                # Trade eigenschappen opslaan in de database
                cur.execute(
                    f"INSERT INTO trade (trade_id, original_id, original, pattern,"
                    f"usd_value, doge_value, timestamp, taker_side, wallet_usd_value) "
                    f"VALUES ("
                    f"'{trade_id2}', "
                    f"'{trade_id}', "
                    f"0, "
                    f"'', "
                    f"'{trade2['usd_value']}',"
                    f"'{trade2['doge_value']}', "
                    f"'{trade2['timestamp']}', "
                    f"'{trade2['taker_side']}', "
                    f"'{new_wallet_usd}')"
                )
                con.commit()
                # Secondary trade completed (verkopen)

            elif (signal == 2 and pattern in patternArray): # Bearish signal
                # primary trade (Short gaan, dus verkopen wat we niet hebben)
                trade_id = uuid.uuid4()
                cur.execute("select wallet_id from wallet")
                trade = {'trade_id': trade_id, 'wallet_id': cur.fetchall()[0][0],
                         'timestamp': candles['date_time'][i]}
                cur.execute("select usd_value from wallet")
                wallet = [{
                    'usd_value': (cur.fetchall()[0][0])
                }]
                trade['wallet_usd_value'] = wallet[0]['usd_value']
                trade['taker_side'] = 0 # Signal 0 want wij zijn de verkopende partij
                trade['original'] = 1
                trade['usd_value'] = 100.00

                trade['doge_value'] = (trade['usd_value'] / candles['close'][i])
                trade['usd_value'] = round(trade['usd_value'], 2)
                trade['doge_value'] = round(trade['doge_value'], 10)

                # Short gaan dus wij verkopen voor 100 usd aan coins
                cur.execute(
                    f"UPDATE wallet "
                    f"SET usd_value = usd_value + {trade['usd_value']}"
                )
                con.commit()
                # Trade eigenschappen bijhouden
                cur.execute(
                    f"INSERT INTO trade (trade_id, original, pattern,"
                    f"usd_value, doge_value, timestamp, taker_side, wallet_usd_value) "
                    f"VALUES ("
                    f"'{trade_id}', "
                    f"1, "
                    f"'{pattern}', "
                    f"'{trade['usd_value']}',"
                    f"'{trade['doge_value']}', "
                    f"'{trade['timestamp']}', "
                    f"'{trade['taker_side']}', "
                    f"'{trade['wallet_usd_value']}')"
                )
                con.commit()
                # Primary trade completed (verkopen)

                # secondary trade (terugkopen)
                trade_id2 = uuid.uuid4()
                cur.execute("select wallet_id from wallet")
                trade2 = {'trade_id': trade_id2, 'wallet_id': cur.fetchall()[0][0],
                          'timestamp': candles['date_time'][i + 1]}
                cur.execute("select usd_value from wallet")
                wallet2 = [{
                    'usd_value': cur.fetchall()[0][0]
                }]
                trade2['wallet_usd_value'] = wallet2[0]['usd_value']
                trade2['taker_side'] = 1
                trade2['original'] = 0
                trade2['doge_value'] = trade['doge_value']
                trade2['usd_value'] = trade['doge_value'] * candles['close'][i + 1]
                trade['usd_value'] = round(trade['usd_value'], 2)
                trade['doge_value'] = round(trade['doge_value'], 10)

                # Terugkopen hoeveelheid coins die we verkocht hadden in de primary trade
                cur.execute(
                    f"UPDATE wallet "
                    f"SET usd_value = usd_value - {trade2['usd_value']}"
                )
                con.commit()
                cur.execute("select usd_value from wallet")
                new_wallet_usd = cur.fetchall()[0][0] # Nieuwe wallet waarde ophalen

                # Trade eigenschappen opslaan
                cur.execute(
                    f"INSERT INTO trade (trade_id, original_id, original, pattern,"
                    f"usd_value, doge_value, timestamp, taker_side, wallet_usd_value) "
                    f"VALUES ("
                    f"'{trade_id2}', "
                    f"'{trade_id}', "
                    f"0, "
                    f"'', "
                    f"'{trade2['usd_value']}',"
                    f"'{trade2['doge_value']}', "
                    f"'{trade2['timestamp']}', "
                    f"'{trade2['taker_side']}', "
                    f"'{new_wallet_usd}')"
                )
                con.commit()
                # secondary trade completed (terugkopen van verkochte coins)
            print("trade done")

def database_connectie_sluiten(connection, bool):
    if bool:
        connection.commit() # Wijzigingen opslaan als dat moet
    connection.close() # Verbinding sluiten
    print("Database verbinding gesloten")
    exit() # Code sluiten

def main():
    print("Welkom!")
    con = verbind_met_db()
    reset_wallet(con)

    # Uncommenten als je nieuwe data gebruikt zodat de historische data opgeschoond wordt
    # onnodige_tijden_verwijderen(con)

    history_trades(con)
    database_connectie_sluiten(con, True)


if __name__ == '__main__':
    main()
