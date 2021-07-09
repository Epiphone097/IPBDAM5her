import datetime
import math
import sqlite3
import uuid
import talib
import pandas as pd

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

patternArray = [
    "CDLBELTHOLD_Bull",
    "CDLHANGINGMAN_Bear",
    "CDLHIKKAKE_Bull"
]

def epoch_to_date_time(ts):
    if get_integer_places(ts) == 10:
        return datetime.datetime.fromtimestamp(ts)
    elif get_integer_places(ts) == 13:
        return datetime.datetime.fromtimestamp(ts / 1000)


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
        return 0, None

    else:
        # patterns = list(numpy.compress(self.current_candle[self.candle_names].keys(), self.current_candle[self.candle_names].values != 0))
        indices = []
        patterns = []
        for i in range(len(candles[candle_names].values[0])):
            if candles[candle_names].values[0][i] < 0 or candles[candle_names].values[0][i] > 0:indices.append(i)
        container = []
        for i in indices:
            if candles[candle_names].values[0][i] > 0:
                container.append(candles[candle_names].keys()[i] + '_Bull')
            else:
                container.append(candles[candle_names].keys()[i] + '_Bear')
        # print(container)
        rank_list = []
        for p in container:
            if p in PATTERN_RANKING:
                rank_list.append(p)
        if len(rank_list) == len(container):
            rank_index_best = rank_list.index(min(rank_list))
            pattern = container[rank_index_best]
            if 'Bull' in pattern:
                return 1, pattern
            else:
                return 2, pattern
        else:
            if 'Bull' in container[0]:
                return 1, container[0]
            else:
                return 2, container[0]


def verbind_met_db(dbnaam):
    con = sqlite3.connect(dbnaam)
    return con


def reset_wallet(con):
    cur = con.cursor()
    cur.execute(f'delete from wallet')
    cur.execute(f'insert into wallet values ("1", 10000, 10000)')
    con.commit()


def onnodige_tijden_verwijderen(con):
    cur = con.cursor()
    historydata = cur.execute('select * from historydata').fetchall()
    for row in historydata:
        tijd = epoch_to_date_time(row[0])
        print(tijd)
        if tijd.hour < 9 or tijd.hour > 21:
            print("delete rij")
            cur.execute(f'delete from historydata where open_timestamp = {row[0]}')
        else:
            print("niet delete")
    con.commit()




def history_trades(con):
    # candle_names2 = talib.get_function_groups()['Pattern Recognition']
    # candle_names = PATTERN_RANKING
    candle_names = talib.get_function_groups()['Pattern Recognition']
    temp = []
    for value in candle_names:
        if "_Bull" in value:
            value = value.replace("_Bull", "")
        elif "_Bear" in value:
            value = value.replace("_Bear", "")
        temp.append(value)
    candle_names = temp
    # candle_names = PATTERN_RANKING
    cur = con.cursor()
    historydata = cur.execute('select * from historydata').fetchall()

    candles = []
    for row in historydata:
        candles.append({
                'date_time': row[0],
                'open': row[1],
                'close': row[2],
                'high': row[3],
                'low': row[4]
            })
    candles = pd.DataFrame(candles)
    candles['date_time'] = candles['date_time'].astype('float64')
    candles['date_time'] = candles['date_time'].apply(epoch_to_date_time)
    open = candles['open']
    high = candles['high']
    low = candles['low']
    close = candles['close']
    for candle in candle_names:
        candles[candle] = getattr(talib, candle)(open, high, low, close)
    candlesLen = len(candles.index)
    for i in range(0, candlesLen-1):
        signal, pattern = check_signal(candles[i:i+1], candle_names)
        # print(f"Signal given: {signal} | Pattern given: {pattern}")
        original_id = 0
        doge_amount = 0
        if candles['date_time'][i].hour < 21:
            # print(candles['date_time'][i])
            if (signal == 1) and pattern in patternArray:
                # primary trade

                trade_id = uuid.uuid4()
                trade = {'trade_id': trade_id, 'wallet_id': cur.execute("select id from wallet").fetchall()[0][0],
                         'timestamp': candles['date_time'][i]}
                wallet = [{
                    'usd_value': (cur.execute("select usd from wallet").fetchall()[0][0]) #-100
                }]
                trade['wallet_usd_value'] = wallet[0]['usd_value']
                if signal == 1:
                    trade['taker_side'] = 1
                else:
                    trade['taker_side'] = 0
                trade['original'] = 1
                trade['usd_value'] = 100.00

                trade['doge_value'] = (trade['usd_value'] / candles['close'][i])
                trade['usd_value'] = round(trade['usd_value'], 2)
                trade['doge_value'] = round(trade['doge_value'], 10)
                if signal == 1:
                    cur.execute(
                        f"UPDATE wallet "
                        f"SET usd = usd - {trade['usd_value']}"
                    )
                    cur.execute(
                        f"INSERT INTO trade (trade_id, original, pattern, eur_value,"
                        f"usd_value, doge_value, timestamp, taker_side, wallet_usd_value) "
                        f"VALUES ("
                        f"'{trade_id}', "
                        f"1, "
                        f"'{pattern}', "
                        f"0, "
                        f"'{trade['usd_value']}',"
                        f"'{trade['doge_value']}', "
                        f"'{trade['timestamp']}', "
                        f"'{trade['taker_side']}', "
                        f"'{trade['wallet_usd_value']}')"
                    )
                # print("primary trade completed")

                #secondary trade
                trade_id2 = uuid.uuid4()
                trade2 = {'trade_id': trade_id2, 'wallet_id': cur.execute("select id from wallet").fetchall()[0][0],
                         'timestamp': candles['date_time'][i+1]}
                wallet2 = [{
                    'usd_value': cur.execute("select usd from wallet").fetchall()[0][0]
                }]
                trade2['wallet_usd_value'] = wallet2[0]['usd_value']
                trade2['taker_side'] = 0
                trade2['original'] = 0
                trade2['doge_value'] = trade['doge_value']
                # Convert €100,- to USD and DOGE
                trade2['usd_value'] = trade['doge_value'] * candles['close'][i+1]
                trade['usd_value'] = round(trade['usd_value'], 2)
                trade['doge_value'] = round(trade['doge_value'], 10)
                #selling coins

                cur.execute(
                    f"UPDATE wallet "
                    f"SET usd = usd + {trade2['usd_value']}"
                )
                new_wallet_usd = cur.execute("select usd from wallet").fetchall()[0][0]
                cur.execute(
                    f"INSERT INTO trade (trade_id, original_id, original, pattern, eur_value,"
                    f"usd_value, doge_value, timestamp, taker_side, wallet_usd_value) "
                    f"VALUES ("
                    f"'{trade_id2}', "
                    f"'{trade_id}', "
                    f"0, "
                    f"'', "
                    f"0, "
                    f"'{trade2['usd_value']}',"
                    f"'{trade2['doge_value']}', "
                    f"'{trade2['timestamp']}', "
                    f"'{trade2['taker_side']}', "
                    f"'{new_wallet_usd}')"
                )
                # print(candles['date_time'][i])
            elif (signal == 2) and pattern in patternArray:
                # primary trade
                trade_id = uuid.uuid4()
                trade = {'trade_id': trade_id, 'wallet_id': cur.execute("select id from wallet").fetchall()[0][0],
                         'timestamp': candles['date_time'][i]}
                wallet = [{
                    'usd_value': (cur.execute("select usd from wallet").fetchall()[0][0]) #+100
                }]
                trade['wallet_usd_value'] = wallet[0]['usd_value']
                if signal == 1:
                    trade['taker_side'] = 1
                else:
                    trade['taker_side'] = 0
                trade['original'] = 1
                trade['usd_value'] = 100.00

                trade['doge_value'] = (trade['usd_value'] / candles['close'][i])
                trade['usd_value'] = round(trade['usd_value'], 2)
                trade['doge_value'] = round(trade['doge_value'], 10)

                cur.execute(
                    f"UPDATE wallet "
                    f"SET usd = usd + {trade['usd_value']}"
                )
                cur.execute(
                    f"INSERT INTO trade (trade_id, original, pattern, eur_value,"
                    f"usd_value, doge_value, timestamp, taker_side, wallet_usd_value) "
                    f"VALUES ("
                    f"'{trade_id}', "
                    f"1, "
                    f"'{pattern}', "
                    f"0, "
                    f"'{trade['usd_value']}',"
                    f"'{trade['doge_value']}', "
                    f"'{trade['timestamp']}', "
                    f"'{trade['taker_side']}', "
                    f"'{trade['wallet_usd_value']}')"
                )
                # print("primary trade completed")

                # secondary trade
                trade_id2 = uuid.uuid4()
                trade2 = {'trade_id': trade_id2, 'wallet_id': cur.execute("select id from wallet").fetchall()[0][0],
                          'timestamp': candles['date_time'][i + 1]}
                wallet2 = [{
                    'usd_value': cur.execute("select usd from wallet").fetchall()[0][0]
                }]
                trade2['wallet_usd_value'] = wallet2[0]['usd_value']
                trade2['taker_side'] = 1
                trade2['original'] = 0
                trade2['doge_value'] = trade['doge_value']
                # Convert €100,- to USD and DOGE
                trade2['usd_value'] = trade['doge_value'] * candles['close'][i + 1]
                trade['usd_value'] = round(trade['usd_value'], 2)
                trade['doge_value'] = round(trade['doge_value'], 10)
                # selling coins

                cur.execute(
                    f"UPDATE wallet "
                    f"SET usd = usd - {trade2['usd_value']}"
                )
                new_wallet_usd = cur.execute("select usd from wallet").fetchall()[0][0]
                cur.execute(
                    f"INSERT INTO trade (trade_id, original_id, original, pattern, eur_value,"
                    f"usd_value, doge_value, timestamp, taker_side, wallet_usd_value) "
                    f"VALUES ("
                    f"'{trade_id2}', "
                    f"'{trade_id}', "
                    f"0, "
                    f"'', "
                    f"0, "
                    f"'{trade2['usd_value']}',"
                    f"'{trade2['doge_value']}', "
                    f"'{trade2['timestamp']}', "
                    f"'{trade2['taker_side']}', "
                    f"'{new_wallet_usd}')"
                )
                # print("secondary trade completed")
                con.commit()

                # time.sleep(2)


def main():
    print("Welkom!")
    con = verbind_met_db("historyDB.db")
    reset_wallet(con)
    # onnodige_tijden_verwijderen(con)
    history_trades(con)


if __name__ == '__main__':
    main()
