import datetime
import math
import uuid
import talib
import pandas as pd
import mysql.connector

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

def verbind_met_db():
    # Verbinding maken met de mySQL database (staging)
    try:
        mydb = mysql.connector.connect(
            host="145.97.16.167",       # IP address
            user="python",              # mySQL user
            password="Welkom#01"       # password
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
    print("Database verbinding gesloten")
    exit() # Code sluiten

def datamarts_legen(con):
    cur = con.cursor()
    tabellen = ["trade", "patroon", "portemonnee", "candlestick"]
    for tabel in tabellen:
        cur.execute(f'truncate dm.{tabel}')
    con.commit()

def incremental_tijd_ophalen(con):
    cur = con.cursor()
    # Laatste moment bekijken dat dit script gedraait heeft in de audittrail
    cur.execute("select max(datetime) from dm.audit where script is HistorischeTradesVanStagingNaarDM.py")
    datetime_incremental = cur.fetchall()
    return datetime_incremental[0]

def staging_naar_datamarts_candlestick(con, incremental_tijd, incremental):
    cur = con.cursor()
    # candlestick tabel ophalen van staging
    cur.execute('select * from staging.candlestick')
    candlesticktabel = cur.fetchall()
    for row in candlesticktabel:
        datetime = row[1].astype('float64')
        # Als incremental load aan staat en de regel is nieuwer dan het laatste moment dat dit script gedraait heeft
        if incremental and datetime > incremental_tijd:
            datetime = datetime.apply(epoch_to_date_time)
            datum_key = datetime.strftime("%Y%m%d") # primary key van datum samenvoegen zodat die uit de datum tabel gebruikt kan worden
            tijd_key = datetime.strftime("%H%M%S")  # primary key van tijd samenvoegen zodat die uit de tijd tabel gebruikt kan worden
            # Regel wegschrijven naar de datamart
            cur.execute(f'insert into dm.candlestick values({row[0]}, {datum_key}, {tijd_key}, {row[2]}, {row[5]}, {row[3]}, {row[4]})')


def staging_naar_datamarts_trade(con, incremental_datetime, incremental):
    cur = con.cursor()
    # trade tabel ophalen van staging
    cur.execute('select * from staging.trade')
    tradetabel = cur.fetchall()
    for row in tradetabel:
        datetime = row[5].astype('float64')
        # Als incremental load aan staat en de regel is nieuwer dan het laatste moment dat dit script gedraait heeft
        if incremental and datetime > incremental_datetime:
            datetime = datetime.apply(epoch_to_date_time)
            datum_key = datetime.strftime("%Y%m%d") # primary key van datum samenvoegen zodat die uit de datum tabel gebruikt kan worden
            tijd_key = datetime.strftime("%H%M%S")  # primary key van tijd samenvoegen zodat die uit de tijd tabel gebruikt kan worden
            cur.execute(f"select patroon_key from dm.patroon where naam is {row[2]}") # Primary key van het patroon van de trade pakken
            patroon_key = cur.fetchall()
            # Regel wegschrijven naar de datamart
            cur.execute(f'insert into dm.trade values({row[0]}, {row[1]}, {patroon_key[0]}, {datum_key}, {tijd_key}, {row[6]}, {row[4]}, {row[5]}, {row[7]})')

def main():
    print("Welkom")
    incremental = True
    con = verbind_met_db()
    if not incremental:
        datamarts_legen(con)
    incremental_datetime = incremental_tijd_ophalen(con)
    staging_naar_datamarts_candlestick(con, incremental_datetime, incremental)
    staging_naar_datamarts_trade(con, incremental_datetime, incremental)
    database_connectie_sluiten(con, True)

if __name__ == '__main__':
    main()