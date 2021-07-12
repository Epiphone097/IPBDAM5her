import re
from datetime import datetime
import datetime
import math
import pandas as pd

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

def timestamps_omzetten_naar_datetime(bestandsnaam, opslaanals):
    with open(bestandsnaam, 'r', newline='') as csvfile: # csv bestand openen
        index = 0 # index bijhouden
        csvlist = []
        for line in csvfile:
            # elke rij in csv bestand splitten op de comma en cleanen
            if index > 1:
                lineclean = line.split(",")
                newlist = []
                for line in lineclean:
                    newlist.append(float(re.sub('[^A-Za-z0-9.]+', '', line)))
                csvlist.append(newlist) # toevoegen in nieuwe lijst
            else:
                index += 1
        listwithtime = []
        for candle in csvlist:
            # Timestamps omzetten naar datetime object zodat het leesbaar is
            candle[1] = epoch_to_date_time(candle[1]).strftime("%m-%d-%Y %H:%M:%S")
            candle[6] = epoch_to_date_time(candle[6]).strftime("%m-%d-%Y %H:%M:%S")
            listwithtime.append(candle)
        df = pd.DataFrame(listwithtime) # Lijst omzetten naar dataframe
        df.to_csv(opslaanals) # Dataframe exporteren naar csv bestand

def main():
    timestamps_omzetten_naar_datetime("historyexport.csv", "historyexportdatetime.csv") # bestandsnamen

if __name__ == '__main__':
    main()