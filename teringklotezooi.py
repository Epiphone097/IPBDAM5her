import math
import re
from datetime import datetime
import csv
import csv
import datetime
import math
import sqlite3
import time
import uuid
import numpy
import talib
from util import currency
import pandas as pd
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

with open('historydata3.csv', 'r', newline='') as csvfile:
    index = 0
    csvlist = []
    for line in csvfile:
        if index > 1:
            lineclean = line.split(",")
            newlist = []
            for line in lineclean:
                newlist.append(float(re.sub('[^A-Za-z0-9.]+', '', line)))
            csvlist.append(newlist)
        else:
            index += 1
    listwithtime = []
    for candle in csvlist:
        candle[0] = epoch_to_date_time(candle[0]).strftime("%m-%d-%Y %H:%M:%S")

        candle[5] = epoch_to_date_time(candle[5]).strftime("%m-%d-%Y %H:%M:%S")


        listwithtime.append(candle)
    df =pd.DataFrame(listwithtime)
    df.to_csv("Dikkenaam.csv")

# timestamp = 1545730073
# dt_obj = datetime.fromtimestamp(1140825600)