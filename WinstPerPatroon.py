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

def dictAanmaken(conn):
    try:
        patroonwinstDict = {}
        index = 1
        cur = conn.cursor()
        cur.execute("SELECT * FROM trade") # Alle trades ophalen
        data = cur.fetchall()
        for i in range(0, int(len(data) / 2)):
            patroon = data[index][3]                        # Pak patroon naam
            voorPrijs = data[index][-1]                     # De prijs voor de trade
            naPrijs = data[index+1][-1]                     # De prijs na de trade
            verschil = float(naPrijs) - float(voorPrijs)    # De winst/verlies van de trade berekenen
            if patroon in patroonwinstDict:                 # Als de patroon al bestaat in de dictionary
                patroonwinstDict[patroon].append(verschil)
            else:                                           # Patroon toevoegen in de dictionary
                patroonwinstDict[patroon] = [verschil]
            index += 2                                      # Index 2 erbij omdat 1 trade bestaat uit 2 rows
        return patroonwinstDict
    except:
        print("Er is iets misgegaan met het ophalen van de data!")
        database_connectie_sluiten(conn, False)

def winstEnVerliesBerekenen(patroonDict):
    winstVerliesDict = {}
    # Voor elk patroon voegen we een key toe met de totale opsomming van alle waardes (dus de winst of verlies)
    for key, value in patroonDict.items():
        verschil = sum(value)
        winstVerliesDict[key] = verschil
    return winstVerliesDict


def totaleWinstVerliesBerekenen(winstVerliesDict):
    allePrijzen = []
    # Alle winst/verlies waardes bij elkaar optellen om de totale winst/verlies te berekenen
    for key, value in winstVerliesDict.items():
        allePrijzen.append(value)
    totaleWinstVerlies = sum(allePrijzen)
    return totaleWinstVerlies

def winstVerliesDictWegschrijven(winstVerliesDict, naam):
    dataframe = pd.DataFrame(winstVerliesDict.items())  # De dictionary omzetten naar een dataframe
    dataframe.to_csv(naam)                              # Dataframe exporteren naar een CSV bestand


def main():
    conn = verbind_met_db()
    patroonDict = dictAanmaken(conn)
    winstVerliesDict = winstEnVerliesBerekenen(patroonDict)
    totaleWinstVerlies = totaleWinstVerliesBerekenen(winstVerliesDict)
    winstVerliesDictWegschrijven(winstVerliesDict, "WinstVerliesExport.csv")
    database_connectie_sluiten(conn, False)



if __name__ == '__main__':
    main()