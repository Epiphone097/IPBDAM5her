import pandas as pd
import csv



def dataInladen():
    # df = pd.read_csv(r'trade_export.csv')
    # print(df)
    data = []
    with open('trade.csv') as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        for row in csv_reader:
            data.append(row)
    return data

def dictAanmaken(data):
    patroonwinstDict = {}
    index = 1

    for i in range(0, int(len(data) / 2)):
        patroon = data[index][3]
        voorPrijs = data[index][-1]
        naPrijs = data[index+1][-1]
        verschil = float(naPrijs) - float(voorPrijs)
        # print(f"Pattern: {patroon}, voorprijs: {voorPrijs} en naprijs: {naPrijs}. Winst/verlies is: {round(verschil, 10)}")
        if patroon in patroonwinstDict:
            patroonwinstDict[patroon].append(verschil)
        else:
            patroonwinstDict[patroon] = [verschil]
        # print(patroonwinstDict)
        index += 2
        # print(index)
    # print("klaar")
    return patroonwinstDict

def winstEnVerliesBerekenen(patroonDict):
    winstVerliesDict = {}
    for key, value in patroonDict.items():
        verschil = sum(value)
        winstVerliesDict[key] = verschil
    return winstVerliesDict


def totaleWinstVerliesBerekenen(winstVerliesDict):
    allePrijzen = []
    for key, value in winstVerliesDict.items():
        allePrijzen.append(value)
    totaleWinstVerlies = sum(allePrijzen)
    return totaleWinstVerlies

def winstVerliesDictWegschrijven(winstVerliesDict):
    dataframe = pd.DataFrame(winstVerliesDict.items())
    dataframe.to_csv("winstverliesexport3.csv")


def main():
    data = dataInladen()
    patroonDict = dictAanmaken(data)
    winstVerliesDict = winstEnVerliesBerekenen(patroonDict)
    totaleWinstVerlies = totaleWinstVerliesBerekenen(winstVerliesDict)
    winstVerliesDictWegschrijven(winstVerliesDict)



if __name__ == '__main__':
    main()