import json

def loadJson(path):
    with open(path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    return data

def getHistory():
    history_data = loadJson('../db/history.json')
    dateList = list(history_data.keys())
    dateList.sort()
    dateList = dateList[-10:]

    result = {}
    for d in dateList:
        result[d] = history_data[d]
    return result