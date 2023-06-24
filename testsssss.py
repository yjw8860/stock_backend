import json

def loadJson(path):
    with open(path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    return data

history_data = loadJson('./db/history.json')
dateList = list(history_data.keys())
dateList.sort()
dateList = dateList[-10:]





