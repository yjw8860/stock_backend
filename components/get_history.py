import json

def loadJson(path):
    with open(path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    return data

def getLastHistory():
    history_data = loadJson('./db/history.json')
    dateList = list(history_data.keys())
    dateList.sort()
    dateList = dateList[-10:]

    result = {}
    for d in dateList:
        result[d] = history_data[d]
    return result

def getSpecificHistory(date, duration):
    if type(duration) != int:
        raise HTTPException(status_code=400, detail="Invalid date format")
    if type(date) != int and len(str(date)) != 8:
        raise HTTPException(status_code=400, detail="Invalid date format")
    duration -= 1
    history_data = loadJson('./db/history.json')
    dateList = list(history_data.keys())

    date = str(date)
    date = f'{date[:4]}-{date[4:6]}-{date[6:8]}'

    dateList.sort()
    result = {}
    if date in dateList:
        idx = dateList.index(date)
        dateList = dateList[idx-duration:idx+1]        
    else:
        dateList = dateList[-duration:]
        
    for d in dateList:
        result[d] = history_data[d]

    
    return result
