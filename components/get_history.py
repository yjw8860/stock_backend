from utils.common_functions import *
import re
from datetime import datetime

def getLastHistory():
    history_data = loadJson('./db/history.json')
    dateList = list(history_data.keys())
    dateList.sort()
    dateList = dateList[-10:]

    result = {}
    for d in dateList:
        result[d] = history_data[d]
    return result

def getSpecificHistory(date, duration=5):
    result = {'featuredData':{}}
    if type(date) != int and len(str(date)) != 8:
        raise HTTPException(status_code=400, detail="Invalid date format")

    history_data = loadJson('./db/history.json')
    dateList = list(history_data.keys())
    dateList = list(filter(lambda x: 'KRMarket' in list(history_data[x].keys()), dateList))
    dateList = list(map(lambda x: int(re.sub('-','', x)), dateList))
    dateList.sort()
    end_idx = find_nearest(dateList, date) + 1
    start_idx = max(end_idx-duration, 0)
    targetDate = dateList[end_idx]
    newDateList = dateList[start_idx:end_idx]
    newDateList = list(map(lambda x: f'{str(x)[:4]}-{str(x)[4:6]}-{str(x)[6:8]}', newDateList))
    oneMinStartDate = ''.join(newDateList[-2].split('-'))
    oneMinEndDate = ''.join(newDateList[-1].split('-')) 

    codeList = []
    for d in newDateList:
        result['featuredData'][d] = history_data[d]
        try:
            codeList.append(list(history_data[d]['KRMarket'].keys()))
        except KeyError as e:
            pass
    codeList = list(set(flatten_with_itertools(codeList)))
    result['tradingDate'] = targetDate
    result['codeList'] = codeList
    result['oneMinStartDate'] = oneMinStartDate
    result['oneMinEndDate'] = oneMinEndDate
    return result
