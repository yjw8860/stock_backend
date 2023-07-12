import pandas as pd
from fastapi import HTTPException
import datetime
import sqlite3
import re

from utils.common_functions import *


def get_1min_db_connection():
    conn = sqlite3.connect('./db/stock_price(1min).db')
    conn.row_factory = sqlite3.Row
    return conn

def refine_row(row):
    dateTime, Open, High, Low, Close, Volume = row
    dateTime = str(dateTime)
    Date = dateTime[:8]
    Time = dateTime[8:]

    return (dateTime, int(Date), int(Time), int(Open), int(High), int(Low), int(Close), int(Volume))

def get1Min(code, end_date, duration=5):
    start = datetime.datetime.now()
    results = {'oneMinChartData':[], 'firstTradingDateTime':0, 'firstOpenPrice':0}
    # date must be in the YYYYMMDD format
    if len(str(end_date))!=8:
        raise HTTPException(status_code=400, detail="Invalid date format")
 
    conn = get_1min_db_connection()
    cursor = conn.cursor()

    # Get all table names in the database
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [row[0] for row in cursor.fetchall()]
    # If the requested table is not in the list of tables, return an error
    if f"A{code}" in tables:
        try:
            # cursor.execute(f"SELECT * FROM A{table_name} WHERE Date >= 20230601 AND Date <= {str(date)};")
            cursor.execute(f"SELECT * FROM A{code}") #모든 data를 가져오는 작업이므로 시간 증가
            rows = cursor.fetchall()
            rows = list(map(lambda x: refine_row(x), rows))
            dates = list(set(map(lambda x: x[1], rows)))
            dates.sort()
            endIdx = find_nearest(dates, int(end_date))+1
            startIdx = max(endIdx-5, 0) #신규 상장된 종목의 경우 5일치 데이터가 없을수도 있음. 
            start_date = dates[startIdx]
            target_date = dates[endIdx]
            print(target_date)
            rows1 = list(filter(lambda x: start_date <= x[1] <= target_date, rows))
            for r in rows1:
                data = {
                    "dateTime":r[0],
                    "date":r[1],
                    "time":r[2],
                    "open":r[3],
                    "high":r[4],
                    "low":r[5],
                    "close":r[6],
                    "amount":r[7]
                }
                results['oneMinChartData'].append(data)
            conn.close()
            newData = list(filter(lambda x: x[1]==target_date, rows))[0]
            firstTradingDateTime, firstOpenPrice = (newData[0], newData[3])
            results["firstOpenPrice"] = firstOpenPrice
            results['firstTradingDateTime'] = firstTradingDateTime
        except sqlite3.OperationalError:  # if the table doesn't exist
            conn.close()
            raise HTTPException(status_code=404, detail="Table not found")
    end = datetime.datetime.now()
    executed_time = end-start
    print(executed_time)
    conn.close()
    return results

def get_1min_multiple_codes(end_date, duration=5):
    start_time = datetime.datetime.now()
    conn = get_1min_db_connection()
    cursor = conn.cursor()

    results = {}

    if len(str(end_date)) != 8:
        raise HTTPException(status_code=400, detail="Invalid date format")
    history_data = loadJson(path='./db/history.json')
    dates = list(history_data.keys())
    dates = list(map(lambda x: int(re.sub('-','',x)), dates))
    end_idx = find_nearest(dates, end_date) + 1
    start_idx = end_idx - duration

    dates = dates[start_idx:end_idx+1]
    dates = list(map(lambda x: str(x), dates))
    dates.sort()
    s_date = dates[0]
    e_date = dates[len(dates)-1]
    codes = []
    for d in dates:
        year = d[:4]
        month = d[4:6]
        date = d[6:8]
        key = f'{year}-{month}-{date}'
        data = history_data[key]
        try:
            kr_data = data['KRMarket']
            codes.append(list(kr_data.keys()))
        except KeyError as e:
            pass
    codes = list(set(flatten_with_itertools(codes)))
    for c in codes:
        results[c] = []
        query = f'SELECT * FROM A{c}' #모든 data를 가져오므로 시간 증가
        cursor.execute(query)
        rows = cursor.fetchall()
        rows = list(map(lambda x: refine_row(x), rows))
        rows = list(filter(lambda x: int(s_date) <= x[1] <= int(e_date), rows))
        for r in rows:
            data = {
                "dateTime":r[0],
                "date":r[1],
                "time":r[2],
                "open":r[3],
                "high":r[4],
                "low":r[5],
                "close":r[6],
                "amount":r[7]
            }
            results[c].append(data)
    conn.close()
    end_time = datetime.datetime.now()
    print(f'소요 시간: {end_time - start_time}')
    return results

def get_close_data(date):
    """
    date 형식: "YYYY-mm-dd" ex)2023-06-08
    """
    conn = get_1min_db_connection()
    cursor = conn.cursor()

    duration = 5
    result = {}

    codes = []
    if type(date) != int and len(str(date)) != 8:
        raise HTTPException(status_code=400, detail="Invalid date format")

    history_data = loadJson('./db/history.json')
    dateList = list(history_data.keys())
    dateList = list(map(lambda x: int(re.sub('-','', x)), dateList))
    dateList.sort()
    end_idx = find_nearest(dateList, date) + 1
    start_idx = end_idx - duration

    newDateList = dateList[start_idx:end_idx]
    newDateList = list(map(lambda x: f'{str(x)[:4]}-{str(x)[4:6]}-{str(x)[6:8]}', newDateList))
    for d in newDateList:
        try:
            codes.append(history_data[d]['KRMarket'].keys())
        except KeyError as e:
            pass
    codes = flatten_with_itertools(codes)
    codes = list(set(codes))

    nowDate = newDateList[len(newDateList)-1]
    
    dateList = list(history_data.keys())
    dateList = list(filter(lambda x: 'KRMarket' in list(history_data[x].keys()), dateList))
    dateList.sort()
    idx = dateList.index(nowDate) + 1
    targetDate = dateList[idx]
    targetDate = int(re.sub('-','',targetDate))

    for c in codes:
        query = f"SELECT * FROM A{c}"
        cursor.execute(query)
        rows = cursor.fetchall()
        rows = list(map(lambda x: refine_row(x), rows))
        rows = list(filter(lambda x: x[1] == targetDate, rows))
        for r in rows:
            dateTime = r[0]
            closePrice = r[6]
            try:
                result[dateTime][c] = closePrice
            except KeyError as e:
                result[dateTime] = {}
                result[dateTime][c] = closePrice
                pass

    return result

