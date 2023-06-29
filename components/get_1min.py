import pandas as pd
from fastapi import HTTPException
import datetime
import sqlite3
from bisect import bisect_left
import re
import json
import itertools

def flatten_with_itertools(a):
    """
    a: 2차원 리스트
    return: 1차원 리스트
    """
    return list(itertools.chain(*a))

def find_nearest(a, b):
    """
    a: integer가 여러개 있는 list
    b: integer
    return: a의 원소 중 b와 가장 가까운 원소의 index 반환
            만약, b와 가장 가까운 원소가 2개라면, b보다 큰 원소의 index를 반환
    """
    idx = bisect_left(a, b)

    # b가 a의 모든 원소보다 작은 경우
    if idx == 0:
        return idx
    # b가 a의 모든 원소보다 큰 경우
    elif idx == len(a):
        return idx - 1
    # b와 가장 가까운 a의 원소가 2개인 경우 (b보다 큰 원소의 index를 반환)
    elif a[idx] - b == b - a[idx - 1]:
        return idx
    # b와 가장 가까운 a의 원소가 1개인 경우
    elif a[idx] - b < b - a[idx - 1]:
        return idx
    else:
        return idx - 1

def loadJson(path):
    with open(path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    return data

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

def get1Min(code, end_date):
    start = datetime.datetime.now()
    result = []
    # date must be in the YYYYMMDD format
    if len(str(end_date) != 8):
        raise HTTPException(status_code=400, detail="Invalid date format")
 
    conn = get_1min_db_connection()
    cursor = conn.cursor()

    # Get all table names in the database
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [row[0] for row in cursor.fetchall()]

    # If the requested table is not in the list of tables, return an error
    if f"A{code}" in tables:
        if len(str(end_date)) == 8:
            try:
                # cursor.execute(f"SELECT * FROM A{table_name} WHERE Date >= 20230601 AND Date <= {str(date)};")
                cursor.execute(f"SELECT * FROM A{code}")
                rows = cursor.fetchall()
                rows = list(map(lambda x: refine_row(x), rows))
                rows = list(filter(lambda x: start_date <= x[1] <= end_date, rows))
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
                    result.append(data)
                conn.close()
            except sqlite3.OperationalError:  # if the table doesn't exist
                conn.close()
                raise HTTPException(status_code=404, detail="Table not found")
    end = datetime.datetime.now()
    executed_time = end-start
    print(executed_time)
    return result

def get_1min_multiple_codes(end_date, duration=5):
    conn = get_1min_db_connection()
    cursor = conn.cursor()

    results = {}

    if len(str(end_date)) != 8:
        raise HTTPException(status_code=400, detail="Invalid date format")
    history_data = loadJson(path='./db/history.json')
    dates = list(history_data.keys())
    dates = list(map(lambda x: int(re.sub('-','',x)), dates))
    end_idx = find_nearest(dates, end_date)
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
        query = f'SELECT * FROM A{c}'
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
    return results

    