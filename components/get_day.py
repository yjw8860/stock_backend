from fastapi import HTTPException
import sqlite3
import itertools
from bisect import bisect_left
import json
import re

def loadJson(path):
    with open(path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    return data

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

def get_day_db_connection():
    conn = sqlite3.connect('./db/stock_price(day).db')
    conn.row_factory = sqlite3.Row
    return conn

def getOneCodeMulitpleDays(code, date):
    result = []
    # date must be in the YYYYMMDD format
    if len(str(date)) != 8:
        raise HTTPException(status_code=400, detail="Invalid date format")
 
    day_conn = get_day_db_connection()
    day_cursor = day_conn.cursor()

    # Get all table names in the database
    day_cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [row[0] for row in day_cursor.fetchall()]

    # If the requested table is not in the list of tables, return an error
    if f"A{code}" in tables:
        if len(str(date)) == 8:
            try:
                day_cursor.execute(f"SELECT * FROM A{code} WHERE Date >= 20200101 AND Date <= {str(date)};")
                rows = day_cursor.fetchall()
                # Convert rows into dict format for returning as JSON
                for row in rows:
                    result.append(dict(row))
                    day_conn.close()
            except sqlite3.OperationalError:  # if the table doesn't exist
                day_conn.close()
                raise HTTPException(status_code=404, detail="Table not found")

    return result

def getMultipleCodesOneDay(codes, date):
    """
    특정 날짜에 해당하는 여러 종목의 Open, High, Low, Close, Amoumt를 조회하는 함수
    code: str이며, 종목코드 리스트에 해당하며, 구분자는 '_'임(ex '005930_035720_035420')
    date: str이며, 날짜에 해당함(YYYYmmdd, ex '20230601')
    """
    if len(str(date)) != 8:
        raise HTTPException(status_code=400, detail="Invalid date format")
    day_conn = get_day_db_connection()
    day_cursor = day_conn.cursor()
    
    result = {}
    codes = codes.split('_')
    print(codes)
    for code in codes:
        day_cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = [row[0] for row in day_cursor.fetchall()]

        # If the requested table is not in the list of tables, return an error
        if f"A{code}" in tables:
            if len(str(date)) == 8:
                try:
                    day_cursor.execute(f"SELECT * FROM A{code} WHERE Date == {str(date)};")
                    rows = day_cursor.fetchall()
                    # Convert rows into dict format for returning as JSON
                    result[code] = dict(rows[0])
                except sqlite3.OperationalError:  # if the table doesn't exist
                    day_conn.close()
                    raise HTTPException(status_code=404, detail="Table not found")
        else:
            pass

    return result

def getMultipleCodesMultipleDays(end_date, duration=5, start_date=20230101):
    results = {}

    day_conn = get_day_db_connection()
    day_cursor = day_conn.cursor()


    history = loadJson('./db/history.json') #이 부분에서 시간 증가
    strKeys = list(history.keys())
    intKeys = list(map(lambda x: int(re.sub('-','', x)), strKeys))
    e_idx = find_nearest(intKeys, end_date) + 1
    s_idx = e_idx - duration
    if(s_idx < 0):
        s_idx = 0
    dateList = strKeys[s_idx:e_idx]
    codes = []

    for d in dateList:
        data = history[d]
        try:
            kr_data = data['KRMarket']
            codes.append(list(kr_data.keys()))
            
        except KeyError as e:
            pass
    codes = list(set(flatten_with_itertools(codes)))
    for c in codes:
        query = f'SELECT * FROM A{c} WHERE date >= {start_date} AND date <= {end_date}'
        day_cursor.execute(query)
        rows = day_cursor.fetchall()
        # rows = list(filter(lambda x: start_date <= x[0] <= date)) 
        results[c] = []
        for r in rows:
            data = {
                "time":r[0],
                "open":r[1],
                "high":r[2],
                "low":r[3],
                "close":r[4],
                "amount":r[5],
            }
            results[c].append(data)

    return results

getMultipleCodesMultipleDays(20230615)
    