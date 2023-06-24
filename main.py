from fastapi import FastAPI, HTTPException
import sqlite3
import json
from fastapi.middleware.cors import CORSMiddleware
import datetime
import pandas as pd

app = FastAPI()

origins = [
    "http://localhost:3000",   # React app
    "http://127.0.0.1:3000",   # React app
]

# Allow the origins defined above, allow credentials (for cookies),
# and allow all types of HTTP methods.
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["get"],
    allow_headers=["*"],
)

def get_day_db_connection():
    conn = sqlite3.connect('./db/stock_price(day).db')
    conn.row_factory = sqlite3.Row
    return conn

def get_1min_db_connection():
    conn = sqlite3.connect('./db/stock_price(1min).db')
    conn.row_factory = sqlite3.Row
    return conn

@app.get("/day/{table_name}/{date}")
async def read_table(table_name: str, date: int):
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
    if f"A{table_name}" in tables:
        if len(str(date)) == 8:
            try:
                day_cursor.execute(f"SELECT * FROM A{table_name} WHERE Date >= 20200101 AND Date <= {str(date)};")
                rows = day_cursor.fetchall()
                # Convert rows into dict format for returning as JSON
                for row in rows:
                    result.append(dict(row))
                    day_conn.close()
            except sqlite3.OperationalError:  # if the table doesn't exist
                day_conn.close()
                raise HTTPException(status_code=404, detail="Table not found")

    return result

def refine_row(row):
    dateTime, Open, High, Low, Close, Volume = row
    dateTime = str(dateTime)
    Date = dateTime[:8]
    Time = dateTime[8:]

    return (int(Date), int(Time), int(Open), int(High), int(Low), int(Close), int(Volume))

@app.get("/1min/")
async def read_table(code: str, start_date: int, end_date:int):
    start = datetime.datetime.now()
    df = pd.DataFrame(columns=['Date','Time','Open','High','Low','Close','Amount'])
    result = {}
    # date must be in the YYYYMMDD format
    if len(str(start_date)) != 8 and len(str(end_date) != 8):
        raise HTTPException(status_code=400, detail="Invalid date format")
 
    conn = get_1min_db_connection()
    cursor = conn.cursor()

    # Get all table names in the database
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [row[0] for row in cursor.fetchall()]

    # If the requested table is not in the list of tables, return an error
    if f"A{code}" in tables:
        if len(str(start_date)) == 8 and len(str(end_date)) == 8:
            try:
                # cursor.execute(f"SELECT * FROM A{table_name} WHERE Date >= 20230601 AND Date <= {str(date)};")
                cursor.execute(f"SELECT * FROM A{code}")
                rows = cursor.fetchall()
                rows = list(map(lambda x: refine_row(x), rows))
                rows = list(filter(lambda x: start_date <= x[0] <= end_date, rows))
                # Convert rows into dict format for returning as JSON
                # rows = list(filter(lambda x: start_date <= x[0] <= end_date, rows))
                for r in rows:
                    data = {
                        'Time':r[1],
                        'Open':r[2],
                        'High':r[3],
                        'Low':r[4],
                        'Close':r[5],
                        'Amount':r[6]
                    }
                    try:
                        result[r[0]].append(data) 
                    except KeyError as e:
                        result[r[0]] = []
                        result[r[0]].append(data)

                conn.close()
            except sqlite3.OperationalError:  # if the table doesn't exist
                conn.close()
                raise HTTPException(status_code=404, detail="Table not found")
    end = datetime.datetime.now()
    executed_time = end-start
    print(executed_time)
    return result

def loadJson(path):
    with open(path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    return data

@app.get("/")
async def show_basic_info():
    history_data = loadJson('./db/history.json')
    dateList = list(history_data.keys())
    dateList.sort()
    dateList = dateList[-10:]

    result = {}
    for d in dateList:
        result[d] = history_data[d]
    return result