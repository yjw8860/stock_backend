import FinanceDataReader as fdr
from pykrx import stock
from tqdm.contrib import tzip
import sqlite3
from datetime import datetime
from pymongo import MongoClient


def get_db_connection():
    conn = sqlite3.connect('./db/stock_price(day).db')
    conn.row_factory = sqlite3.Row
    return conn

def get_db_connection():
    conn = sqlite3.connect('../db/stock_price(day).db')
    conn.row_factory = sqlite3.Row
    return conn

def getDayStockData():
    conn = get_db_connection()
    cursor = conn.cursor()

    client = MongoClient('localhost', 27017)
    endDate = datetime.now().strftime('%Y%m%d')
    marketList = ["KOSPI", 'KOSDAQ']
    restInfoDB = client['restInfo']

    codes = []
    for m in marketList:
        stockList = fdr.StockListing(m)
        codeList = stockList['Code'].tolist()
        nameList = stockList['Name'].tolist()
        for (c, n) in tzip(codeList, nameList):
            codes.append(c)
            query = f'SELECT * FROM A{c}'
            try:
                cursor.execute(query)
                rows = cursor.fetchall()
                startDate = str(rows[0][0])
            except:
                startDate = '20230101'
                pass
            collection = restInfoDB[c]
            newData = {'Name':n, 'table':[]}
            restInfo = stock.get_market_cap(startDate, endDate, c, 'd')
            restInfo = restInfo.drop(['시가총액'], axis=1)
            for info in restInfo.iterrows():
                newData['table'].append(
                    {'Date':info[0].strftime('%Y%m%d'),
                     'Volume':int(info[1][0]),
                     'Amount':int(info[1][1]),
                     'Cap':int(info[1][2])}
                )
            collection.insert_one(newData)

getDayStockData()
# client = MongoClient('localhost', 27017)
# client.drop_database('restInfo')
