from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd

from components.get_day import getOneCodeMulitpleDays, getMultipleCodesOneDay, getMultipleCodesMultipleDays
from components.get_1min import get1Min, get_1min_multiple_codes
from components.get_history import getLastHistory, getSpecificHistory

app = FastAPI()

origins = [
    "http://localhost:3000",   # React app
    "http://192.168.219.108:3000",   # React app
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

@app.get("/1min/")
async def read_1min_data(code: str, start_date: int, end_date:int):
    return get1Min(code, start_date, end_date)

@app.get("/day/")
async def read_one_code_multiple_days(code: str, date: int):
    return getOneCodeMulitpleDays(code, date)

@app.get("/oneDay/")
async def read_multiple_codes_one_day(codes: str, date: int):
    return getMultipleCodesOneDay(codes, date)


@app.get("/history/")
async def read_current_history():
    return getLastHistory()

@app.get("/specificHistory/")
async def read_specific_history(date: int):
    return getSpecificHistory(date)

@app.get("/1min_multiple_codes/")
async def read_1min_multiple_codes_data(end_date:int):
    return get_1min_multiple_codes(end_date)

@app.get("/day_multiple_codes/")
async def read_day_muliple_codes_data(end_date:int, duration:int, start_date:int):
    return getMultipleCodesMultipleDays(end_date, duration, start_date)
