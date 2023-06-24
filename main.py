from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd

from components.get_day import getDay
from components.get_1min import get1Min
from components.get_history import getHistory

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

@app.get("/1min/")
async def read_1min_data(code: str, start_date: int, end_date:int):
    return get1Min(code, start_date, end_date)

@app.get("/day/")
async def read_day_data(code: str, date: int):
    return getDay(code, date)

@app.get("/")
async def show_basic_info():
    return getHistory()