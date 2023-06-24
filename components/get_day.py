from fastapi import HTTPException
import sqlite3

def get_day_db_connection():
    conn = sqlite3.connect('./db/stock_price(day).db')
    conn.row_factory = sqlite3.Row
    return conn

def getDay(code, date):
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