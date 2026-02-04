import sqlite3
import pandas as pd

CSV_PATH = "data/nrel_alt_fuel_stations_raw.csv"
DB_PATH = "tests/fixtures/nrel_sample.sqlite"
TABLE_NAME = "fuel_stations"

df = pd.read_csv(CSV_PATH)

conn = sqlite3.connect(DB_PATH)
df.columns = [c.replace(".", "__") for c in df.columns]
df.to_sql(TABLE_NAME, conn, if_exists="replace", index=False)
conn.close()

print("SQLite DB created:", DB_PATH)
