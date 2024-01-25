# Name:                                            Renacin Matadeen
# Date:                                               01/10/2024
# Title                  Main functions used within data collection effort will be found in this file
#
# ----------------------------------------------------------------------------------------------------------------------
import time
import sqlite3
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.dates import DateFormatter
# ----------------------------------------------------------------------------------------------------------------------

# Read Data From Database
db_path = r"/Users/renacin/Desktop/DataStorage.db"
con = sqlite3.connect(db_path)
sql_query = f'''
-- Step #1: Only Pull The Time When A Datapoint Was Collected

SELECT *
FROM BUS_LOC_DB
'''
df = pd.read_sql_query(sql_query, con)
df = df.drop_duplicates(subset=['u_id'])
con.close()


# We Need To Group Data In 15 Min Intervals
df['YEAR'] = pd.to_datetime(df['dt_colc']).dt.year
df['MONTH'] = pd.to_datetime(df['dt_colc']).dt.month
df['DAY'] = pd.to_datetime(df['dt_colc']).dt.day
df['R15_TM'] = pd.to_datetime(df['dt_colc']).dt.floor('15T').dt.time

# Create A New Datetime Timestamp
df["STR_COL"] = df['YEAR'].astype(str) + "-" + df['MONTH'].astype(str) + "-" + df['DAY'].astype(str) + " " + df['R15_TM'].astype(str)
df["DT_COL"] = pd.to_datetime(df["STR_COL"], format='%Y-%m-%d %H:%M:%S')


# Grab The Day, Month, Year For Additional Sorting
def num_unique(x): return len(x.unique())
grped_time = df.groupby(['DT_COL'], as_index=False).agg(
						COUNT_R30 = ("DT_COL", "count"),
						COUNT_RTS = ("route_id", num_unique),
						COUNT_BUS = ("vehicle_id", num_unique)
						)


fig, ax = plt.subplots()
ax.scatter(grped_time["DT_COL"], grped_time["COUNT_BUS"], marker ="x")
ax.set_xlabel("Time (15 Min Interval)")
ax.set_ylabel("# Of Buses")
ax.set_title("Number Of Brampton Transit Buses Over A Given Time")

myFmt = DateFormatter("%d - %H:%S")
ax.xaxis.set_major_formatter(myFmt)

## Rotate date labels automatically
fig.autofmt_xdate()
plt.show()


"""
# Export Data
out_path = r"/Users/renacin/Desktop/Out.csv"
grped_time.to_csv(out_path, index=False)
"""
