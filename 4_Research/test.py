# Name:                                            Renacin Matadeen
# Date:                                               01/10/2024
# Title                  Main functions used within data collection effort will be found in this file
#
# ----------------------------------------------------------------------------------------------------------------------
import time
import sqlite3
import numpy as np
import pandas as pd
# ----------------------------------------------------------------------------------------------------------------------

# Read In Parsed Data From Data Collection Effort
data_path = r"/Users/renacin/Desktop/Test.csv"
df = pd.read_csv(data_path)

# Remove Duplicate U_IDs Just In Case: We Went From 152'436 TO 151'987: Diff Of 449 Duplicate IDs

# Look At Just dt_colc, Look At Data Points Collected Every Half Hour
df['YEAR'] = pd.to_datetime(df['dt_colc']).dt.year
df['MONTH'] = pd.to_datetime(df['dt_colc']).dt.month
df['DAY'] = pd.to_datetime(df['dt_colc']).dt.day
df['R30_TM'] = pd.to_datetime(df['dt_colc']).dt.floor('30T').dt.time

# Grab The Day, Month, Year For Additional Sorting
def num_unique(x): return len(x.unique())
grped_time = df.groupby(['YEAR', 'MONTH', 'DAY', 'R30_TM'], as_index=False).agg(
						COUNT_R30 = ("R30_TM", "count"),
						COUNT_RTS = ("route_id", num_unique),
						COUNT_BUS = ("vehicle_id", num_unique)
						)

# Export Data
out_path = r"/Users/renacin/Desktop/Out.csv"
grped_time.to_csv(out_path, index=False)
