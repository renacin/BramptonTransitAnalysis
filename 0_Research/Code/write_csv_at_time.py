# Name:                                            Renacin Matadeen
# Date:                                               01/10/2024
# Title                  Main functions used within data collection effort will be found in this file
#
# ----------------------------------------------------------------------------------------------------------------------
import time
import datetime
import pandas as pd
import numpy as np
# ----------------------------------------------------------------------------------------------------------------------

"""
# Note That 3:00AM Would Be Stored As 03:00
# Depending On When I Start This, The First Alarm Will Be The Next Day (+1) At 0300 AM
alrm_dt = str(datetime.datetime.now().strftime('%Y-%m-%d'))
alrm_tm = "09:21"

while True:

	# Current Time
	nw_dt = str(datetime.datetime.now().strftime('%Y-%m-%d'))
	nw_tm = str(datetime.datetime.now().strftime('%H:%M'))

	# If It's 0300AM, Export Data To CSV, Clean DB Tables, Generate Graphics, Etc...
	if (nw_tm == alrm_tm) & (nw_dt == alrm_dt):
		print(f"{nw_tm}: It's Time To Clean Up The DB")

		# Set The New Alarm Date To Tomorrow
		alrm_dt = str((datetime.datetime.now() + datetime.timedelta(days=1)).strftime('%Y-%m-%d'))

	# If It's Not 0300AM Just Collect Data
	else:
		print(f"{nw_tm}: Collecting Data")

	# Wait Before Going Through Loop Again
	time.sleep(10)
"""



out_path = r"/Users/renacin/Desktop"

input_val = True
DB_NAME = "TESTING"
nw_dt = str(datetime.datetime.now().strftime('%Y-%m-%d'))

df = pd.DataFrame(np.random.randn(100, 6))

# Remove Any Duplicate Values Based On A Column Provided
df = df.drop_duplicates().reset_index()


# We Need To Keep An A Version Of The Input Dataframe With No Rows & Just Columns
em_df = df.iloc[:0].copy()

# Are We Exporting All Columns? If Value = True Then All, If List Then All Cols In List
# if input_val
if type(input_val) == list:
	df = df[input_val]

elif input_val == True:
	pass

else:
	print("Invalid Arguement")
	raise ValueError("Invalid Choice: Choose Either List Of Columns, Or True")

# Export Data
db_path = out_path + f"/{DB_NAME}_{nw_dt}.csv"
df.to_csv(db_path, index=False)

# Write Over DB Table So It's Now Empty
df = em_df
