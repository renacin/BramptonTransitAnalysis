# Name:                                            Renacin Matadeen
# Date:                                               01/10/2024
# Title                  Main functions used within data collection effort will be found in this file
#
# ----------------------------------------------------------------------------------------------------------------------
import time
import datetime
# ----------------------------------------------------------------------------------------------------------------------

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
