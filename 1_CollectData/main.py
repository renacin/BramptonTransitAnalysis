# Name:                                            Renacin Matadeen
# Date:                                               01/27/2024
# Title                                Main Logic Of Data Collector & Output Formatter
#
# ----------------------------------------------------------------------------------------------------------------------
from Functions.collect_data import DataCollector
import datetime
import time
# ----------------------------------------------------------------------------------------------------------------------

# Main Logic Of Python Code
def main():

	# Define Paths Needed
	out_path = r"/Users/renacin/Documents/BramptonTransitAnalysis/3_Data"
	db_path = out_path + "/DataStorage.db"

	# Create An Instance Of The Data Collector
	Collector = DataCollector(db_path, skp_rte_dwn=True, skp_stp_dwn=True)

	# Scheduled Maintenance Will Be The Next Day (+1) At 0300 AM, Export Data From DB To CSV, And Clear The Database
	alrm_dt = str(datetime.datetime.now().strftime('%Y-%m-%d'))
	alrm_tm = "22:32"

	# Keep Data Collector Running
	while True:

		# Get The Current Time
		nw_dt = str(datetime.datetime.now().strftime('%Y-%m-%d'))
		nw_tm = str(datetime.datetime.now().strftime('%H:%M'))

		# Use Error Catching So It Keeps On Going
		try:

			# If It's 0300AM, Export Data To CSV, Clean DB Tables, Generate Graphics, Etc...
			if (nw_tm == alrm_tm) & (nw_dt == alrm_dt):

				# Perform Data Maintenance
				print(f"{nw_tm}: It's Time To Clean Up The DB")

				# Set The New Maintenance Alarm For The Next Day
				alrm_dt = str((datetime.datetime.now() + datetime.timedelta(days=1)).strftime('%Y-%m-%d'))


			# If It's Not Scheduled Maintenance
			else:
				print(f"{nw_tm}: Collecting Data")

				"""
				Collector.get_bus_loc()
				time.sleep(15)
				"""

			time.sleep(10)


		except KeyboardInterrupt:
			now = datetime.datetime.now()
			dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
			print(f" Stoped By User: {dt_string}")
			break


		except Exception as e:
			print(f"Error: {e}")
			break




# ----------------------------------------------------------------------------------------------------------------------
# Entry Point Into Python Code
if __name__ == "__main__":
	main()
