# Name:                                            Renacin Matadeen
# Date:                                              01/05/2024
# Title                                   Brampton Transit But Location Parse
#
# ----------------------------------------------------------------------------------------------------------------------
from Funcs.webcrawler import *
# ----------------------------------------------------------------------------------------------------------------------


def collect_data():
    """ This function will define the main logic of this data collection experiment """

    # Instantiate webcrawler, and connect to databse
    Crawler = WebCrawler()
    SQLite_DB = SQLite_Database(r"DataStorage.db")

    while True:
        # Constantly gather from JSON stream | Every 30 seconds?
        raw_data = Crawler.gather_transit_data()
        cleaned_data = Crawler.clean_transit_data(raw_data)

        # Append Data To Database
        SQLite_DB.addtoDB(cleaned_data, 1)

        # Wait 30 Seconds
        time.sleep(30)


ß
# ----------------------------------------------------------------------------------------------------------------------

# Main Entry Point Into Python Code
if __name__ == "__main__":

	# Collect Data & Append To Database
	collect_data()