# Name:                                            Renacin Matadeen
# Date:                                               04/25/2021
# Title                                   Brampton Transit But Location Parse
#
# ----------------------------------------------------------------------------------------------------------------------
from Funcs.webcrawler import *
from Funcs.analysis import *
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
        weather_data = Crawler.gather_weather_data()

        # Append Data To Database
        SQLite_DB.addtoDB(weather_data, 2)
        SQLite_DB.addtoDB(cleaned_data, 1)

        # Wait 30 Seconds
        time.sleep(30)


def analyze_data():
    """ This function will define the main logic of the data exploration section of this project """

    # Instantiate data collection class, and pass the location of the dataset
    Col_Data = DataCollection(r"Data\Transit_Data.csv")
    Col_Data.process_transit_data()


# ----------------------------------------------------------------------------------------------------------------------

# Main Entry Point Into Python Code
if __name__ == "__main__":

    # collect_data()
    analyze_data()
