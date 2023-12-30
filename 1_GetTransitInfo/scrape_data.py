# Name:                                            Renacin Matadeen
# Date:                                               12/29/2023
# Title                                Gather Transit Route Info, Stops In A Route
#
# ----------------------------------------------------------------------------------------------------------------------
import requests
from bs4 import BeautifulSoup
# ----------------------------------------------------------------------------------------------------------------------
def main():
	""" This function will define the main logic of the data exploration section of this project """
	URL="https://www1.brampton.ca/EN/residents/transit/plan-your-trip/Pages/Schedules-and-Maps.aspx"
	page=requests.get(URL)
	soup=BeautifulSoup(page.content, "html.parser")
	print(soup)

"""
Schedules/.*?/.*?/.*?/[0-9]{1,2}
"""
# ----------------------------------------------------------------------------------------------------------------------
# Main Entry Point Into Python Code
if __name__ == "__main__":
    main()
