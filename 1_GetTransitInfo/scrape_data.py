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

	# Navigate To WebPage
	URL = "https://www1.brampton.ca/EN/residents/transit/plan-your-trip/Pages/Schedules-and-Maps.aspx"
	page = requests.get(URL)

	# Parse All HTML Data, Find All HREF Tags
	rt_lk, rt_dir= [], []
	soup = BeautifulSoup(page.content, "html.parser")
	for tag in soup.find_all('a', href=True):
		str_ref = str(tag)
		if "https://www.triplinx.ca/en/route-schedules/6/RouteSchedules/" in str_ref:
			for var in ['<a href="', '</a>']:
				str_ref = str_ref.replace(var, "")

			# Parse Out Route Link As Well As Direction
			raw_link, dir = str_ref.split('">')
			link = raw_link.split('?')[0]

			# Append Data To Lists
			rt_lk.append(link + "#trips")
			rt_dir.append(dir)

	print(rt_lk)


# ----------------------------------------------------------------------------------------------------------------------
# Main Entry Point Into Python Code
if __name__ == "__main__":
    main()


"""

https://www.triplinx.ca/en/route-schedules/6/RouteSchedules/zum-queen/510/501-zum-queen-eastbound-hwy-7-at-eb-vaughan-metrop/1

"""
