# Name:                                            Renacin Matadeen
# Date:                                               12/29/2023
# Title                                Gather Transit Route Info, Stops In A Route
#
# ---------------------------------------------------------------------------------------------------------------------
import requests
import pandas as pd
from bs4 import BeautifulSoup
# ---------------------------------------------------------------------------------------------------------------------


def pull_routes(transit_url):
	"""
	This function navigates to Brampton Transit's Routes & Map Page.
	It parses all hrefs related to routes - including variations of a given route.
	It returns a pandas dataframe with the route name, direction, and link.
	"""

	# Navigate To WebPage
	page = requests.get(transit_url)

	# Parse All HTML Data, Find All HREF Tags
	rt_data = []
	soup = BeautifulSoup(page.content, "html.parser")
	for tag in soup.find_all('a', href=True):
		str_ref = str(tag)
		if "https://www.triplinx.ca/en/route-schedules/6/RouteSchedules/" in str_ref:
			for var in ['<a href="', '</a>']:
				str_ref = str_ref.replace(var, "")

			# Parse Out Route Name, Direction, Group, Link, Etc...
			raw_link, dir = str_ref.split('">')
			link = raw_link.split('?')[0]
			full_data = [link + "#trips", dir] + link.split("/")[7:10]
			rt_data.append(full_data)

	# Return A Pandas Dataframe With Route Data
	return pd.DataFrame(rt_data, columns=["RT_LINK", "RT_DIR", "RT_GRP", "RT_NUM", "RT_NM"])


# ---------------------------------------------------------------------------------------------------------------------
def main():
	rt_df = pull_routes("https://www1.brampton.ca/EN/residents/transit/plan-your-trip/Pages/Schedules-and-Maps.aspx")



# ---------------------------------------------------------------------------------------------------------------------

# Main Entry Point Into Python Code
if __name__ == "__main__":
    main()
