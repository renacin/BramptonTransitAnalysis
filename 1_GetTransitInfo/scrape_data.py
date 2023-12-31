# Name:                                            Renacin Matadeen
# Date:                                               12/29/2023
# Title                                Gather Transit Route Info, Stops In A Route
#
# ---------------------------------------------------------------------------------------------------------------------
import requests
import pandas as pd
from bs4 import BeautifulSoup
# ---------------------------------------------------------------------------------------------------------------------


def get_rt_info(transit_url):
	"""
	Given a URL, thi function navigates to Brampton Transit's Routes & Map Page,
	parses all hrefs related to routes, and returns a pandas dataframe with the
	scraped data.
	"""

	# Navigate To WebPage & Grab HTML Data
	page = requests.get(transit_url)
	soup = BeautifulSoup(page.content, "html.parser")

	# Parse All HTML Data, Find All HREF Tags
	rt_data = []
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



def get_rt_stops(rt_list):
	"""
	Given a list of links, this function navigates through each, pulling information
	regarding each bus stop visited. This function returns a pandas dataframe.
	"""

	for rt in rt_list:
		# Navigate To WebPage
		page = requests.get(rt)
		soup = BeautifulSoup(page.content, "html.parser")

		# Parse All HTML Data, Find All HREF Tags
		for tag in soup.find_all('a', href=True):
			str_ref = str(tag)
			if "StopSchedules/" in str_ref:
				print(str_ref)

		break


"""

<a class="link-to-stop"
href="/en/stop-schedules/28/StopSchedules/smart-vmc-terminal-route-501-zum-queen-wb/53762/zum-queen/510/501-zum-queen-eastbound-501c-zum-queen-east-407/1?Date=12%2F31%2F2023&amp;
PhysicalId=807115">Smart VMC Terminal - Route 501 Zum Queen WB</a>

"""




# ---------------------------------------------------------------------------------------------------------------------
def main():
	rt_df = get_rt_info("https://www1.brampton.ca/EN/residents/transit/plan-your-trip/Pages/Schedules-and-Maps.aspx")
	get_rt_stops(rt_df["RT_LINK"].to_list())


# ---------------------------------------------------------------------------------------------------------------------

# Main Entry Point Into Python Code
if __name__ == "__main__":
    main()
