# Name:                                            Renacin Matadeen
# Date:                                               12/29/2023
# Title                                Gather Transit Route Info, Stops In A Route
#
# ---------------------------------------------------------------------------------------------------------------------
import re
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



def get_rt_stops(rt_links, rt_names):
	"""
	Given a list of links, this function navigates through each, pulling information
	regarding each bus stop visited. This function returns a pandas dataframe.
	"""

	stp_data = []
	num_rts = len(rt_names)
	counter = 1
	for link, name in zip(rt_links, rt_names):

		# Navigate To WebPage, Pull All HTML, Convert To String, Use Regex To Pull All Stop Names
		page = requests.get(link)
		soup = BeautifulSoup(page.content, "html.parser")
		hrefs = soup.find_all(class_="link-to-stop")

		# Create A List Of The Bus Stops Found W/ Name For Join To Main Data
		rw_bs = [name + "###" + str(x).split('">')[1].replace("</a>", "") for x in hrefs]
		stp_data.extend(rw_bs)

		print(f"Parsed: {name}, Progress: {counter}/{num_rts}, Num Stops: {len(rw_bs)}")
		counter += 1


	# Return A Pandas Dataframe With Route Data
	stp_df = pd.DataFrame(stp_data, columns=["RAW_DATA"])
	stp_df[["RT_NM", "STP_NM"]] = stp_df["RAW_DATA"].str.split("###", n=1, expand=True)
	stp_df.drop(["RAW_DATA"], axis=1, inplace=True)

	return stp_df



# ---------------------------------------------------------------------------------------------------------------------
def main():

	# Define Needed Variables
	out_path = r"/Users/renacin/Documents/BramptonTransitAnalysis/8_Data"

	# Gather All Needed Data
	rt_df = get_rt_info("https://www1.brampton.ca/EN/residents/transit/plan-your-trip/Pages/Schedules-and-Maps.aspx")
	stp_df = get_rt_stops(rt_df["RT_LINK"].to_list(), rt_df["RT_NM"].to_list())

	# Using All Stops As Main Data, Left Join Route Information, Export As CSV
	stp_data_df = stp_df.merge(rt_df, on='RT_NM', how='left')
	main_out_path = out_path + "/routes_and_bus_stops.csv"
	stp_data_df.to_csv(main_out_path, index=False)




# ---------------------------------------------------------------------------------------------------------------------

# Main Entry Point Into Python Code
if __name__ == "__main__":
    main()
