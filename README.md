# Transit GTFS Data Project: Collection & Analysis


## Introduction:
This project is ever growing, and expanding as time goes on.

Originally, I wanted this project to be a test of my data acquisition, and storage skills.
Can I parse data from a website, and store it effectively in some sort of data base? Turns out I can.
Developing a sort-of-live database was fun, and quite cool to see it grow in size every day.

Having all of that data, it was inevitable that I would want to analyze it.
And so this project then turned into a data analytics attempt. Can I discern patterns from the data that I collected?

The patterns I identified were quite simple. A histogram, line graph, and a few maps later I knew I could do accomplish my goal.

Time went on, and I kept coming back to this project. The more I poked at it, the more I wanted to try again, but with all that I've learned.
And so here we are. This is my 4th attempt at this project?


## Observations And Questions:
* [12-29-2023] - Can I interpret traffic patterns from transit GTFS data?


## Procedure:
1. [12-29-2023] - For All Routes In A Given Transit System, Determine Which Stops A Bus Visits
2. [01-15-2024] - Create A System That Can Effectively Parse, And Store GTFS Data Into A Database
3. [01-22-2024] - For A Given Period Of Time, Run System - Gather GTFS Data On A Headless Computer. Build In Contingencies!
4. [00-00-0000] - Having Collected Data, Can I Interpret Traffic Patterns From GTFS Data Collected?


## Overview Of Code:
### 1. Determining All Bus Routes, And The Order Of Bus Stop Arrivals
	Layout Of Logic & Concerns:
	+ Navigate 2: https://www1.brampton.ca/EN/residents/transit/plan-your-trip/Pages/Schedules-andMaps.aspx. Parse all bus routes.
	+ For each bus route, parse each bus stop, grab bus name and additional details
	+ Possible Concern: The Bus Numbers On The Website Do Not Match The Data In Brampton Transits CSV. How Will I Fix This?
		- Wasn't too much of a concern. The bus stop names were pretty accurate as is. I just used those.
	+ Add supplementary data to all bus stops parsed, IE.. the route, number of stop in route, ETC..
	+ Make small formatting fixes
	+ Add bus stop location data from City Of Brampton Open Data
	+ Store all data in a SQLite3 database


### 2. Parse Realtime GTFS Data For Each Bus, And Append To A Live Database
	Layout Of Logic & Concerns:
	+ Instantiate A Class That Stores The Following Logic:
		+ Checks The Validity Of Needed Database Tables
		+ Pulls Needed Bus Routes, And Details Bus Stop Details, Uploads To The Database
		+ Pulls GTFS Data From JSON API

	+ Every 15 Seconds Rerun The Last Step From Above
	+ Ensure All Data Is Valid, An Error Catching Is Present


### 3. Move To RaspBerry PI & Run For A Short Test Period (Can It Run For A Week?)
	Issues Identified:
	+ Started Running Jan 15th 2024, Running Well So Far
	+ As The Number Of Rows Increased, So Did The Time It Took To Perform A DB Update. This Will Cause Issues Days Into Data Collection Attempt.
	+ Need A More Efficient Methodology. Ideas?
	+ Possible: https://vikborges.com/articles/simple-way-to-update-a-sqlite-database-table-from-a-csv-file-using-pandas/
	+ File Size Got Really Big, Really Quick. Look Into Appropriate Datatypes.
	+ Possible: https://sqlite.org/datatype3.html
	+ Solution: Dynamic Caching Of U_IDs Stored In 10 Min Time Period


### 4. Analyzing Test Data. How Can We Visualize The Data That We Have Collected?
	Possible Research Questions:
	+ How Much Data Are We Collecting, Does The Amount Of Data Vary By Time?
	+ If Data Collected Does Vary By Time, IE There Are No Buses Running, Can We Run Another Script In Between?
	+ Maybe A Script That Does Some Data Cleaning, And Maybe Does Some Basic Analysis? Maybe Even Makes A BackUp Of The Database, Just Incase?
