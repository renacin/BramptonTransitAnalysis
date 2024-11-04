# Transit GTFS Data Project: Collection & Analysis


## Introduction:
Originally, I wanted this project to be a test of my data acquisition, and storage skills.
Can I parse data from a website, and store it effectively in some sort of data base? Turns out I can.
Developing a live-ish database was fun, and quite cool. Seeing the database grow in size every day was neat.

Having all of that data, it was inevitable that I would try to analyze it.
And so this project then turned into a data analytics attempt. Can I discern patterns from the data that I collected?

The patterns I identified were quite simple. A histogram, line graph, and a few maps later I knew I could do accomplish my goal.

Time went on, and I kept coming back to this project. The more I poked at it, the more I wanted to try again; each new attempt with
a different set of skills, always learning something new.

And so here we are, I think this is my 5th attempt at this project - and it has now morphed into a pet project where I am
trying to create a system that not only continuously gather's data in the most efficient way possible - but also provides analytics
of the day before's transit network.


## Purpose Of This Project:
* [12-29-2023] - Can I create an efficient data collector, and analytics pipeline to interpret day-before metrics from Brampton Transit GTFS data?


## Goals Of This Project:
* [11-01-2024] - Create An Efficient Data Collection Pipeline - Have It Running On A Raspberry Pi
* [12-31-2024] - Create An Equally Efficient Data Analytics System That Determine KPIs From The Day Before's Data


## Overview Of Data Collector Pipeline (REPLACE THIS SECTION WITH CONTROL FLOW DIAGRAM!):
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




## Possible Research Questions:
	+ How Much Data Are We Collecting, Does The Amount Of Data Vary By Time?
	+ If Data Collected Does Vary By Time, IE There Are No Buses Running, Can We Run Another Script In Between?
	+ Maybe A Script That Does Some Data Cleaning, And Maybe Does Some Basic Analysis? Maybe Even Makes A BackUp Of The Database, Just Incase?
	+ What type of analytics do I want to do?
	+ What type of data am I collecting? Does it make sense? Are there any errors preventing me from letting this run for a long period of time?
	+ Identified an issue where the RPI3 Kernel will kill the script. I looks like a memory issue. I need to profile everything and optimize!
	+ Can we identify long term patterns in our data?
	+ Do system outages happen, if so can we exclude impacted trips from our analysis?
	+ Can We Upload To Dropbox?
