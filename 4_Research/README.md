# Transit GTFS Data Project: Collection & Analysis

## Introduction - Research:
	+ This section of this project is reserved for testing, and research.

	+ If there was some area of difficulty, the process to identifying a solution
	  will be documented here.

## 1. (Database) Update Issue
	+ One challenging issue I faced in this project, was the process of
	  reaching out to the JSON API, and adding unique rows to a database table.

	+ At first, it may not sound too complicated. But when dealing with a JSON API that
	  has irregular updating schedules, and the data it's pushing out has the
	  possibility of containing non-unique rows from previous API calls, the problem
	  of adding unique data to a database table becomes much harder.

	+ In previous attempts, I read new JSON data as a data-frame, while
	  also reading the old database table as a data-frame. I would then merge both
	  and drop duplicates. At the time I thought it . It was only
	  until recently that I realized that each data pull was getting longer. This
	  was because I was reading in the entirety of the old database table - a variable
	  that would always get bigger, and put an ever increasing strain on memory resources.

	+ To fix this I needed a way to upload unique rows to the old database table without
	  having to read in the entire table.

	+ The solution I came up with was a dynamic cache of "U_ID"s. I know that sounds
	  complicated, but let me explain. The column "U_ID" contains



## 2. (Database) Daily Offloading RPI3 Memory Constraints
	  bus_loc_df["u_id"] = bus_loc_df["route_id"] + "_" + bus_loc_df["vehicle_id"] + "_" + bus_loc_df["timestamp"].astype(str)
