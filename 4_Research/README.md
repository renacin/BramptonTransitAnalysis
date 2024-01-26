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
	  complicated, but let me explain. The column "U_ID" is based on, route_id,
	  vehicle_id, and timestamp. This creates a unique identifier for a bus location
	  update.

	+ Once we pull the JSON data, we create the U_ID column. However, before we add
	  those new rows to our database table, we check to see if those U_IDs are in
	  another database table, a cache of U_IDs gathered in the last 10 minutes.
	  If they can't be found in the cache we add them to the old database table.
	  Note, that we add the new rows to the 10 min cache database table as they are
	  now not unique, and we remove rows that are older than 10 min.

	+ One additional speed up that I made use of was the timeout feature of the
	  get request. It doesn't make data transfers faster, but it does make things
	  more consistent over all.

<center><img src="https://github.com/renacin/BramptonTransitAnalysis/blob/master/4_Research/Images/DB_UPDATE_1.png" width="200" /><center/>
<img src="https://github.com/renacin/BramptonTransitAnalysis/blob/master/4_Research/Images/DB_UPDATE_2.png" width="400" />
<img src="https://github.com/renacin/BramptonTransitAnalysis/blob/master/4_Research/Images/DB_UPDATE_3.png" width="200" />




## 2. (Database) Daily Offloading RPI3 Memory Constraints
	+ TODO
