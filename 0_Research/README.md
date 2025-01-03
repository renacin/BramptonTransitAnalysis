# Transit GTFS Data Project: Collection & Analysis

## Introduction - Research:
	+ This section of this project is reserved for testing, and research.

	+ If there was some area of difficulty, the process to identifying a solution
	  will be documented here.


## 1.0 (Database) Update Issue
	+ One challenging issue I faced in this project, was the process of
	  reaching out to the JSON API, and adding unique rows to a database table.

	+ It may not sound complicated, nut when dealing with a JSON API that
	  has irregular updating schedules, and the data it's pushing out has the
	  possibility of containing non-unique rows from previous API calls, the problem
	  of adding unique data to a database table becomes much harder.

	+ In previous attempts, I read new JSON data as a data-frame, while
	  also reading the old database table as a data-frame. I would then merge both
	  and drop duplicates. At the time I thought it the most efficient way of doing things.
	  It was only until recently that by doing this each subsequent pull would take more
	  and more resources to complete. Think about it, I read in new data as a data-frame, as
	  as the entire database as a data-frame, every time. I was reading in the entirety of the old database table
	  a variable that would always get bigger, and put an ever increasing strain on memory resources.

	+ To fix this I needed a way to upload unique rows to the old database table without
	  having to read in the entire table.

	+ The solution I came up with was a dynamic cache of "U_ID"s. I know that sounds
	  complicated, but let me explain. The column "U_ID" is based on, ROUTE_ID,
	  VEHICLE_ID, and TIMESTAMP. This creates a unique identifier for a bus location
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

	+ Another optimization that I made was to offload the database everyday at 3:00AM.
	  This may sound a bit confusing, how does offloading a database improve memory efficiency?
	  Well instead of writing to a large database that gets bigger every second, why not
	  upload to a database that is only as large as the day's total updates.

	+ One improvement that I thought was pretty useful was the implementation of the VACCUM feature in SQLite3.
	  There were certain situations where the database would offload all of it's data for the day, but still stay at
	  around 10MB - even though it was empty. After looking into the issue further this is because when data is written,
	  then deleted, the storage system can become fragmented. And as a result, the overall size of the database will most likely
	  still be large. The VACCUM feature helps solve this by defragmenting the storage system, and database.

	+ Below you can see the overall time it took to make a database update. In Methodology 1, I simply read both
	  the API call, and the entire database as a pandas data-frame, merged the two, and wrote to the database.
	  Methodology 2, depicts the implementation of a daily offload feature. And Methodology 3, depicts the usage of the
	  VACCUM feature in SQLite3.

DB Update: Methodology 1
<p align="center">
<img src="https://github.com/renacin/BramptonTransitAnalysis/blob/master/0_Research/Images/DB_UPDATE_1.png" width="500"/>
</p>

DB Update: Methodology 2
<p align="center">
<img src="https://github.com/renacin/BramptonTransitAnalysis/blob/master/0_Research/Images/DB_UPDATE_2.png" width="500"/>
</p>

DB Update: Methodology 3
<p align="center">
<img src="https://github.com/renacin/BramptonTransitAnalysis/blob/master/0_Research/Images/DB_UPDATE_3.png" width="500"/>
</p>


## 1.1 (System) Memory Leak Issue
	+ Another issue I faced during the development of this project was a persistent memory leak.

	+ At first, When I would run the program on my Raspberry Pi 3 the program would run fairly well.
	  Memory usage was within reason, but never out of the ordinary. However, over time the memory usage would slowly
	  trend upwards. After about 2 weeks of collecting data it would eventually cause a SIG KILL, and would crash.

	+ Looking into the issue, it may be due to the fact that Python doesn't release memory as one would expect.

	+ Here are some resources I found that were helpful:
		- https://stackoverflow.com/questions/15455048/releasing-memory-in-python

	+ A solution that I found helpful was to implement the use of child processes (threads/multiprocessing). When
	  a child process is killed all related memory is released

	+ After much testing it was determined this was the best way to ensure that there are no memory leaks in the code, not the cleanest - but it works


## 1.2 (System) Overall Program Size
	+ Since I want this program to run continuously on a Raspberry Pi 3, it needs to be as efficient as possible.
	+ To reduce the memory overhead at any given moment I made sure to only import certain libraries when needed, and
	  when not needed deleted. Will this cause unintended side-effects? Maybe, only time will tell.

	+ It looks like any weird behavior with the child process was because I wasn't making use of the wait() function with the ProcessPoolExecuter.
	  Everything seems to be working fine.


## 1.3 (Logic) Estimate Arrival At Missing Points
	+ Before we estimate the possible arrival times at bus stops between two bus stops that are not one-after the other,
	  we must first start small and determine the average speed a bus might have been travelling during a given route. All data points are considered,
	  if there are not enough data points we do not delineate between days of the week, and use a general average.

	+ Below you can see general statistics for the 501-349 Bus Route. We can observe that all three main metrics (average speed, standard deviation, and variance)
	  drop as we collect more data. It can be observed that around 100'000 is when we start to reach an equilibrium point. I did try to remove zeros from the data,
	  but I didn't produce the results I expected; a lot of variance in the observations for not much confidence in the new information derived.
	  When a bus makes a stop that's considered in it's overall speed; every data point should be considered.

Bus Speed Observations: Including Zero Speed Observations
<p align="center">
<img src="https://github.com/renacin/BramptonTransitAnalysis/blob/master/0_Research/Images/501-349_Statistics_Overtime.PNG" width="500"/>
</p>


	+ How do we determine arrival times between observations. I'm going to start out small and reuse the original bus location interpretation methodology.
	+ Hopefully, by using a more accurate metric of bus speed, we can determine more accurate arrival times for each bus.
	+ We need to collect a lot of data before we can begin to validate the process that we recreated. Do the output data make sense?
	+ Will all of this make sense? Am I doing this right? Ughhh, the doubt is setting in. When will this project end.



## 2.0 (Logic) Logging Feature
	+ Currently the code is setup to print out information if a Class variable is set to TRUE. However I want this to change.
	+ I want it so if that Class variable is set to true it will write information to a text file for that day. This would be
	  an easier way of logging data, and will not clog up the console window.
