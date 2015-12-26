ptv-timetable-gtfs
==================

Converts the PTV Timetable JSON API to Google's GTFS format for map navigation potential

This is currently a work in progress

Requirements for use
--

 - A PTV TTAPI api key from [here](https://ptv.vic.gov.au/about-ptv/ptv-data-and-reports/digital-products/ptv-timetable-api/)
 - A sqlalchemy compatiable database server (ideally you want fast writes to use this script - aka with mysql use innodb_flush_log_at_trx_commit = 2)
 - ```pip install requests dataset pymysql``` (change pymysql to your db plugin)
 - pypy is recommended to perform this quicker
 - Run this from AWS ap-southeast2 to cut down latency

Running
--

Modify config.ini with your details.

I've been testing this by running inside of screen

```pypy updater.py 1> op.log 2> error.log```

For some reason the threads aren't dying at the end when the queue is empty so for the time being I'm just killing the script when I see in the log all the threads have ended. 


After it's down you can use ```expoter.py``` to dump the database into txt files. You'll still need to have an agency.txt


Todo
--

Scripts to automate the entire process
Build default files for agency and feed_info


Linting of the data. There are many bugs with the data provided:

 - Time travelling services
 - Services where the time doesn't change for each stop
 - Unused stops
 - Routes / Trips with no stopping patten
 - VLine missing because there is no way to work out if it's a coach or a train


General bugs - There is a bug related to times becoming negatives

Features
 - This exporter currently does the minimum to meet the specs. Ideally we should pull in more data. Things of intrest are fare costs, zones, colors for train routes, accessbility, short names (trams and bus numbers), headsigns (eg stopping before the end of the line), bikes , pickup type, timepoint 
 - Shapes, this is a hard one since we would need to grab the GIS data from data.vic.gov.au and crop it somehow