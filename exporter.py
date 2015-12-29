import ConfigParser
import dataset

config = ConfigParser.RawConfigParser()
config.read('config.ini')

db = dataset.connect(config.get('database', 'string'))

stop_db = db['stop']
stop_times_db = db['stop_times']
stop_times_db.create_index(["stop_id"])
db.query('create index a on stop_times (stop_id(50))') #no idea why I need to do this the above should do it.
db.commit()

print "starting stop query"
stop_query = db.query('select stops.stop_id,stop_name,stop_lat,stop_lon from stops inner join (select distinct stop_id from stop_times ) as t on t.stop_id = stops.stop_id')
print "stopping stop query"
stop_times_query = db.query('SELECT trip_id, arrival_time, departure_time, stop_id, stop_sequence FROM stop_times')

#This is to remove duplicate stops with the same lat/lng

stops_latlng = {}
stops_sub = {}

print "Starting stop lint"
for row in stop_query: 
	latlngindex = row["stop_id"].split("M")[-1] + ":" +  str(row["stop_lat"]) +":" + str(row["stop_lon"])
	if latlngindex in stops_latlng:
		db.query("UPDATE stop_times set stop_id='" + stops_latlng[latlngindex] + "' where stop_id='" +row["stop_id"]+ "'")
		db.commit()
		stop_db.delete(stop_id=row["stop_id"])
		stops_sub[row["stop_id"]] = stops_latlng[latlngindex]
	else:
		stops_latlng[latlngindex] = row["stop_id"]
		stops_sub[row["stop_id"]] = stops_latlng[latlngindex]
print "Stopped lint"

stop_times_query = db.query('SELECT trip_id, arrival_time, departure_time, stop_id, stop_sequence FROM stop_times')

stop_query = db.query('select stops.stop_id,stop_name,stop_lat,stop_lon from stops inner join (select distinct stop_id from stop_times ) as t on t.stop_id = stops.stop_id;')

routes_query = db.query('SELECT route_id, "0" as agency_id ,"" as route_short_name,route_long_name,route_type FROM routes')

trips_query = db.query('SELECT route_id,service_id,trip_id FROM trips')



calendar_dates_query = db.query('SELECT service_id, date, exception_type FROM calendar_dates')


dataset.freeze(stop_query,format='csv',filename='stops.txt')
dataset.freeze(routes_query,format='csv',filename='routes.txt')
dataset.freeze(trips_query,format='csv',filename='trips.txt')
dataset.freeze(stop_times_query,format='csv',filename='stop_times.txt')
dataset.freeze(calendar_dates_query,format='csv',filename='calendar_dates.txt')

