import pandas as pd
import numpy as np
import json
import sys
import requests
import datetime

def get_response(start_lat, start_lng, end_lat, end_lng, departure_time, index, total):
	url = 'https://maps.googleapis.com/maps/api/distancematrix/json?'
	api_key = 'AIzaSyBxr-Lw3k3Vutwslnhd8cdGgLmY7KjTuU4'
	data = {'origins' : '{0},{1}'.format(start_lat,start_lng), 'departure_time':str(departure_time),
	'destinations':'{0},{1}'.format(end_lat,end_lng),'key':api_key}
	try:
		response = requests.get(url, params = data)
		status_code = response.status_code
		data = json.loads(response.text)
		print(status_code)
		data = data['rows'][0]['elements'][0]
		duration = data['duration']['value']
		distance = data['distance']['value']
		duration_in_traffic = data['duration_in_traffic']['value']
		values = [status_code, duration, distance, duration_in_traffic]
	except:
		print("Error")
		values = [None, None, None, None]
	print("{0} out of {1} | {2:.2f}% Complete".format(index, total, index*1./total * 100))
	return values

def get_data(dataset_filepath, routes_filepath):
	try:
		df = pd.read_csv(routes_filepath)
	except IOError:
		df = pd.read_csv(dataset_filepath)
		df['routes_id'] = df.index
		df = df[['routes_id','start station latitude','start station longitude',
		'end station latitude', 'end station longitude', 'weekday', 'hour']]
		df['routes_status_code'] = df['routes_id'].apply(lambda x: None)
	ids_pulled = [int(i[0]) for i in df[['routes_id', 'routes_status_code']].values if pd.notnull(i[1])]
	ids_to_pull = [int(i[0]) for i in df[['routes_id', 'routes_status_code']].values if pd.isnull(i[1])]
	total = len(ids_to_pull)
	counter = 1
	epoch = datetime.datetime(1970,1,1)
	while len(ids_to_pull) > 0:
		route_id = ids_to_pull[0]
		today = datetime.datetime.today()
		current_weekday = today.weekday()
		current_hour = today.hour
		weekday = int(df.iloc[route_id]['weekday'])
		hour = int(df.iloc[route_id]['hour'])
		hour_difference = hour - current_hour
		hour_difference = 1 if hour_difference < 1 else 0
		day_difference = weekday - (current_weekday + hour_difference)
		day_difference = (1 + weekday + (6 - current_weekday)) if day_difference < 0 else day_difference
		target_day = datetime.datetime(today.year, today.month, today.day, hour) + datetime.timedelta(days=day_difference)
		departure_time = int((target_day - epoch).total_seconds())
		start_lat = df.iloc[route_id]['start station latitude']
		start_lng = df.iloc[route_id]['start station longitude']
		end_lat = df.iloc[route_id]['end station latitude']
		end_lng = df.iloc[route_id]['end station longitude']
		values = get_response(start_lat, start_lng, end_lat, end_lng, departure_time, counter, total)
		df.set_value(route_id,'routes_status_code', values[0])
		df.set_value(route_id, 'duration', values[1])
		df.set_value(route_id, 'distance', values[2])
		df.set_value(route_id, 'duration_in_time', values[3])
		df.to_csv(routes_filepath, index = False)
		ids_to_pull.remove(route_id)
		counter = counter + 1
	print(df['routes_status_code'].value_counts())
	print(df['routes_status_code'].isnull().sum())
	print("Complete")

dataset_filepath = sys.argv[1]
routes_filepath = sys.argv[2]
get_data(dataset_filepath, routes_filepath)