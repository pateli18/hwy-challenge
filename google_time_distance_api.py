import pandas as pd
import numpy as np
import json
import sys
import requests

def get_response(start_lat, start_lng, end_lat, end_lng, index, total):
	url = 'https://maps.googleapis.com/maps/api/distancematrix/json?'
	api_key = 'AIzaSyBxr-Lw3k3Vutwslnhd8cdGgLmY7KjTuU4'
	data = {'origins' : '{0},{1}'.format(start_lat,start_lng), 'departure_time':'1490000400',
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
		'end station latitude', 'end station longitude']]
		df['routes_status_code'] = df['routes_id'].apply(lambda x: None)
	ids_pulled = [int(i[0]) for i in df[['routes_id', 'routes_status_code']].values if pd.notnull(i[1])]
	ids_to_pull = [int(i[0]) for i in df[['routes_id', 'routes_status_code']].values if pd.isnull(i[1])]
	total = len(ids_to_pull)
	counter = 1
	while len(ids_to_pull) > 0:
		route_id = ids_to_pull[0]
		start_lat = df.iloc[route_id]['start station latitude']
		start_lng = df.iloc[route_id]['start station longitude']
		end_lat = df.iloc[route_id]['end station latitude']
		end_lng = df.iloc[route_id]['end station longitude']
		values = get_response(start_lat, start_lng, end_lat, end_lng, counter, total)
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