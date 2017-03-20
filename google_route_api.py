import pandas as pd
import numpy as np
import json
import sys
import requests

def get_response(start_lat, start_lng, end_lat, end_lng, route_type, index, total):
	url = 'https://maps.googleapis.com/maps/api/directions/json?'
	api_key = 'AIzaSyBxr-Lw3k3Vutwslnhd8cdGgLmY7KjTuU4'
	data = {'origin' : '{0},{1}'.format(start_lat,start_lng),
	'destination':'{0},{1}'.format(end_lat,end_lng),'key':api_key,'mode':route_type}
	try:
		response = requests.get(url, params = data)
		status_code = response.status_code
		data = json.loads(response.text)
		print(status_code)
		steps = data['routes'][0]['legs'][0]['steps']
		polylines = [polyline['polyline']['points'] for polyline in steps]
		segment_times = [segment['duration']['value'] for segment in steps]
		values = [status_code, polylines, segment_times]
	except:
		print("Error")
		values = [None, None, None]
	print("{0} out of {1} | {2:.2f}% Complete".format(index, total, index*1./total * 100))
	return values

def get_data(dataset_filepath, routes_filepath, route_type):
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
		values = get_response(start_lat, start_lng, end_lat, end_lng, route_type, counter, total)
		df.set_value(route_id,'routes_status_code', values[0])
		df.set_value(route_id, route_type + '_polylines', str(values[1]))
		df.set_value(route_id, route_type + '_duration', str(values[2]))
		df.to_csv(routes_filepath, index = False)
		ids_to_pull.remove(route_id)
		counter = counter + 1
	print(df['routes_status_code'].value_counts())
	print(df['routes_status_code'].isnull().sum())
	print("Complete")

dataset_filepath = sys.argv[1]
routes_filepath = sys.argv[2]
route_type = sys.argv[3]
get_data(dataset_filepath, routes_filepath, route_type)