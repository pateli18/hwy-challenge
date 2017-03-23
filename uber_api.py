import pandas as pd
import numpy as np
import json
import sys
import requests
import time

def get_response(start_lat, start_lng, end_lat, end_lng, index, total):
	url = 'https://api.uber.com/v1.2/estimates/price?start_latitude={0}&start_longitude={1}&end_latitude={2}&end_longitude={3}'.format(start_lat, start_lng, end_lat, end_lng)
	api_token ='Rmah_eOq6rBjc-UjQZtszGJX3JAaqhf620WaHYgM'
	authorization_token = 'Token ' + api_token 
	try:
		response = requests.get(url, headers={'Authorization': authorization_token,
			'Accept-Language':'en_US','Content-Type':'application/json'})
		status_code = response.status_code
		data = json.loads(response.text)
		print(status_code)
		uber_pool = data['prices'][0]
		uber_x = data['prices'][1]
		values = [status_code, uber_pool['distance'], uber_pool['duration'], 
		uber_pool['high_estimate'], uber_pool['low_estimate'],
		uber_x['distance'], uber_x['duration'], uber_x['high_estimate'], uber_x['low_estimate']]
	except:
		print("Error")
		values = [None, None, None, None, None, None, None, None, None]
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
		start_time = time.time()
		values = get_response(start_lat, start_lng, end_lat, end_lng, counter, total)
		df.set_value(route_id,'routes_status_code', values[0])
		df.set_value(route_id, 'uber_pool_distance', values[1])
		df.set_value(route_id, 'uber_pool_duration', values[2])
		df.set_value(route_id, 'uber_pool_high_estimate', values[3])
		df.set_value(route_id, 'uber_pool_low_estimate', values[4])
		df.set_value(route_id, 'uber_x_distance', values[5])
		df.set_value(route_id, 'uber_x_duration', values[6])
		df.set_value(route_id, 'uber_x_high_estimate', values[7])
		df.set_value(route_id, 'uber_x_low_estimate', values[8])
		df.to_csv(routes_filepath, index = False)
		ids_to_pull.remove(route_id)
		counter = counter + 1
		end_time = time.time()
		time.sleep(max(1.8 - (end_time - start_time), 0))
	print(df['routes_status_code'].value_counts())
	print(df['routes_status_code'].isnull().sum())
	print("Complete")

dataset_filepath = sys.argv[1]
routes_filepath = sys.argv[2]
get_data(dataset_filepath, routes_filepath)