import json
import pandas as pd
from sklearn import cluster
import psycopg2
import uuid

def skynet(event, context):
	'''
	'''

	body = event['body']

	#make sure that body at the high level has a token
	try:
		try:
			token = body['token']
			#TODO RAISE ERRORS IN A BETTER WAY. MAYBE USE A CONTEXT MANAGER
			uses =  check_token_in_db(token)
		except KeyError:
			raise TokenError('Token not provided in body')

		try:
			validate_high_level_inputs(body)
		### TODO ADD ERRORS
		except :

		addresses = body['addreses']

		number_of_addreses = len(addresses)

		if number_of_addreses > uses and uses != -1:
			raise TokenError("Insufficient token uses for dataset of this size: %s uses remaining" % str(uses))
	except TokenError:
		#some stuff here

	except InputsError:
		#some stuff here too

	guid = uuid.uuid4()
	#api call to second api with guid and events body
	#construct return blo


	to_geocode =[]
	
	errors = []

	lat_long = []
	index = 0
	for address in addresess:
		address['input_order'] = index
		index += 1

		try:

			if address['type'] == 'address':
				assert address['zip'] is not None and address['street1'] is not None
				to_geocode.append(address)
			elif address['type'] == 'geocode':
				assert -180.0 <= float(address['latitude']) <= 180.0 and -180.0 <= float(address['longitude']) <= 180.0
				lat_long.append(address)
			else:
				address['error'] = 'address type not valid'
				errors.append(address)
		except KeyError:
			address['error'] = 'Necessary address fields not provided'
			errors.append(address)
		except ValueError:
			address['error'] = 'Invalid coordinates'
			errors.append(address)
		except AssertionError:
			address['error']= "Address data missing or invalid"
			errors.append(address)



	geocode_addresses, geo_codeerrors = geocode_addresses(to_geocode)
	errors.extend(geo_codeerrors)
	lat_long.extend(geocode_addresses)

	number_of_groups = body['number_of_groups']

	cluster_dict = cluster_addresses(lat_long, number_of_groups)
	for address in lat_long:
		address['group'] = cluster_dict[address['input_order']]

	list_of_clusters.extend(errors)


def db_connect():
	'''
	'''
	conn = psycopg2.connect('dbname=skynet user=lambda_client password= notkillhumans\
	 host=skynet.c5so35nk6wzw.us-west-1.rds.amazonaws.com')

	return conn.cursor()





def cluster_addresses(lat_long, number_of_groups):
	'''
	inputs: 
		list of dictionaries that contain latitudes and longitudes
		number_of_groups
	outputs: pandas data frame with additional column of cluster number
	'''
	df = pd.DataFrame(lat_long)

	pam = cluster.AgglomerativeClustering(n_clusters= number_of_groups)

	prediction = pd.DataFrame({'group':pam.fit_predict(df[['latitude', 'longitude']])})
	clustered_df = pd.concat([df,prediction, axis = 1)
	return {iterable['input_order'] : iterable['group'] for _, iterable in  clustered_df.iterrows()}	



def geocode_addresses(addreses_to_geocode):
	'''
	inputs:
		list of addresses that need to be geocoded
	outputs:
		list of errors
		list of geocoded addresese

	'''

	return geocoded_addreses, errors




class TokenError(Exception):
	def __init__(self,msg):
		self.msg = msg


class InputsError(Exception):
	def __init__(self,msg):
		self.msg = msg



def check_token_in_db(token):
	'''
	inputs:
		token
	outputs:
		number of authorized uses
		errors if token is not present in DB
	'''
	cursor = db_connect()
	cursor.execute('SELECT * FROM tokens WHERE token = %s' %token)
	db_response = cursor.fetchall()
	try:
		uses = db_response[0][1]
	except IndexError:
		raise TokenError('Invalid token')
	return uses


def validate_high_level_inputs(body):
	try:
		assert type(body['addresses']) == list
		assert type(body['number_of_groups']) == int
	except KeyError:
		raise InputsError('Expected fields missing from input')
	except AssertionError:
		raise InputsError('Inputs of incorrect type') 


