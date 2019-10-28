import urllib
import json
import requests
import heapq
import pytz
from pymongo import MongoClient
import pandas as pd
import codecs
from datetime import datetime
from flask_restplus import inputs
from flask import Flask
from flask import request
from flask_restplus import Resource, Api
from flask_restplus import fields
from flask_restplus import reqparse

app = Flask(__name__)
api = Api(app)

# The following is the schema of indicator
indicator_model = api.model('Indicator', { "indicator_id" : fields.String})



@api.route('/Indicator')
class Question1_3(Resource):
	
	@api.response(200, 'Successful')
	@api.response(404, 'Null Dataset')
	@api.doc(description="Get all indicators")
	
	def get(self):
		 
		doc = db.myCollection.find();
		return_dic = []
		tmp_ele = {}
		i = 0
		for indicator_element in doc:
			tmp_ele["location"] = "/Indicator/"+indicator_element["indicator"]
			tmp_ele["collection_id"] = indicator_element["collection_id"]
			tmp_ele["creation_time"] = indicator_element["creation_time"]
			tmp_ele["indicator"] = indicator_element["indicator"]
			return_dic.append(tmp_ele)
			tmp_ele = {}
			 
		return_dic = json.loads(json.dumps(return_dic))
		if len(return_dic) == 0: 
			return {"message": "Null Dataset"}, 404
		return return_dic,200


	@api.response(200, 'OK')
	@api.response(201, 'Created')
	@api.response(400, 'Validation Error')
	@api.response(404, 'Not Found')
	@api.doc(description="Add a new indicator")
	@api.expect(indicator_model, validate=True)
	def post(self):

		#requst
		tmp_indicator = request.json
		url = "http://api.worldbank.org/v2/countries/all/indicators/"+tmp_indicator["indicator_id"]+"?date=2012:2017&format=json&per_page=20000"
		req = requests.get(url)
		df = json.loads(req.content)
		

		######### case: 400 #########

		#case 1: missing idicator
		if 'indicator_id' not in tmp_indicator:
			return {"message": "Missing Indicator"}, 400
 
		#case 2: input indicator id doesn't exist in the data source 
		if 	len(df) == 1:
			return {"message": "Invalid Indicator ID"}, 400

		#case 3: data set is null, e.g.  XGDP.56.FSGOV.FDINSTADM.FFD
		if 	df[1] is None:
			return {"message": "Null Dataset"}, 404
		


		######### case: 200 #########

		return_dic = {}
		indicator = df[1][1]['indicator']['id']
		indicator_value = df[1][1]['indicator']['value']
		tmp_time = datetime.now(pytz.utc)
		cur_time = tmp_time.strftime("%Y-%m-%dT%H:%M:%SZ")
		collection_id = indicator

		print(db.myCollection.find({ "collection_id" : collection_id }).count())
		#case 4: duplicate indidcator
		if db.myCollection.find({ "collection_id" : collection_id }).count() != 0:
			tmp = db.myCollection.find({ "collection_id" : collection_id  })
			return_dic["location"] = "/Indicator/"+ collection_id 
			return_dic["collection_id"] = indicator 
			return_dic["creation_time"] = tmp[0]["creation_time"]
			return_dic["indicator"] = indicator
			return return_dic, 200

		
		#case 5: new indicator, e.g. UPP.INS.DEMO.XQ

		#data cleaning
		df = df[1]
		for element in df :
			element['country'] = element['country']['value']
			del element['decimal'] 
			del element['obs_status']
			del element['unit']
			del element['countryiso3code']
			del element['indicator'] 

		# returning information
		return_dic["location"] = "/Indicator/"+collection_id
		return_dic["collection_id"] = collection_id
		return_dic["creation_time"] = cur_time
		return_dic["indicator"] = indicator


		# Store to MongoDB
		data_to_mongo = {}
		data_to_mongo["collection_id"] = collection_id
		data_to_mongo["indicator"] = indicator
		data_to_mongo["indicator_value"] = indicator_value
		data_to_mongo["creation_time"] = cur_time
		data_to_mongo["entries"] = df

		
		#store data to json
		records = json.loads(json.dumps(data_to_mongo))

		db.myCollection.insert(records)


		return return_dic, 201



@api.route('/Indicator/<collection_id>')
class Question2_4(Resource):

	@api.response(404, 'Indicator was not found')
	@api.response(200, 'Successful')
	@api.doc(description="Delete an indicator by its collection id")
	def delete(self, collection_id):
		

		#case 404: Indicator was not found
		if db.myCollection.find({ "collection_id" : collection_id }).count() == 0:
			return {"message": "Collection = {} is not in the database!".format(collection_id)}, 404

		#case 200: delete otherwise
		db.myCollection.delete_one( { "collection_id" : collection_id } )
		return {"message": "Collection = {} is removed from the database!".format(collection_id)}, 200

	
	@api.response(404, 'Indicator was not found')
	@api.response(200, 'Successful')
	def get(self, collection_id):

		#case 404: Indicator was not found
		if db.myCollection.find({"collection_id" : collection_id}).count() == 0:
			return {"message": "Collection = {} is not in the database!".format(collection_id)}, 404

		#case 200: return the required index
		tmp_data = db.myCollection.find({"collection_id" : collection_id})[0]
		print(tmp_data)

		return_dic = {}
		return_dic["collection_id"] = collection_id
		return_dic["indicator"] = tmp_data["indicator"]
		return_dic["indicator_value"] = tmp_data["indicator_value"]
		return_dic["creation_time"] = tmp_data["creation_time"]
		return_dic["entries"] = tmp_data["entries"]
		

		return_dic = json.loads(json.dumps(return_dic))
		return  return_dic,200



@api.route('/Indicator/<collection_id>/<year>/<country>')
class Question5(Resource):
	
	@api.response(200, 'Successful')
	@api.response(404, 'Indicator was not found')
	@api.doc(description="Get specific indicator")
	
	def get(self, collection_id,year,country):
		
		
		#case 404: Indicator was not found
		if db.myCollection.find( {"collection_id" : collection_id}).count() == 0:
			return {"message": "Collection = {}, year = {}, country = {} is not in the database!".format(collection_id,year,country)}, 404

		doc = db.myCollection.find( {"collection_id" : collection_id})[0]
		entries = doc["entries"]
		

		#case 200: specific element is traced successfully
		for tmp in entries:
			if tmp["country"] == country and tmp["date"] == year:
				
				return_dic = {}
				return_dic["collection_id"] = collection_id
				return_dic["indicator"] = doc["indicator"]
				return_dic["counrty"] = country
				return_dic["year"] = year
				return_dic["value"] = tmp["value"]
				
				return_dic = json.loads(json.dumps(return_dic))
				return return_dic,200

		#case 404: Indicator was not found
		return {"message": "Collection = {}, year = {}, country = {} is not in the database!".format(collection_id,year,country)}, 404


@api.route('/Indicator/<collection_id>/<year>')
class Question6(Resource):

	@api.response(200, 'Successful')
	@api.response(404, 'Indicator was not found')
	@api.doc(description="Get aspecific indicator")
	@api.param("query")
	def get(self,collection_id,year):
		
		# case 1: invalid query
		query = request.values.get("query")
		query = str(query)
		if (query[0] != 't') and (query[0] != 'b'):
			return {"message": "Error Query"}, 404

		#top query
		if query[0] == 't':
			top = query[0:3]
			rank = query[3:]
			query = top
			if (top != 'top') or not rank.isdigit():
				return {"message": "Error Query"}, 404
		#bottom query
		if query[0] == 'b':
			bottom = query[0:6]
			rank = query[6:]
			query = bottom
			if (bottom != 'bottom') or not rank.isdigit():
				return {"message": "Error Query"}, 404


		rank = int(rank)

		if rank<1 or rank > 100:
			return {"message": "Error Query"}, 404
		#case 4: inalid year
		year_check = ['2012','2013','2014','2015','2016','2017']
		if year not in year_check:
			return {"message": "Error Year"}, 404
		#case 3: invalid collection_id
		if db.myCollection.find({"collection_id" : collection_id}).count() == 0:
			return {"message": "Collection = {}, year = {} is not in the database!".format(collection_id,year)}, 404

		doc = db.myCollection.find({"collection_id" : collection_id})[0]
		
		entries = doc["entries"] 
		out_entries = []
		

		for element in entries:
			if element["date"] == year:
				out_entries.append(element)
	
		out_entries= json.loads(json.dumps(out_entries))
		out_entries = sorted( out_entries, key=lambda k: (k['value'] is not None, k['value']), reverse=True)
		
		return_dic = {}
		return_dic["indicator"] = doc["indicator"]
		return_dic["indicator_value"] =  doc["indicator_value"]
		#return_dic["entries"]

		if query == "top":
			out_entries[:]= out_entries[0:rank]
			return_dic["entries"] = out_entries
		
		if query == "bottom":
			out_entries[:]= out_entries[-rank:]
			return_dic["entries"] = out_entries
		
		return  return_dic ,200


if __name__ == '__main__':
	
	#build connection to mongoDB
	connection = MongoClient('mongodb://assi2:password@ds253871.mlab.com:53871/comp9321ass2')
	
	db = connection["comp9321ass2"]

	app.run(debug=True)
	