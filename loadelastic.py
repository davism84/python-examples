import requests, json, os
import argparse
import pandas as pd
import ijson
import time

# Elasticsearch python libs
from elasticsearch import Elasticsearch
from elasticsearch import helpers


directory = ""
indexName = "pathology"
typeName = "reports"
THRESHOLD = 10000  # this regulates how much data gets loaded then is processed in a bulk group

def loadit():
	es = Elasticsearch([{'host': 'localhost', 'port': '9200'}])

	for filename in os.listdir(directory):
		if filename.endswith(".json"):
			json_filename = directory+filename
			print("Loading " + json_filename)			
			
			with open(json_filename, 'r') as input_file:
				i = 1
				batchCtr = 1
				bulk_action = []
				bulkCount = 0
				for rec in ijson.items(input_file, 'item'):
					print(rec['id'])
					bulk = {
						"_index"  : indexName,
						"_type"   : typeName,
						"_id"     : rec['id'],
						"_source" : rec,
					}
					bulk_action.append(bulk)
					i = i + 1
					batchCtr = batchCtr + 1
					if batchCtr > THRESHOLD:
						try:
							#print(bulk_action)
							bulkCount = bulkCount + batchCtr
							helpers.bulk(es, bulk_action)
							print ('Imported data ' + str(bulkCount-1) + ' successfully from ' + json_filename)
							batchCtr = 1
							bulk_action = []
						except Exception as ex:
							print ('Error:' + ex)
				if i < THRESHOLD:
					try:
						helpers.bulk(es, bulk_action)
						print ('Imported data ' + str(i-1) + ' successfully from ' + json_filename)
						batchCtr = 1
						bulk_action = []
					except Exception as ex:
						print ('Error:' + ex)
	

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	parser.add_argument("-d", required=True, help="dir path to json file(s)")
	parser.add_argument("-thres", help="set the batch threshold")
	parser.add_argument("-i", help="set the index name")
	parser.add_argument("-t", help="set the type")

	args = parser.parse_args()

	if args.d:
		directory = args.d
		if directory[-1] != '/':
			directory = directory + '/'
	if args.thres:
		THRESHOLD = int(args.thres)
		print ("Batch threshold: " + str(THRESHOLD))
		print(type(THRESHOLD))
	if args.i:
		indexName = args.i
	if args.t:
		typeName = args.t

	start = time.time()
	loadit()
	end = time.time()
	print("Elapsed time: {}".format((end-start)))



