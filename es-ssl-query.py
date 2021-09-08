# Elasticsearch python libs
from elasticsearch import Elasticsearch
from elasticsearch import helpers
from ssl import create_default_context


def do():
	ssl_context = create_default_context(cafile="<CERT HERE>")

	es = Elasticsearch(['<HOST>'],
		http_auth=('USERNAME', 'PASSWORD'),
		scheme="https",
		port=9200,
		ssl_context=ssl_context
		)

	sample_by_date = {
		"query": {
			"bool": {
				"must": [],
				"filter": [
				{
					"bool": {
						"should": [
						{
							"match_phrase": {
								"REPORT_YEAR.keyword": "2019"
							}
						}
						]
						}
				}
				]
			}
		}
	}

	res = es.search(index="neptune", body=sample_by_date)

	for rec in res['hits']['hits']:
		print(rec["_source"])

if __name__ == "__main__":
	do()