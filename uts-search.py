# uses the UTS API to search for concepts
# usage:   python uts-search.py -cui C0006104 -key <apikey>
import requests
import json
import argparse
import lxml.html as lh
from lxml.html import fromstring

apikey = ""

# get a ticket granting ticket for this session.
def getTGT():
	url = "https://utslogin.nlm.nih.gov/cas/v1/api-key"

	payload = "apikey=" + apikey
	headers = {
		'content-type': "application/x-www-form-urlencoded",
	}

	response = requests.request("POST", url, data=payload, headers=headers)

	response = fromstring(response.text)
	## extract the entire URL needed from the HTML form (action attribute) returned - looks similar to https://utslogin.nlm.nih.gov/cas/v1/tickets/TGT-36471-aYqNLN2rFIJPXKzxwdTNC5ZT7z3B3cTAKfSc5ndHQcUxeaDOLN-cas
	## we make a POST call to this URL in the getst method
	tgt = response.xpath('//form/@action')[0]

	#print(tgt)
	return tgt

# Get a Service Ticket using the Ticket Granting Ticket
def getST(tgt):
	url = tgt   #"https://utslogin.nlm.nih.gov/cas/v1/api-key/" + tgt

	payload = "service=http%3A%2F%2Fumlsks.nlm.nih.gov"
	headers = {
		'content-type': "application/x-www-form-urlencoded",
	}

	response = requests.request("POST", url, data=payload, headers=headers)

	#print(response.text)
	return response.text

def getCUIRelations(cui):
	tgt = getTGT() # get a ticket granting ticket for this session.
	st = getST(tgt) # Get a Service Ticket using the Ticket Granting Ticket

	url = "https://uts-ws.nlm.nih.gov/rest/content/current/CUI/" + cui +"/relations"

	querystring = {"ticket":st}

	headers = {
		'cache-control': "no-cache",
	}

	response = requests.request("GET", url, headers=headers, params=querystring)

	#print(response.text)
	return response.text

def parseRelations(resp):
	relations = []
	jdata = json.loads(resp)
	rels = jdata['result']
	for r in rels:
		name = r['relatedIdName']
		rid = r['relatedId']
		start = rid.rfind("/")
		cui = rid[start+1:len(rid)]
		relative = {'cui':cui, 'name':name}
		relations.append(relative)
	return relations

def getCUI(cui):

	tgt = getTGT() # get a ticket granting ticket for this session.
	st = getST(tgt) # Get a Service Ticket using the Ticket Granting Ticket

	url = "https://uts-ws.nlm.nih.gov/rest/content/current/CUI/" + cui

	querystring = {"ticket":st}

	headers = {
		'cache-control': "no-cache",
	}

	response = requests.request("GET", url, headers=headers, params=querystring)

	#print(response.text)
	return response.text

def textSearch(text):
	results = []
	tgt = getTGT() # get a ticket granting ticket for this session.
	st = getST(tgt) # Get a Service Ticket using the Ticket Granting Ticket

	url = "https://uts-ws.nlm.nih.gov/rest/search/current"

	querystring = {"string":text,"ticket": st}

	headers = {
		'cache-control': "no-cache",
		'postman-token': "129575d9-1c67-1439-3d42-f18e18c92f0a"
		}

	response = requests.request("GET", url, headers=headers, params=querystring)
	return response.text

def parseTextResults(resp):
	results = []
	jdata = json.loads(resp)
	rels = jdata['result']['results']
	for r in rels:
		#print(r)
		name = r['name']
		cui = r['ui']
		x = {'cui':cui, 'name':name}
		results.append(x)
	return results


if __name__ == "__main__":

	parser = argparse.ArgumentParser()
	parser.add_argument("-c", "--cui", dest = "cui", help="CUI")
	parser.add_argument("-t", "--text", dest = "searchText", help="Search text")
	parser.add_argument("-k", "--key", required = True, dest = "apikey", help="API Key")
	args = parser.parse_args()

	cui = args.cui
	apikey = args.apikey
	searchText = args.searchText

	if cui:
		print ("CUI: " + cui)
		relatives = parseRelations(getCUIRelations(cui))
		if relatives:
			print("Relationships")
			for x in relatives:
				print(x['name'] + ' (' + x['cui'] + ')' )
	if searchText:
		print("Searching for ..."+searchText)
		results = parseTextResults(textSearch(searchText))
		if results:
			# quickly find the searched text in the list of results
			match = filter(lambda x: x['name'].lower() == searchText.lower(), results)
			if match:
				for c in match:
					print('Search Term: ' + c['name'] + ' (' + c['cui'] + ')' )  # display my match string
			print("Suggested Concepts...")
			for x in results:
				print(x['name'] + ' (' + x['cui'] + ')' )



