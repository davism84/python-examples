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


if __name__ == "__main__":

	parser = argparse.ArgumentParser()
	parser.add_argument("-c", "--cui", required = True, dest = "cui", help="CUI")
	parser.add_argument("-k", "--key", required = True, dest = "apikey", help="API Key")
	args = parser.parse_args()

	cui = args.cui
	apikey = args.apikey

	print ("CUI: " + cui)
	print(getCUI(cui))
	print("Relationships")
	print(getCUIRelations(cui))


