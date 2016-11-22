# this builds a master file for using with ETL process
import json
import requests
import csv
import ast
#import simplejson as json

masterfile = 'data/HIPEC-Master.csv'
masterMRNs = {}
masterIds = []
url = "https://www.ctsiredcap.pitt.edu/redcap/api/"
token = '<ADD TOKEN HERE'
	
# payload fields for redcap
staticFields = {'token': token, 'format':'json', 'content':'record'}

# select which field names you want from redcap
fields = ['study_id', 'date_of_surgery']

# general method for formating a multipart message
def encode_multipart_formdata(staticFields, fields, records):
    """
     Return (content_type, body) ready for httplib.HTTP instance
    """
    BOUNDARY = '---011000010111000001101001'
    CRLF = '\r\n'
    L = []
    for (key, value) in staticFields.items():
        L.append('--' + BOUNDARY)
        L.append('Content-Disposition: form-data; name="%s"' % key)
        L.append('')
        L.append(value)

    i = 0
    for value in fields:
        L.append('--' + BOUNDARY)
        L.append('Content-Disposition: form-data; name="fields[%s]"' % str(i))
        L.append('')
        L.append(value)
        i=i+1

    i = 0
    for value in records:
        L.append('--' + BOUNDARY)
        L.append('Content-Disposition: form-data; name="records[%s]"' % str(i))
        L.append('')
        L.append(str(value))
        i=i+1
    L.append('--' + BOUNDARY + '--')
    L.append('')
    body = CRLF.join(L)
    content_type = {
#		'content-type': "multipart/form-data; boundary=---011000010111000001101001",
    	'cache-control': "no-cache"
    }

    return content_type, body

def readMaster():
	print ('Reading ' + masterfile)
	f = open(masterfile)
	master_f = csv.reader(f)

	next(master_f)  # skip the header
	for row in master_f:
  		studyId = row[0]
  		epic = row[4]
  		if epic:
  			epic = epic.rjust(9, '0') 

  		medipac = row[5] 
  		if medipac:
  			medipac = medipac.rjust(9, '0') 

  		masterMRNs[studyId] = epic + ',' + medipac # get epic and mrn
  		masterIds.append(int(row[0]))   # convert the ids to int
	f.close()	
	print (len(masterMRNs))

def getRecordfromRedcap():
	# you can specify certain records
	records = []

	# build the multipart body
	content, body = encode_multipart_formdata(staticFields, fields, records)

	headers = {
	'content-type': "multipart/form-data; boundary=---011000010111000001101001",
    'cache-control': "no-cache"
    }
    # post
	response = requests.request("POST", url, data=body, headers=headers)
	rec = ast.literal_eval(response.text)
	return rec

#def buildNewMaster():
#	for study_id in masterIds:

def buildMaster():
	print ('Building new master file...')
	o = open('data/mars-master2.csv', 'w')
	try:
		header = 'study_id, epic_mrn, medipac_mrn, surg_date_1, surg_date_2, surg_date_3, surg_date_4, surg_date_5'
		o.write(header)
		o.write('\n')
		hipecs = getRecordfromRedcap()
		for studyid in masterMRNs:			
			mrns = masterMRNs[studyid]

			if len(mrns) > 1:  # only build those with mrns
				surgDates = ''
				for rec in hipecs:
					sid = rec['study_id']
					#print (type(sid))
					#print (type(studyid))
					if sid == int(studyid):
						if rec['date_of_surgery']:
							surgDates = surgDates + rec['date_of_surgery'] + ','
						#print (rec['redcap_event_name'] + '=' + rec['date_of_surgery'])
				o.write(str(studyid) + ',' + mrns + ',' + surgDates)
				o.write('\n')
	finally:
		o.close()

def main():
	##r= getRecordfromRedcap(1)    # test
	readMaster()	
	buildMaster()
	print('done')

if __name__ == '__main__':
	main()