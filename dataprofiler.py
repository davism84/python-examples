import pandas as pd
import csv
import argparse
import time

df = ''
headers = ''
totalRecs = 0
datafile = ''
outfile = 'stats.csv'
datevarfile = ''
strMode = False
datevarlist = []

def loadData(filename):
	try:
		print('Loading data file...' + filename)
		if strMode:
			return pd.read_csv(filename, dtype=str)
		else:
			return pd.read_csv(filename)
	except Exception as e:
		raise
	else:
		pass
	finally:
		pass

def loadDateColumnVars(datefile):
	l = []
	try:
		print('Loading date variables file...' + datefile)
		v = pd.read_csv(datefile)
		l = list(v)
	except Exception as e:
		pass
	else:
		pass
	finally:
		return l

def checkDates(dlist):
	invalids = 0
	for dt in dlist:
		if dt != dt:
			continue
		else:
			try:
				time.strptime(dt, '%Y%m%d')
				#print (dt)
			except Exception as e:
				#print ('invalid date')
				invalids = invalids + 1
				continue
	#print('number of invalid dates ' + str(invalids))
	return invalids

def process():
	df = loadData(datafile)   #'c:/Users/midavis/Downloads/Breast-DCIS-Cases-Treasure-No-MRNs.csv'
	totalRecs = len(df)
	#print (df)
	headers = list(df)   # get a list of headers
	#print (headers)
	#print (len(headers))
	print('Writing data stats file...' + outfile)
	with open(outfile, 'w') as csvfile:
		try:
			fieldnames = ['column', 'count', 'percentage', 'top_value', 'top_freq', 'unique_values', 'datatype', 'invalid_dates']
			writer = csv.DictWriter(csvfile, fieldnames=fieldnames, dialect='excel', lineterminator='\n')
			writer.writeheader()

			for col in headers:
				v = df[col].describe()
				dtype = df[col].dtype
				percent = (v[0] / totalRecs) * 100
				invalidDt = 0
				# if we have dates identified then do a check
				try:
					idx = datevarlist.index(col)
					dl = list(df[col])
					invalidDt = checkDates(dl)
				except Exception as e:
					invalidDt = '-' 

				if dtype == 'object':
					writer.writerow({'column': col, 'count': v[0], 'percentage': '{:3.1f}'.format(percent), 
						'top_value': v[2], 'top_freq': v[3], 'unique_values': v[1], 'datatype': dtype, 'invalid_dates': invalidDt})
				else:
					writer.writerow({'column': col, 'count': v[0], 'percentage': '{:3.1f}'.format(percent), 
						'top_value': v[6], 'top_freq': '-', 'unique_values': '-', 'datatype': dtype, 'invalid_dates': invalidDt})
		finally:
			csvfile.close()

if __name__ == '__main__':
	parser = argparse.ArgumentParser()
	parser.add_argument("datafile", help="data filename")
	parser.add_argument("-s", "--string", action="store_true", help="open file in string mode")
	parser.add_argument("-o", "--outfile", help="output file; default will be: stats.csv")
	parser.add_argument("-d", "--datevars", help="date variable file for checking dates")	
	args = parser.parse_args()

	if args.datafile:
  		datafile = args.datafile  
	if args.string:
		strMode = True
	if args.outfile:
		outfile = args.outfile
	if args.datevars:
		datevarfile = args.datevars
		datevarlist = loadDateColumnVars(datevarfile)
		#print (datevarlist)

	process()