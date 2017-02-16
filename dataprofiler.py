# how to plot using matplotlib
# >>> pd.value_counts(df['FLURECUR_TYPE'].values).plot.barh()
#>>> plt.show()
import pandas as pd
import csv
import argparse
import time
import logging


df = ''
headers = ''
totalRecs = 0
datafile = ''
outfile = 'stats.csv'
datevarfile = ''
datefmtmask = '%Y%m%d'
errorLog = 'errors.log'
strMode = False
datevarlist = []
invalidDtList = []

# load the actual data file
# return a dataframe
def loadData(filename):
	try:
		print('Loading data file...' + filename)
		if strMode:
			return pd.read_csv(filename, dtype=str, encoding='utf-8')
		else:
			return pd.read_csv(filename)
	except Exception as e:
		raise
	else:
		pass
	finally:
		pass

# load the list of specified date columns from a csv
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

# check for valid dates, this code is specfic to Cancer registry dates
# assuming the following format YYYYMMDD
# dlist - list of dates to check
# returns a count of the number of invalid dates
def checkDates(dlist):
	invalids = 0
	for dt in dlist:
		if dt != dt:
			pass
		else:
			try:
				if strMode == False:  # dates read in as numeric
					strDt = str(int(dt))
				else:
					strDt = dt

				# save original
				origDate = strDt

				# if date contains either 9999 e.g, 20079999 or 99 e.g., 20161299, replace the these
				if strDt != '99999999' and strDt.rfind('9999') > -1:
					strDt = strDt[::-1].replace('9999', '0360')[::-1] # reversed
				elif strDt != '99999999' and strDt.rfind('99') > -1:
					strDt = strDt[::-1].replace('99', '51')[::-1]  # this reverse the string assuming in the format 20161299 will be 20161215

				# if the date values are not either of these missing values flags by CR, then try to test date,
				# otherwise don't count these as invalid
				if strDt != '99999999' and strDt != '0':
					time.strptime(strDt, datefmtmask)    # make this configurable
				#print (dt)
			except Exception as e:
				#print ('invalid date')
				invalids = invalids + 1
				logging.debug('invalid date: orig %s converted %s', origDate, strDt)
				continue
	#print('number of invalid dates ' + str(invalids))
	return invalids

def process():
	df = loadData(datafile)   
	totalRecs = len(df)
	#print (df)
	headers = list(df)   # get a list of headers
	#print (headers)
	#print (len(headers))
	print('Writing data stats file...' + outfile)
	with open(outfile, 'w') as csvfile:
		try:
			fieldnames = ['column', 'count', 'percentage', 'top_value', 'top_freq', 'unique_values', 'invalid_dates']
			writer = csv.DictWriter(csvfile, fieldnames=fieldnames, dialect='excel', lineterminator='\n', quoting=csv.QUOTE_NONNUMERIC)
			writer.writeheader()

			for col in headers:
				v = df[col].describe()
				#print(v)
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
					topValStr = str(v[2])
					unqValStr = str(v[1])
					writer.writerow({'column': col, 'count': v[0], 'percentage': '{:3.1f}%'.format(percent), 
						'top_value': topValStr, 'top_freq': v[3], 'unique_values': unqValStr, 'invalid_dates': invalidDt})
				else:
					writer.writerow({'column': col, 'count': v[0], 'percentage': '{:3.1f}%'.format(percent), 
						'top_value': v[6], 'top_freq': '-', 'unique_values': '-', 'invalid_dates': invalidDt})
		finally:
			csvfile.close()
			logging.shutdown()
			logging.getLogger(None).handlers = []

if __name__ == '__main__':
	parser = argparse.ArgumentParser()
	parser.add_argument("datafile", help="data filename")
#	parser.add_argument("-e", "--errors", action="store_true", help="output errors")	
	parser.add_argument("-s", "--string", action="store_true", help="open file in string mode")
	parser.add_argument("-o", "--outfile", help="output file; default will be: stats.csv")
	parser.add_argument("-d", "--datevars", help="date variable file for checking dates")	
	parser.add_argument("-f", "--datefmtmask", help="date formart mask for checking dates")
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
	if args.datefmtmask:
		datefmtmask = args.datefmtmask
	
	errorFile =  outfile + '-errors.log'	
	logging.basicConfig(filename=errorFile,level=logging.DEBUG)
	process()