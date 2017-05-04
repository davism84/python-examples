# how to plot using matplotlib
# >>> pd.value_counts(df['FLURECUR_TYPE'].values).plot.barh()
#>>> plt.show()
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
datefmtmask = '%Y%m%d'
strMode = False
datevarlist = []

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

# check for valid dates
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
				time.strptime(strDt, datefmtmask)    # make this configurable
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
			fieldnames = ['column', 'count', 'percentage', 'unique_values', 'top_value', 'top_freq', 'second_top_value', 'second_top_value_freq',
				'tertiary_top_value', 'tertiary_top_value_freq', 'invalid_dates']
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
					bins = df[col].value_counts()  # count by categories
					secValStr = '-'
					secValStrFreq = '-'
					terValStr = '-'
					terValStrFreq = '-'
					if (len(bins) > 2):  # set the top 3 if they exist
						secValStr = bins.index[1]
						secValStrFreq = bins[1]
						terValStr = bins.index[2]
						terValStrFreq = bins[2]


					writer.writerow({'column': col, 'count': v[0], 'percentage': '{:3.1f}%'.format(percent), 
						'unique_values': unqValStr, 'top_value': topValStr, 'top_freq': v[3], 'second_top_value': secValStr, 'second_top_value_freq':secValStrFreq, 
						'tertiary_top_value': terValStr, 'tertiary_top_value_freq':terValStrFreq, 'invalid_dates': invalidDt})
				else:
					writer.writerow({'column': col, 'count': v[0], 'percentage': '{:3.1f}%'.format(percent), 
						'top_value': v[6], 'top_freq': '-', 'unique_values': '-', 'invalid_dates': invalidDt})
		finally:
			csvfile.close()

if __name__ == '__main__':
	parser = argparse.ArgumentParser()
	parser.add_argument("datafile", help="data filename")
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

	process()