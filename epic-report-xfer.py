# how to plot using matplotlib
# >>> pd.value_counts(df['FLURECUR_TYPE'].values).plot.barh()
#>>> plt.show()
import pandas as pd
import csv
import argparse
import time
import logging


textColName = ''
outfile = 'epic-report.txt'


def process(datafile):
	#df = loadData(datafile)   
	print('converting to text style report...' + datafile)
	out = open(outfile, 'a')
	with open(datafile) as csvfile:

		try:
			reader = csv.DictReader(csvfile)
			lastMrn = ''
			cnt = 0
			for row in reader:
				mrn = row['MEDIPAC_MRN']
				if cnt == 0:
					lastMrn = mrn
					out.write(generateHeader(mrn))
				if mrn ==  lastMrn:
					out.write(row[textColName])
				else:
					out.write('\nE_O_R\n\n')
					out.write(generateHeader(mrn))
					out.write(row[textColName])
				lastMrn = mrn
				cnt = cnt + 1
		finally:
			out.close()
			#logging.shutdown()
			#logging.getLogger(None).handlers = []
def generateHeader(mrn):
	return "S_O_H\n|" + str(mrn) + "||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||\nE_O_H\n"


if __name__ == '__main__':
	parser = argparse.ArgumentParser()
	parser.add_argument("reportfile", help="EPIC report filename")
	parser.add_argument("-t", "--textfield", dest="textfield", required = True, help="field containing text report")
	parser.add_argument("-o", "--outfile", dest="outfile", required = False, help="file for output report")
	args = parser.parse_args()

	if args.reportfile:
		datafile = args.reportfile
	
	textColName = args.textfield

	if args.outfile:
		outfile = args.outfile

	process(datafile)