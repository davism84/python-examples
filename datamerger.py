from pandas import DataFrame, read_csv
import pandas as pd
import numpy as np
import argparse
import math
import os

infile = ""
KEYNAME = 'ID'

def process():
	# read csv
	df = pd.read_csv(infile, dtype=str, encoding='utf-8')
	print("----------------------------------------------")
	print("File: " + infile)
	print("Total Rows: " + str(df[KEYNAME].count()) + " Columns: " + str(len(df.columns)))

	# drop the full row duplicates
	ndf = df.drop_duplicates(keep='last')

	# get those IDs that are dup
	#ndf.loc[ndf.duplicated([KEYNAME]), :]
#	print(ndf)

	# create a temp dup matrix where the 'is_dup_id' is True
	#dup_ids = ndf[ndf.is_dup_id]
	dups = ndf.loc[ndf.duplicated([KEYNAME]), :]
	print("# of IDs with duplicate rows: " + str(len(dups)))

	# the dup ids
	di = dups[KEYNAME].drop_duplicates()
	print("----------------------------------------------")
	# get column names
	colhdrs = ndf.columns
#	print(colhdrs)

	colmaxvalues = {}
	allDupGrps = pd.DataFrame(columns=colhdrs)

	# maxValues = {}
	#
	# dupSummary = dups.groupby(KEYNAME).count()
	#
	# for col in colhdrs:
	# 	if col != KEYNAME:
	# 		cnt = dupSummary[col].max()
	# 		z = {'repeats': cnt}
	#		maxValues[col] = z

	for c, dupid in di.iteritems():

		# init a new sub group df to loop through columns getting dups
		dupsubgrp = pd.DataFrame(columns=colhdrs)
		# loop through ndf collecting the row data
		for i, row in ndf.iterrows():
			id = row[0]  # get the id
			#print("Checking: " + str(dupid) + " with " + str(id));
			if dupid == id:  # get the id to compare, if its a dup
				#print("Found the dup " + str(dupid))
				# add values to the df
				dupsubgrp.loc[len(dupsubgrp)] = row.values
				allDupGrps.loc[len(allDupGrps)] = row.values
		#print(dupsubgrp[KEYNAME])
		# now check to see if we have more than one row in our subgrp
		if dupsubgrp[KEYNAME].count() > 1:
			#print("Subgrp total: " + str(dupsubgrp[KEYNAME].count()))
			idx = 0
			# loop through all the columns to
			# start checking to see how many we need replicate
			for col in colhdrs:
				nonNullTotal = dupsubgrp[col].count()  # get all non null value totals
				nullTotal = sum(pd.isnull(dupsubgrp[col]))  # get those that are null
				rowCount = nonNullTotal + nullTotal
				coldups = dupsubgrp.duplicated([col], keep='first')  # get the dups within the column
				coldupsvals = coldups.values
				# search of all the  non-dups (true), gives back non-empty element array
				element = np.where(coldupsvals == False)
				dupcnt = len(element[0]) #+ 1   # add one to include the first dup row
				dupLocations = element[0].tolist()
				col = col.strip()


				#if dupcnt > 1:
				v = {'index': str(idx),'repeats': str(dupcnt)}  #, 'repeatvals': dupValues}  #'idxpos': element,
				try:
					maxval = colmaxvalues[col]  #look up the col max val counter
					if dupcnt > int(maxval['repeats']):
						colmaxvalues[col] = v
				except:
					colmaxvalues[col] = v

						#colmaxvalues[col] = rowCount
				idx = idx + 1



	print("Columns that need repeating:")
	print(len(colmaxvalues))
	print(colmaxvalues)
	# build a new df with expanded columns
	expandedColHdrs = []

	print("Expanding the columns with new variables...")
	for c in colhdrs:
		try:
			c = c.strip()
			repeatMaxVal = colmaxvalues[c]  # check to see if column needs expanded
			#print(repeatMaxVal)
			idx = 0
			repeats = int(repeatMaxVal['repeats'])
			while idx < repeats:
				if idx == 0 or repeats < 2:
					expandedColHdrs.append(c)
				else:
					nextColName = c + "_" + str(idx)
					expandedColHdrs.append(nextColName)
				idx = idx + 1
		except:
			expandedColHdrs.append(c.strip())

	print("Expanded column headers...")
	print(expandedColHdrs)

	# new df
	expdf = pd.DataFrame(columns=expandedColHdrs)
	expcolhdrs = expdf.columns

	# loop throught dup sub grp to merge rows
	#print(allDupGrps[KEYNAME])
	print("Processing the sub-groups and merging data for these dup IDs...")
	grouped = allDupGrps.groupby(allDupGrps[KEYNAME])
	grpids = list(grouped.groups.keys())
#	print(grpids)
#	print("----------")

	rowDict = []
	# iterate over all the groups
	for grpid in grpids:
		#print(grpid)
		df0 = grouped.get_group(grpid)

		rowCnt = 0
		listSeries = ""

		# iterate over one grouping
#		for dfRow in df0.iterrows():
		dfRow = df0.iloc[0]  # get the first row
		lsRow = list(dfRow)  # convert the tuple to a list

		listSeries = dict(dfRow)  # initialize the dictionary with first values

		for chv in colhdrs:  # cycle through all the original column headers
			#columnInfo = colmaxvalues[chv] #look up the col max val counter
			#repeats = int(columnInfo['repeats'])
			coldups = df0.duplicated([chv], keep='first')  # get the dups within the column
			coldupsvals = coldups.values
			# search of all the  non-dups (true), gives back non-empty element array
			element = np.where(coldupsvals == False)
			dupcnt = len(element[0])  # + 1   # add one to include the first dup row

			if dupcnt > 1:  # only process repeating columns

				dupLocations = element[0].tolist()

				repeatVals = []

				grpIdxs = grouped.indices[grpid]  # get the group indices, they change for these subgroupings

				for i in dupLocations:
					try:
						grpIdx = grpIdxs[i]    # translate to the actual group index, it does not start at zero
						repeatVals.append(df0[chv][grpIdx])
					except KeyError:
						continue
#				repeatVals = columnInfo['repeatvals']
				varInsCnt = 0  # set variable instance counter
				for rv in repeatVals:
					if varInsCnt == 0:
						varInsCnt += 1
						continue
					else:
						extColName = chv + "_" + str(varInsCnt)  # form next variable name to check against
						d = {extColName: rv}
						listSeries.update(d)  # assuming there are no multiples and just add var/data
					varInsCnt += 1
		rowCnt = rowCnt+1
#		print(listSeries)
		#d = dict(listSeries) # convert only the data to a dictionary
		rowDict.append(listSeries)

#	print("Dictionary...")
#	print(rowDict)
	print("Saving the merged records...")
	#expdf = expdf.from_records(rowDict)
#	print(expcolhdrs)

	fn = infile.split(".")
	csvfile = fn[0] + '-merged.csv'

	if os.path.exists(csvfile):
		os.remove(csvfile)

	outfile = open(csvfile, 'w')

# write out the headers
	for h in expcolhdrs:
		outfile.write(h + ',')
	outfile.write('\n')

	for record in rowDict:
		# add a blank record to dataframe
		#expdf.append(pd.Series(name=record[KEYNAME]))
		#print(record)
		#print(record.keys())
		for key in expcolhdrs:
#		for key in record.keys():
#			expdf.loc[record[KEYNAME], key] = record[key]
			#expdf.replace({key,record[KEYNAME]}, record[key])
			value = None
			try:
				value = record[key]
				if math.isnan(float(value)):
					value = None
			except:
				pass

			if value == None:
				value = ""

			try:
				if value.find(',') > 0:
					outfile.write('"' + str(value) + '"'+ ',')
				else:
					outfile.write(str(value) + ',')
			except:
				outfile.write(str(value) + ',')
				pass
		outfile.write('\n')
			#print(key + ': ' + str(value) + ',')



	print('Updated Dataframe columns...')
	print(expdf.columns)
	print("Post clean-up of duplicate records...")
	# Post clean-up, by removing all the duplicate ids data
	# and retain the non-dup data
	for c, dupid in di.iteritems():
		ndf = ndf[ndf[KEYNAME] != int(dupid)]  # drop col with specific ID

	for rowidx, row in ndf.iterrows():
#	for key, value in ndf.iteritems():
		#print(dict(row))
		dictrow = dict(row)  # convert row to a dictionary
		for key in expcolhdrs:
			try:
				value = dictrow[key]
				try:
					if math.isnan(float(value)):
						value = ""
				except:
					pass

				if value == None:
					value = ""

				try:
					if value.find(',') > 0:
						outfile.write('"' + str(value) + '"'+ ',')
					else:
						outfile.write(str(value) + ',')
				except:
					outfile.write(str(value) + ',')
					pass
			except:
				outfile.write(',')
				pass
		outfile.write('\n')

	outfile.close()

#	mc = len(merged.columns)
	dc = len(df.columns)
#	columnDiff = dc- mc

	print("----------------------------------------------")
	print("Saved merges to File: " + csvfile)
	print("Original Rows: " + str(df[KEYNAME].count()) + " Columns: " + str(len(df.columns)))
#	print("Merged Rows  : " + str(merged[KEYNAME].count()) + " Columns: " + str(len(merged.columns)) + " Repeat Columns added: " + str(abs(columnDiff)))

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	parser.add_argument("infile", help="Data file to merge")
	parser.add_argument("-k", help="set the KEYNAME")
	args = parser.parse_args()

	if args.infile:
		infile = args.infile

	if args.k:
		KEYNAME = args.k

	if infile:
		process()
