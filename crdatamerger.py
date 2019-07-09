from pandas import DataFrame, read_csv
import pandas as pd
import numpy as np
import argparse
import math
import os

infile = ""

def process():
	# read csv
	df = pd.read_csv(infile)
	print("----------------------------------------------")
	print("File: " + infile)
	print("Total Rows: " + str(df['ID'].count()) + " Columns: " + str(len(df.columns)))

	# drop the full row duplicates
	ndf = df.drop_duplicates(keep='last')

	# get those IDs that are dup
	#ndf.loc[ndf.duplicated(["ID"]), :]
	print(ndf)

	# create a temp dup matrix where the 'is_dup_id' is True
	#dup_ids = ndf[ndf.is_dup_id]
	dups = ndf.loc[ndf.duplicated(["ID"]), :]
	print("# of IDs with duplicate rows: " + str(len(dups)))

	# the dup ids
	di = dups["ID"].drop_duplicates()
	print("----------------------------------------------")
	# get column names
	colhdrs = ndf.columns
#	print(colhdrs)

	colmaxvalues = {}
	allDupGrps = pd.DataFrame(columns=colhdrs)

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
		#print(dupsubgrp["ID"])
		# now check to see if we have more than one row in our subgrp
		if dupsubgrp["ID"].count() > 1:
			#print("Subgrp total: " + str(dupsubgrp["ID"].count()))
			idx = 0
			# loop through all the columns to
			# start checking to see how many we need replicate
			for col in colhdrs:
				nonNullTotal = dupsubgrp[col].count()  # get all non null value totals
				nullTotal = sum(pd.isnull(dupsubgrp[col]))  # get those that are null
				rowCount = nonNullTotal + nullTotal
				coldups = dupsubgrp.duplicated([col]) #, keep='first')  # get the dups within the column
				coldupsvals = coldups.values
				#print(col)
				#print(adups)
				#print(dupsubgrp[col])
				# search of all the  non-dups (true), gives back non-empty element array
				element = np.where(coldupsvals == False)
				#if cdups.values.max:
				#print(element)
				#print(len(element[0]))
				dupcnt = len(element[0]) #+ 1   # add one to include the first dup row
				#a = element[0].tolist()
				#cnt = a.count(True) + 1   # include the first element
					#print("Column has dups")
				col = col.strip()
				#if dupcnt > 1:
				v = {'index': str(idx),'repeats': str(dupcnt)}
				try:
					maxval = colmaxvalues[col]  #look up the col max val counter
					if rowCount > maxval:
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
	#print(allDupGrps["ID"])
	print("Processing the sub-groups and merging data for these dup IDs...")
	grouped = allDupGrps.groupby(allDupGrps["ID"])
	grpids = list(grouped.groups.keys())
#	print(grpids)
#	print("----------")

	rowDict = []
	# iterate over all the groups
	for grpid in grpids:
		#print(grpid)
		df0 = grouped.get_group(grpid)

		rowCnt = 0
		k = df0.keys()
		listSeries = ""
		# iterate over one grouping
		for dfRow in df0.iterrows():
			lsRow = list(dfRow)  # convert the tuple to a list

			if rowCnt == 0:
				listSeries = dict(lsRow[1])  # initialize the dictionary

			for chv in colhdrs:  # cycle through all the original column headers
				varInsCnt = 1
				try:
					#chv = chv.strip()
					repeatVal = colmaxvalues[chv] #look up the col max val counter
					repeats = int(repeatVal['repeats'])
					if repeats > 1:
						nextDataVal = lsRow[1][chv]
						if (listSeries[chv] != nextDataVal):
							newLabel = chv + "_" + str(varInsCnt)
							d = {newLabel:nextDataVal}
							listSeries.update(d)   # assuming there are no multiples and just add var/data
							varInsCnt = varInsCnt + 1
				except KeyError:
					pass
			rowCnt = rowCnt+1
#		print(listSeries)
		#d = dict(listSeries) # convert only the data to a dictionary
		rowDict.append(listSeries)

	print("Dictionary...")
	print(rowDict)
	print("Saving the merged records...")
	#expdf = expdf.from_records(rowDict)
	print(expcolhdrs)

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
		#expdf.append(pd.Series(name=record['ID']))
		print(record)
		#print(record.keys())
		for key in expcolhdrs:
#		for key in record.keys():
#			expdf.loc[record['ID'], key] = record[key]
			#expdf.replace({key,record['ID']}, record[key])
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
		ndf = ndf[ndf.ID != int(dupid)]  # drop col with specific ID

	for rowidx, row in ndf.iterrows():
#	for key, value in ndf.iteritems():
		print(dict(row))
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
	print("Original Rows: " + str(df['ID'].count()) + " Columns: " + str(len(df.columns)))
#	print("Merged Rows  : " + str(merged['ID'].count()) + " Columns: " + str(len(merged.columns)) + " Repeat Columns added: " + str(abs(columnDiff)))

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	parser.add_argument("infile", help="Data file to merge")
	args = parser.parse_args()

	if args.infile:
		infile = args.infile

	if infile:
		process()
