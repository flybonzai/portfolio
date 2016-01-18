#!/usr/bin/env python
'''
Written 10/15/2015 by McKay Harris

This program is a pre-process script that reads in a CSV file, pulls out
the client recon totals, maps each line to a dictionary, and then outputs 
the data in the following format:
 01 Header Row
 02 Transaction Rows
 03 OCR Line
 Finally, the program checks internal totals against those provided by the 
 client.
'''
import csv
import pdb
import sys


def main():
	infile = sys.argv[1]
	outfile = sys.argv[2]
	try:
		with open(infile, 'rbU') as csvinfile:
			# Create reader object, get fieldnames for later on
			reader, fieldnames = open_reader(csvinfile)
			
			# Create a list of the rows of dictionaries
			data_list = list(reader)
			
			# Capture client totals to compare against internal totals
			client_totals = list(get_recon_totals(data_list))
			print 'Client Totals: {0} {1} {2}.'.format(client_totals[0],
													client_totals[1],
													client_totals[2])
			
			# Create a list to contain section header information										 
			header_list = create_header_list(data_list)
			
			# Create dictionary that contains header list as the key,
			# then a value of all rows that match as a list of dictionaries.
			master_dict = map_data(header_list, data_list)
			
			# Write data to processed file, create recon counts to compare 
			# to client totals
			rrd_totals = list(write_data(master_dict, outfile, fieldnames))
			print 'RRD Totals: {0} {1} {2}.'.format(rrd_totals[0],
													rrd_totals[1],
													rrd_totals[2])
			
			recon_totals(client_totals, rrd_totals)
	except IOError as e:
		print 'Unable to open file:', e
		
def open_reader(infile):
	'''
	Uses DictReader from the csv module to take the first header line
	as the fieldnames, then applies them to each element in the file.
	Returns the DictReader object and the fieldnames being used (used
	later when data is printed out with DictWriter.)
	'''
	reader = csv.DictReader(infile, delimiter=',')
	return reader, reader.fieldnames
	

def create_header_list(data_list):
	'''
	We need a header row to differentiate between individual packages
	to be printed in Dialogue, so we create a header list that 
	contains tuples of the variations on people ID and donation date.
	Each entry in this list will be unique, i.e. the relationship
	between PEOPLE_ID and DON_DATE is one to many.
	'''
	header_list = []
	for row in data_list:
		if (row['PEOPLE_ID'], row['DON_DATE']) not in header_list:
			header_list.append((row['PEOPLE_ID'], row['DON_DATE']))
	return header_list
	
	
def map_data(header_list, data_list):
	'''
	This part of the program takes the header list, matches all rows
	that contain the same PEOPLE_ID/DON_DATE combination, and maps
	them to a dictionary with the header row as the key and a list
	of the matching transaction lines (as dictionaries) as the value.
	It also adds various fields to the header list that will be used 
	for totals on the composed receipt.
	'''
	master_dict = {}
	client_section_list = []
	for element in header_list:
		amt_sum = 0
		for row in data_list:
			if (row['PEOPLE_ID'], row['DON_DATE']) == element:
				try:
					client_section_list.append(row)
					amt_sum += float(row['AMOUNT'])
				except ValueError:
					pass
					
		element = list(element)
		element_list = [client_section_list[0]['DEDUCT_AMT'],
					client_section_list[0]['ND_AMT'],
					client_section_list[0]['DEDUCT_YTD'],
					client_section_list[0]['NONDEDUCT_YTD']
					]
		try:
			element_list.append((float(client_section_list[0]['DEDUCT_YTD']) +
								 float(client_section_list[0]['NONDEDUCT_YTD'])
								 ))
		except ValueError:
			pass
		
		element_list.append(pg_cnt(len(client_section_list))) # creates a page count list for Dialogue
		element_list.append(amt_sum) # BR-0003
		element_list.append(client_section_list[0]['INT_CODE_EX0003'])
		element_list.append(client_section_list[0]['INT_CODE_EX0006'])
		element_list.append(client_section_list[0]['INT_CODE_EX0028'])
		element_list.append(client_section_list[0]['INT_CODE_EX0023'])
		element.extend(element_list) # adds multiple items to the header_list
		element = tuple(element) # set as a tuple to use for our key
		master_dict[element] = client_section_list
		client_section_list = []
		amt_sum = 0
	return master_dict

		
def write_data(in_dict, outfile, in_fieldnames):
	'''
	This function creates two writers, one normal CSV Writer, and then a 
	DictWriter.  This is done because the header list items are not in 
	dictionary form and thus cannot be parsed by the DictWriter.  Before
	each line is added either a 01, specifying a header row, 02,
	which signifies transaction rows, or 03, which is an OCR line row.
	'''
	with open(outfile, 'wb') as writer_outfile:
		writer = csv.writer(writer_outfile, delimiter='|')
		dict_writer = csv.DictWriter(writer_outfile,
									 fieldnames=in_fieldnames,
									 extrasaction='ignore',
									 delimiter='|')
		tot_cnt = 0
		rec_cnt = 0
		email_cnt = 0
		email_cnt_flag = False
		for k, v in in_dict.iteritems():	
			ocr_list = []
			if k[0] != '': # Eliminates the empty lines from the footer
				writer_outfile.write('01|') # Specifies row as a header row
				writer.writerow(k)
				rec_cnt += 1
				for i, e in enumerate(v):
					if v[i]['INT_CODE_EX0006'] != '' \
					or v[i]['INT_CODE_EX0028'] != '':
						email_cnt_flag = True
					# Add the designation numbers to a list and pad them 
					# out to 10 digits with zero's.  
					ocr_list.append(v[i]['DESIG'].rjust(9, '0'))
					writer_outfile.write('02|') # Specifies as secondary row
					dict_writer.writerow(e)
					tot_cnt += 1
				if email_cnt_flag: # Counter should only go up once for each
					email_cnt += 1 # package if it contains the proper flag
					email_cnt_flag = False
				string1 = '03|{0} '.format(v[i]['PEOPLE_ID'].rjust(10, '0')) 
				string2 = '000000000 ' * 6
				# string2 = '0' * 60
				start = 0
				end = start + 10
				cnt = 0
				# pdb.set_trace()
				# Takes the designation numbers from the file and first pads
				# them with zero's.  Then it takes six at a time and writes 
				# them out to the file.  If the total is not a multiple of 
				# 6 it pads the rest of the fields with zero's.
				for ocr_seg in ocr_list:
					if cnt >= 6:
						# pdb.set_trace()
						writer_outfile.write(string1 + string2 + '\n')
						cnt = 0
						start = 0
						end = 10
						string2 = '000000000 ' * 6
														
					string2 = string2[:start] + ocr_seg + ' ' + string2[end:]
					start += 10
					end += 10
					cnt += 1
				writer_outfile.write(string1 + string2 + '\n')

				ocr_list = []
		return tot_cnt, rec_cnt, email_cnt
				
		
def get_recon_totals(data_list):
	'''
	The client file currently comes in with 3 lines at the bottom that contain
	the 3 totals against which we are reconciling.  This function strips them
	out and returns them for use in the recon_totals function.
	'''
	client_tot_cnt = 0
	client_rec_cnt = 0
	client_erec_cnt = 0
	
	for line in data_list:
		if line['RECEIPT_NUMBER'] == 'T' \
		and line['LAST_REASON'] == 'Total Amount':
			print 'Total Amount found.'
			client_tot_cnt = int(line['JAN_AMT'])
		elif line['RECEIPT_NUMBER'] == 'T' \
		and line['LAST_REASON'] == 'Receipt Count':
			print 'Receipt Count found.'
			client_rec_cnt = int(line['JAN_AMT'])
		elif line['RECEIPT_NUMBER'] == 'T' \
		and line['LAST_REASON'] == 'Email Receipt Count':
			print 'E-Receipt Count Found.'
			client_erec_cnt = int(line['JAN_AMT'])
		
	return client_tot_cnt, client_rec_cnt, client_erec_cnt
	

def recon_totals(client_totals, rrd_totals):
	'''
	This function simply compares the three totals we received from the
	client against our own internal counts.
	'''
	for x, y in zip(client_totals, rrd_totals):
		if x != y:
			raise ValueError('Recon Totals Do Not Match!! Client: {0} RRD: {1}'.format(x, y))
	
	print 'Totals matched up!  All is well with the Force.'
	

def pg_cnt(list_length):
	page_cnt = list_length // 6
	rem = list_length % 6
	if rem > 0:
		page_cnt += 1 
	return page_cnt
	
	
if __name__ == '__main__':
	main()
