#!/usr/bin/env python
"""
A module to create a test database.
"""
import sqlite3
import logging
import sys


def main():
	log_format = ' %(asctime)s - %(levelname)s - %(message)s'
	logging.basicConfig(level = logging.DEBUG, format = log_format)
	logging.disable(logging.DEBUG)
	# Create database file
	sqlite_file = sys.argv[1]
	# Set variables with the table names
	table_name1 = 'aud_qty'
	table_name2 = 'timestamps'
	
	try:
		logging.debug('Starting database creation.')
		# Create connection to the database
		with sqlite3.connect(sqlite_file) as conn:
			cursor = conn.cursor()
	
			# Create first table
			cursor.execute('CREATE TABLE IF NOT EXISTS {0} (pivot_id INTEGER PRIMARY KEY,\
															facility_code CHAR(6) default LGN,\
															folder CHAR(125) NOT NULL,\
															zip_file_name CHAR(64),\
															order_num CHAR(14),\
															sent_to_pivot TEXT,\
															print_date TEXT,\
															bill_type CHAR(12),\
															file_name CHAR(64),\
															bill_mgr_email CHAR(64),\
															bill_cnt CHAR(64),\
															ebill_cnt CHAR(64),\
															mailed_cnt CHAR(64),\
															FOREIGN KEY(pivot_id) REFERENCES timestamps(pivot_id)\
															)'.format(table_name1))
		
			cursor.execute('CREATE TABLE IF NOT EXISTS {0} (pivot_id INTEGER PRIMARY KEY,\
															hshake1 TEXT,\
															hsh1_email_sent TEXT,\
															hshake2 TEXT,\
															hsh2_email_sent TEXT,\
															approved TEXT,\
															app_email_sent TEXT,\
															disapproved TEXT,\
															dis_email_sent TEXT,\
															cancelled TEXT,\
															can_email_sent TEXT\
															)'.format(table_name2))

		
			# conn.commit()
	except sqlite3.Error as e:
		print 'Error occured: {0}: '.format(e)
	
	finally:
		logging.debug('Releasing resources.')
		if conn:
			conn.close()

if __name__ == '__main__':
	main()
