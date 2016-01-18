#!/usr/bin/env python
"""
This module will check the handshake files sent by Pivot based on the following conventions:
- First handshake file (loaded to the CFL, *auditv2*): Check every half-hour
- Second handshake file (proofs are loaded and available, *handshake*): Check every 2 hours
"""
import smtplib
from email.mime.text import MIMEText
import sqlite3
from os import chdir, listdir
import sys
import logging


def main():
	log_format = ' %(asctime)s - %(levelname)s - %(message)s'
	logging.basicConfig(level = logging.INFO, format = log_format)
	logging.disable(logging.CRITICAL)
	
	# Connect to db
	# sqlite_file = '/bcs/lgnt/clientapp/csvbill/database/McKay/csv.db'
	sqlite_file = '****'
	
	# Email lists to be passed to send_email function
	team1 = []
	team2 = [] 
	try:
		with sqlite3.connect(sqlite_file) as conn:
			cursor = conn.cursor()	
			logging.info('Checking first handshakes')
			# Check first handshakes
			check_hshk(cursor, 'hshake1', 'hsh1_email_sent', 30, team1)
			
			logging.info('Checking second handshakes')
			# Check second handshakes
			check_hshk(cursor, 'hshake2', 'hsh2_email_sent', 120, team2)
			
	except sqlite3.Error as e:
		print 'Error occurred: {0}'.format(e)
		
	finally:
		if conn:
			conn.close()


def check_hshk(cursor, hshk_type, hshk_sent, piv_time_limit, recipient_list):
	"""
	Checks for missing handshake files and sends out an error email if the time limit
	for the individual handshake has been exceeded.  It does this by measuring the 
	amount of seconds that have elapsed since a file was sent to Pivot and converting 
	them into minutes.  This module also updates whether an email has been sent or 
	not in the database, making it so that repeat emails will not be sent.
	Inputs:
	cursor - The SQLite cursor to be used to execute the SQL queries
	hshk_type - For now this is either hshake1 or hshake2, which are 
				the names for the respective columns in the database.
	hshk_sent - This is the column name for the "Sent" column in the 
				database, indicating whether an email has already 
				been sent, and should not be resent.
	piv_time_limit - This is an amount of minutes set by the Pivot
					 team as to how long we should wait for a file
					 before sending out an email.
	recipient_list - The list of email addresses who should receive
					 a missing handshake file email.
	"""
	
	if hshk_type == 'hshake1':
		cursor.execute("""SELECT a.pivot_id, a.zip_file_name, a.sent_to_pivot, a.file_name
						  FROM aud_qty as a JOIN timestamps as t 
						  ON a.pivot_id = t.pivot_id
						  WHERE t.{0} is NULL AND t.{1} is NULL
						  AND ((strftime('%s', DATETIME('now')) - 
						  	    strftime('%s', a.sent_to_pivot)) / (60)) > ?;""".format(hshk_type, hshk_sent), (piv_time_limit,))
	else:
		cursor.execute("""SELECT a.pivot_id, a.zip_file_name, a.sent_to_pivot, a.file_name
						  FROM aud_qty as a JOIN timestamps as t 
						  ON a.pivot_id = t.pivot_id
						  WHERE t.{0} is NULL AND t.{1} is NULL AND hshake1 is NOT NULL
						  AND ((strftime('%s', DATETIME('now')) - 
							    strftime('%s', a.sent_to_pivot)) / (60)) > ?;""".format(hshk_type, hshk_sent), (piv_time_limit,))
	# update sent_email field
	miss_list = [row for row in cursor.fetchall()]
	logging.info(miss_list)
	if miss_list:
		send_email(recipient_list, miss_list, hshk_type)
	piv_id = [element[0] for element in miss_list]
	for id in piv_id:
		cursor.execute("""UPDATE timestamps
						  SET {0} = 'Y'
						  WHERE pivot_id = ?""".format(hshk_sent), (id,))
		logging.debug(miss_list)
				
				
def send_email(recipient_list, missing_list, hshake_type):
	"""
	Sends out an email to everyone on the recipient_list with the file
	name and time the file was sent to Pivot for every file on the 
	missing_list.
	Inputs:
	recipient_list - A list with all the email addresses that should
					 receive an email about the missing files found.
	missing_list - A list of tuples with the Pivot ID, zip file name,
				   and Time Sent to Pivot.  
	"""
	msg = '\n'.join(['File name: {0}\n'
					 'Sent to Pivot at: {1}\n'
					 'Pivot ID: {2}\n'
					 'Property Code: {3}\n\n'.format(element[1],
										    		 element[2],
										    		 element[0],
										    		 element[3]) for element in missing_list])
	msg = MIMEText(msg)
	msg['Subject'] = 'Subject: Alert!! {0} files missing!'.format(hshake_type.capitalize())
	msg['From'] = r'***@***.com'
	msg['To'] = r'To: {0}'.format(', '.join(recipient_list))

	server = smtplib.SMTP(r'mail.rrd.com')
	try:
		server.sendmail(msg['From'], recipient_list, msg.as_string())
	finally:
		server.quit()


if __name__ == '__main__':
	main()
