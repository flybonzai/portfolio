#!/usr/bin/env python
"""
This program runs in the setvars_audit.sh script and has the 
following purposes:
-Creates a new row with the Primary Key (pivot_id)
-inserts values into the folder, order_num, and file_name 
 fields for the row in sqlite
"""
import sys
import sqlite3
from datetime import datetime


def main():
	order_n = str(sys.argv[1])
	counter = str(sys.argv[2])
	cur_dir = sys.argv[3]
	cur_file = sys.argv[4]
	# gets just the project code
	project_code = '.'.join(cur_file.split('/')[-1].split('.')[-3:-1]) 
	now = datetime.now()
	formatted_dt = '{0}/{1}/{2}-{3}:{4}:{5}'.format(now.month,
												                          now.day,
												                          now.year,
												                          now.hour,
												                          now.minute,
												                          now.second)

	sqlite_file = '/bcs/lgnp/clientapp/csvbill/csv.db'
	csv_connection = sqlite3.connect(sqlite_file)
	c = csv_connection.cursor()
	c.execute('INSERT OR IGNORE INTO aud_qty(pivot_id, '
											                    'folder, '
											                    'order_num, '
											                    'file_name, '
											                    'sent_to_pivot) '
											                    'VALUES(?, ?, ?, ?, DATETIME(\'now\'))', 
											                    ((order_n + counter),
											                    cur_dir, 
											                    order_n, 
											                    project_code))
	csv_connection.commit()
	csv_connection.close()
	
if __name__ == '__main__':
	main()
