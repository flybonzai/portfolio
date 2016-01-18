#!/usr/bin/env python
"""
Written 12/16/2015 by McKay Harris

Pre-process script for *******.  This process does the following:
    
    - Splits off the Header row ("H") into a separate file for easier
      processing in Dialogue.
"""
from csv import reader, writer, QUOTE_MINIMAL
from datetime import date
from decimal import Decimal
import logging
from os import getcwd, mkdir, path
from sys import argv
from types import IntType, FloatType


def main():
    logging.basicConfig(level=logging.INFO, format='[%(levelname)s] - %(message)s')
    # logging.disable(logging.CRITICAL)

    # I/O files
    infile = argv[1]
    header_file = argv[2]
    transact_file = argv[3]
    report_file = get_rpt_path()

    # Create parser object and run the process
    parser = GPParser(infile, header_file, transact_file, report_file)
    parser.run_process()


class GPParser(object):
    """Object to take a client data file and strip outfile
       header and transaction records into different files.

       Inputs:
       - A single CSV file

       Outputs:
       - A CSV file containing only the header record
       - A CSV file containing all the transacional data

       Instance Variables:
       - self.infile - The infile read in at instance creation
       - self.header_of - The outfile for the header data
       - self.transact_of - The outfile for the transactional data
       - self.header_data - A list of the header rows
       - self.transact_data - A list of the transaction rows
       """

    def __init__(self, infile, header_file, transact_file, report_file):
        self.infile = infile
        self.header_of = header_file
        self.transact_of = transact_file
        self.rpt_of = report_file
        logging.debug('Header file is {0}, '\
                     'Trans file is {1}'.format(self.header_of,
                                                self.transact_of))

    def _create_reader(self):
        """Create a csv reader and calls _parse_headers."""
    
        with open(self.infile, 'r') as inf:
            csv_reader = reader(inf, quotechar='"')
            self._parse_headers(csv_reader)

    def _parse_headers(self, reader_obj):
        """Separate header files ("H") from transaction files."""
    
        headers = []
        transactions = []
    
        for row in reader_obj:
            row_type = row[0]
            logging.debug('Row type is: {0}'.format(row_type))
            if row_type == 'H':
                logging.debug('Row added to header list.')
                headers.append(row)
            else:
                logging.debug('Row added to transaction list.')
                transactions.append(row)
    
        # Debugging and verification
        logging.debug('Header list contains: {0}'.format('\n'.join([str(header) for header
            in headers])))
        logging.debug('Transaction list contains: {0}'.format(
            '\n'.join([str(trans) for trans in transactions])))
    
        # Send the two lists to the _recon_totals function for reconciliation
        self.header_data, self.transact_data = headers, transactions

    def _recon_totals(self):
        """
        Reconcile the check total amount and document count and write out the file name,
        check numbers, vendor names, and timestamp to weekly report.
        """
    
        # Client totals
        client_doc_count = int(self.header_data[0][6])
        client_check_tot = Decimal(str(self.header_data[0][7]))
        # Double check variable typing for reconciliation totals.
        logging.info('Document count is: {0}'.format(client_doc_count))
        # doc_var_type = type(client_doc_count)
        # assert doc_var_type is IntType, 'Doc count is not an integer: {0}'.format(
        #    doc_var_type) 
        logging.info('Check Total is: {0}'.format(client_check_tot))
        # check_var_type = type(client_check_tot)
        # assert check_var_type is FloatType, 'Check tot is not a float: {0}'.format(
        #    check_var_type)
    
        # RRD totals
        rrd_doc_count = 0
        rrd_check_tot = Decimal(str(0.0))
        
        with open(self.rpt_of, 'a') as rpt_outfile:
            for transact in self.transact_data:
                row_type = transact[0]
                logging.debug('Transaction type is: {0}'.format(row_type))
                
                if row_type == 'P':
                    # Reconciliation
                    rrd_doc_count += 1
                    trans_chk_amt = Decimal(str(transact[12]))
                    # trans_chk_type = type(trans_chk_amt)
                    # assert trans_chk_type is FloatType, 'Transaction Check Total is '\
                    #                                     'not a float: {0}'.format(
                    #                                         trans_chk_type)
                    rrd_check_tot += trans_chk_amt
                    # Reporting
                    vend_name = transact[2]
                    file_name = self.infile.split('/')[-1]
                    print('File name', file_name)
                    check_num = transact[9]
                    cur_time = date.today()
                    rpt_outfile.write('{0:<50}{1:<50}{2:<30}{3}\n'.format(file_name,
                                                                          vend_name,
                                                                          check_num,
                                                                          cur_time))
        # Reconcile totals and return the lists for writing if they are correct
        if (client_doc_count, client_check_tot) == (rrd_doc_count, rrd_check_tot):
            logging.info('Recon totals match!')
            return True
        else:
            raise ValueError('\nRecon totals do not match! Client: {0} {1}\n'
                             'RRD {2} {3}'.format(client_doc_count,
                                                  client_check_tot,
                                                  rrd_doc_count,
                                                  rrd_check_tot))

    def run_process(self):
        """Kicks off the entire process."""

        self._create_reader()
        if self._recon_totals():
            self._write_file(self.header_of, self.header_data)
            self._write_file(self.transact_of, self.transact_data)

    def _write_file(self, outfile, data):
        """Write out data to an outfile."""
        with open(outfile, 'wb') as outf:
            logging.info('Writing to file: {0}'.format(outfile))
            csv_writer = writer(outf, quotechar='"', quoting=QUOTE_MINIMAL)
            for row in data:
                csv_writer.writerow(row)
            logging.info('Finished up, exiting program.')

def get_rpt_path():
    # Takes path and splits the "t" or "p" off the dalp/t
    prj_base_dir = path.split(getcwd())[0]
    # lvl = prj_base_dir.split('/')[2][-1].lower()
    report_file_dir = path.join(prj_base_dir, 'reports')
    if not path.exists(report_file_dir):
        mkdir(report_file_dir)
    report_file = path.join(report_file_dir, 'weekly_rpt.txt')
    return report_file


if __name__ == '__main__':
    main()
