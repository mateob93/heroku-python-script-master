import os
import time
from datetime import datetime
import sys
import numpy as np
from psycopg2 import extensions, connect, InterfaceError, OperationalError, errorcodes, errors
import psycopg2.errors
from psycopg2 import __version__ as psycopg2_version

# Estabish connection
conn = psycopg2.connect(host="ec2-174-129-254-216.compute-1.amazonaws.com", database="dffja4igmjagb2",
                        user="rqalxjjwytlbpa",
                        password="ae2da8dade7014ca5e1f6cc5af99b995ebc39f47035079b316e2101b9cdcba78")
                        
##
# Function to convert   
def listToString(s):
    # initialize an empty string 
    str1 = ""

    # traverse in the string   
    for ele in s:
        str1 += ele

        # return string
    return str1

# define a function that parses the connection's poll() response
def check_poll_status():
    """
    extensions.POLL_OK == 0
    extensions.POLL_READ == 1
    extensions.POLL_WRITE == 2
    """

    if conn.poll() == extensions.POLL_OK:
        print ("POLL: POLL_OK")
    if conn.poll() == extensions.POLL_READ:
        print ("POLL: POLL_READ")
    if conn.poll() == extensions.POLL_WRITE:
        print ("POLL: POLL_WRITE")
    return conn.poll()
    
# define a function that returns the PostgreSQL connection status
def get_transaction_status():

    # print the connection status
    print ("\nconn.status:", conn.status)

    # evaluate the status for the PostgreSQL connection
    if conn.status == extensions.STATUS_READY:
        print ("psycopg2 status #1: Connection is ready for a transaction.")

    elif conn.status == extensions.STATUS_BEGIN:
        print ("psycopg2 status #2: An open transaction is in process.")

    elif conn.status == extensions.STATUS_IN_TRANSACTION:
        print ("psycopg2 status #3: An exception has occured.")
        print ("Use tpc_commit() or tpc_rollback() to end transaction")

    elif conn.status == extensions.STATUS_PREPARED:
        print ("psycopg2 status #4:A transcation is in the 2nd phase of the process.")
    return conn.status

# define a function that handles and parses psycopg2 exceptions
def print_psycopg2_exception(err):
    # get details about the exception
    err_type, err_obj, traceback = sys.exc_info()

    # get the line number when exception occured
    line_num = traceback.tb_lineno
    
    # print the connect() error
    print ("\npsycopg2 ERROR:", err, "on line number:", line_num)
    print ("psycopg2 traceback:", traceback, "-- type:", err_type)

    # psycopg2 extensions.Diagnostics object attribute
    print ("\nextensions.Diagnostics:", err.diag)

    # print the pgcode and pgerror exceptions
    print ("pgerror:", err.pgerror)
    print ("pgcode:", err.pgcode, "\n")
    
# Write single values to DB
def writeToDatabase(cols, values, tablename):
    sqlquery = "INSERT INTO " + tablename + "("
    for i in range(len(cols) - 1):
        sqlquery = sqlquery + cols[i] + ","

    sqlquery = sqlquery + cols[-1] + ") VALUES("
    for i in range(len(cols) - 1):
        sqlquery = sqlquery + "%s,"
    sqlquery = sqlquery + "%s);"
    cur = conn.cursor()
    try:
        cur.execute(sqlquery, values)
    except Exception as err:
        print_psycopg2_exception(err)
        conn.rollback()

    conn.commit()
    cur.close()

# Write multiple values to DB
def writeManyToDatabase(cols, values, tablename):
    sqlquery = "INSERT INTO " + tablename + "("
    for i in range(len(cols) - 1):
        sqlquery = sqlquery + cols[i] + ","

    sqlquery = sqlquery + cols[-1] + ") VALUES {}"
    cur = conn.cursor()
    tup = tuple(map(tuple, values))
    args_str = ','.join(['%s'] * len(tup))
    sqlquery = sqlquery.format(args_str)
    try:
        cur.execute(sqlquery, tup)
    except Exception as err:
        print_psycopg2_exception(err)
        conn.rollback()
    conn.commit()
    cur.close()

def appendData(data, cols, vals, arrayInput):
    label = ""
    if data[0] in arrayInput:
        cols.append(data[0])
        vals.append(data[1])
        label = label + "%s"
    elif data[0].isnumeric():
        date = datetime.strptime(data[0], '%Y%m%d%H%M%S').strftime('%m/%d/%Y %H:%M:%S')
        cols.append('DATE')
        vals.append(date)
        label = label + "%s"

    return cols, vals, label
    
def main(InData):
    
    InData = InData.replace("\r","")
    start_time = time.time()

    # get transaction status BEFORE
    get_transaction_status()

    # get the poll status BEFORE
    check_poll_status()

    # Define columns headers
    paquete_headers = ['ID', 'LEN'];
    segment_headers = ['SEGMENT_ID', 'DATE', 'MICROSEC', 'RATE', 'FLAG1', 'FLAG2', 'FLAG3', 'FLAG4']
    data_headers = ['time', 'acc', 'magfieldx', 'magfieldy', 'magfieldz']

    # Convert file, if exists, to .txt
    '''if not os.path.exists('paquete_ejemplo.txt'):
        pre, ext = os.path.splitext('paquete_ejemplo')
        os.rename('paquete_ejemplo', pre + '.txt')

    file = open("paquete_ejemplo.txt", "r", encoding="utf-8");'''
    content = listToString(InData);
    content = content.split('\nBEGIN_SEGMENT\n');

    lines = content[0].splitlines()
    columns = []
    values = []
    # Write to packet table
    for line in lines:
        line = line.split(":")
        if line[0] == 'ID':
            packetID = line[1]
        [columns, values, label] = appendData(line, columns, values, paquete_headers)

    writeToDatabase(columns, values, "paquete")

    # Write to segments table
    for segment in content[1:]:
        segment = segment.split('\nBEGIN_DATA\n')
        aux = segment[1].split('\nEND_DATA\n')
        data = aux[0]
        lines = segment[0].splitlines()
        columns = []
        values = []
        columns.append('PACKETID')
        values.append(packetID)
        for line in lines:
            line = line.split(":")
            if line[0] == 'SEGMENT_ID':
                segID = line[1]
            [columns, values, label] = appendData(line, columns, values, segment_headers)

        writeToDatabase(columns, values, "segments")

        # Write to data table
        n = 2
        data = [data[i:i + n] for i in range(0, len(data) - 1, n)]
        rows = int(len(data) / 5)
        dataaux = np.asarray(data)
        dataaux = dataaux.reshape(rows, 5)
        segment_ids = np.full(
            shape=(rows, 1),
            fill_value=segID)
        packet_ids = np.full(
            shape=(rows, 1),
            fill_value=packetID)
        values = np.column_stack((dataaux, segment_ids, packet_ids))
        #values = np.column_stack((values,))

        columns = []
        columns = columns + data_headers
        columns.append('seg_id')
        columns.append('packet_id')
        writeManyToDatabase(columns, values, "DATA")

    conn.close()
    '''# polling the connection will now raise an error
    try:
        # get the poll one last time
        check_poll_status()
    except InterfaceError as error:
        print ("psycopg2 ERROR:", error)'''

    print("--- %s seconds ---" % (time.time() - start_time))
    
if __name__ == "__main__":
    main(sys.argv[1])
