#!/usr/bin/python27-virtual-hadoop

from cassandra.cluster import Cluster
from subprocess import Popen,PIPE
import sys, csv, datetime

def main():
    #Connect to cassandra and keyspace
    cluster = Cluster(['some_IP_or_DNS'],
        protocol_version = 1, #very important
        auth_provider=getCreds)
    session = cluster.connect('hosting_stats')

    with open('/some_directory/product_hits_hourly.tsv', 'rb') as csvfile:
        dataDictionary = csv.DictReader(csvfile, delimiter='\t')
  	    # some_table_name (product text,interval_from timestamp,interval_to timestamp,hit_count bigint,PRIMARY KEY (product))
        # Header in the file is "product\tinterval_from\tinterval_to\thits\n".
        for row in dataDictionary:
            insert_query = "INSERT INTO some_table_name(product,interval_from,interval_to,hit_count) "
            insert_query += "VALUES ('%s',%s,%s,%s)" % (row['product'], long(row['interval_from']), long(row['interval_to']), long(row['hits']))
            try:
                query_result = session.execute(insert_query)
            except:
                print "Cassndra Insert Exception: '%s'\nQuery: '%s'" % (sys.exc_info()[0], insert_query)
                raise


# function to run cassandra credentials, in this case, hardcoded
def getCreds(ip):
    return {'username' : 'write_user', 'password': 'write_password'}

if __name__ == "__main__":
# check command line args
    main()
