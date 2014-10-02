#!/usr/bin/python27-virtual-hadoop

from cassandra.cluster import Cluster
from subprocess import Popen,PIPE
import sys, csv, datetime

def main():
    #Connect to cassandra and keyspace
    cluster = Cluster(['123.456.789.0', '123.456.789.1', '123.456.789.2'],
        protocol_version = 1, #very important
        auth_provider=getCreds)
    session = cluster.connect('hosting_stats')

    with open('/home/user/cassandra/product_hits_hourly.tsv', 'rb') as csvfile:
        dataDictionary = csv.DictReader(csvfile, delimiter='\t')
  	    # hosting_access (product text,interval_from timestamp,interval_to timestamp,access_count bigint,PRIMARY KEY (product))
        # Header in the file is "product\tinterval_from\tinterval_to\thits\n".
        if checkFile(dataDictionary) > 0:
            for row in dataDictionary:
                insert_query = "INSERT INTO hosting_access(product,interval_from,interval_to,access_count) "
                insert_query += "VALUES ('%s',%s,%s,%s)" % (row['product'], long(row['interval_from']), long(row['interval_to']), long(row['hits']))
                try:
                    query_result = session.execute(insert_query)
                except:
                    print "Cassndra Insert Exception: '%s'\nQuery: '%s'" % (sys.exc_info()[0], insert_query)
                    raise
        else:
            sys.exit("There is no data in the import file!")


# function to run cassandra credentials, in this case, hardcoded
def getCreds(ip):
    return {'username' : 'writer', 'password': 'password'}

def checkFile(dataDictionary):
    return len(list(dataDictionary))

if __name__ == "__main__":
# check command line args
    main()
