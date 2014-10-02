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

    query = "SELECT product, interval_from, interval_to, access_count FROM hosting_access"
    query_result = session.execute(query)
    print query_result



# function to run cassandra credentials, in this case, hardcoded
def getCreds(ip):
    return {'username' : 'reader', 'password': 'password'}


if __name__ == "__main__":
# check command line args
    main()
