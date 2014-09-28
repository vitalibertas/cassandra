#!/bin/bash
#Hourly product hits to cassandra

# some_table_name (product text,interval_from timestamp,interval_to timestamp,hit_count bigint,PRIMARY KEY (product))


processTimeFile="/some_directory/cassandra/hits_hourly.state"

if [ ! -f "$processTimeFile" ]; then
    echo "Can not find $processTimeFile."
    exit 1
else
    processTime=$(date --date="$(sed 's/T/ /' $processTimeFile)" '+%Y-%m-%d %H:00:00')
fi

if [ -z "$processTime" ]; then
    echo "The last process time cannot be read."
    exit 1
else
    processTime=$(date --date="$processTime 60 minutes" '+%Y-%m-%d %H:00:00')
    processEnd=$(date --date="$processTime 60 minutes" '+%Y-%m-%d %H:00:00')
fi

printf 'product\tinterval_from\tinterval_to\thits\n' > /some_directory/cassandra/product_hits_hourly.tsv
hive -e "SELECT  product
                ,unix_timestamp('$processTime') AS interval_from
                ,unix_timestamp('$processEnd') AS interval_to
                ,COUNT(*) AS hits
           FROM hosting_stats.access_logs
          WHERE event_time >= cast('$processTime' AS timestamp)
            AND event_time < cast('$processEnd' AS timestamp)
       GROUP BY  product
                ,unix_timestamp('$processTime')
                ,unix_timestamp('$processEnd')
       ORDER BY  product
                ,interval_from;" >> /some_directory/cassandra/product_hits_hourly.tsv

if [ $? -eq 0 ]; then
    /some_directory/cassandra/producthits_hourly.py
else
    echo "The Hive script did not process correctly for $processTime."
    exit 1
fi

if [ $? -eq 0 ]; then
    printf "$processTime\n" > $processTimeFile
else
    echo "The Python script did not insert into Cassandra correctly for $processTime."
    exit 1
fi
