#!/bin/bash
#Hourly product hits to cassandra

processTimeFile="/home/user/cassandra/hits_hourly.state"
accessLogsTimeFile="/home/user/cassandra/access_hourly.state"
logFile="/home/user/logs/product_hits.log"
email="name@domain.com"

#Check Hosting Access Logs are being pumped into the Hive table.
if [ ! -f "$accessLogsTimeFile" ]; then
    echo "Can not find $accessLogsTimeFile." | mail -s "Hits Job Access Hourly State File Missing" -r hosting-hadoop-noreply@`hostname -f` $email 2>> $logFile
    exit 1
else
    accessTime=$(date --date="$(sed 's/T/ /' $accessLogsTimeFile)" '+%Y-%m-%d %H:00:00')
fi

if [ -z "$accessLogsTimeFile" ]; then
    echo "The last process time for access logs cannot be read." | mail -s "Hits Job Access Hourly Process Time Corrupt" -r hosting-hadoop-noreply@`hostname -f` $email 2>> $logFile
    exit 1
fi

#Check that there is a state file for the last feed to Cassandra and that the time is less than Access Hourly process time.
if [ ! -f "$processTimeFile" ]; then
    echo "Can not find $processTimeFile."
    echo "Error" | mail -s "Cassandra Hits Job State File Missing" -r hosting-hadoop-noreply@`hostname -f` $email 2>> $logFile
    exit 1
else
    processTime=$(date --date="$(sed 's/T/ /' $processTimeFile)" '+%Y-%m-%d %H:00:00')
fi

if [ -z "$processTime" ]; then
    echo "The last process time cannot be read." | mail -s "Cassandra Hits Job Process Time Corrupt" -r hosting-hadoop-noreply@`hostname -f`  $email 2>> $logFile
    exit 1
elif (( $(date --date="$accessTime" '+%s') < $(date --date="$processTime 60 minutes" '+%s') )) ; then
    echo "Cassandra Hits Job Processing is ahead of Access Log Job Processing." | mail -s "Cassandra Hits Job Processing Ahead of Access Log Job Processing!" -r hosting-hadoop-noreply@`hostname -f` $email 2>> $logFile
    exit 1
else
    processTime=$(date --date="$processTime 60 minutes" '+%Y-%m-%d %H:00:00')
    processEnd=$(date --date="$processTime 60 minutes" '+%Y-%m-%d %H:00:00')
fi



printf 'product\tinterval_from\tinterval_to\thits\n' > /home/user/cassandra/product_hits_hourly.tsv
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
                ,interval_from;" >> /home/user/cassandra/product_hits_hourly.tsv

if [ $? -eq 0 ]; then
    /home/user/cassandra/producthits_hourly.py
else
    echo "The Hive script did not process correctly for $processTime." | mail -s "Hits Job Hive Script Update Failed" -r hosting-hadoop-noreply@`hostname -f` $email 2>> $logFile
    exit 1
fi

if [ $? -eq 0 ]; then
    printf "$processTime\n" > $processTimeFile
else
    echo "The Python script did not insert into Cassandra correctly for $processTime." | mail -s "Hits Job Python Script Cassandra Insert Failed" -r hosting-hadoop-noreply@`hostname -f` $email 2>> $logFile
    exit 1
fi
