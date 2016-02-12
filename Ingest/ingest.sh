#!/bin/bash

 while read F ;
 do
 echo $F
 
 spark-submit --master spark://ip-172-31-1-40:7077 --executor-memory 40G --driver-memory 40G s3_to_parquet.py $F

 echo "Stored in HDFS "
 hadoop fs -ls /reddit/data

done <fileList.txt