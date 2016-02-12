 #!/bin/bash

 while read F ;
 do
 echo $F


 arrIN=(${F//// })
 
 spark-submit --master spark://ip-172-31-1-40:7077 --executor-memory 45G --driver-memory 45G --packages TargetHolding/pyspark-cassandra:0.2.3 --conf spark.cassandra.connection.host=172.31.1.44 flashback.py ${arrIN[1]}


done <fileList.txt
