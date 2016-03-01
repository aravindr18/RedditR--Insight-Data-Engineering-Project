""" This is a useful tester file to check if the parquet table has been dumped properly 
"""

import sys
import pyspark
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, Row
from pyspark.sql.types import *
from pyspark.storagelevel import StorageLevel

Conf = (SparkConf().setAppName("s3ToParquet"))
sc = SparkContext(conf=Conf)
sqlContext = SQLContext(sc)


parquetFile = sqlContext.read.parquet("hdfs://ec2-52-71-113-80.compute-1.amazonaws.com:9000/reddit/data/RC_2007-10.parquet")

parquetFile.registerTempTable("temp");
testEntry = sqlContext.sql("SELECT count(*) FROM temp WHERE author ='cup' ")

print testEntry.collect()
