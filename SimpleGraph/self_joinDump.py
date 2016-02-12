import pyspark
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, Row
from pyspark.sql.types import *
from pyspark.storagelevel import StorageLevel
import sys

keyspace = 'flashback'


def main(argv):
    Conf = (SparkConf().setAppName("SimpleGraph"))
    sc = SparkContext(conf=Conf)
    sqlContext = SQLContext(sc)


    dirPath = "hdfs://ec2-52-71-113-80.compute-1.amazonaws.com:9000/reddit/data/"+argv[1]+".parquet"

    rawDF = sqlContext.read.parquet(dirPath).registerTempTable("comments")
    
    
    
    df = sqlContext.sql("""
    SELECT t1.subreddit as Subreddit,
       
       t1.id as OrigId ,                t2.id as RespId,
       t1.author AS OrigAuth,              t2.author AS RespAuth,
       t1.score  AS OrigScore,             t2.score  AS RespScore,
       t1.ups    AS OrigUps,               t2.ups    AS RespUps,
       t1.downs  AS OrigDowns,             t2.downs  AS RespDowns,
       t1.controversiality AS OrigControv, t2.controversiality AS RespControv
FROM comments t1 INNER JOIN comments t2 ON CONCAT("t1_",t1.id) = t2.parent_id where t1.author!='[deleted]' and t2.author!='[deleted]'
""")

    
    df.write.parquet("hdfs://ec2-52-71-113-80.compute-1.amazonaws.com:9000/reddit/data/"+argv[1]+"-selfjoin.parquet")



if __name__ =="__main__":
    main(sys.argv)
