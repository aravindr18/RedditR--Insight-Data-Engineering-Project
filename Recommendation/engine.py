""" backbone of the ALS recommendation algorithm """

import pyspark
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, Row
from pyspark.sql.types import *
from pyspark.mllib.recommendation import ALS, Rating
from pyspark.storagelevel import StorageLevel
import sys

keyspace = 'flashback'

def hashFunction(string):
    return abs(hash(string)) % (10 ** 8)

def main(argv):

    Conf = (SparkConf().setAppName("recommendation"))
    sc = SparkContext(conf=Conf)
    sqlContext = SQLContext(sc)

    dirPath = "hdfs://ec2-52-71-113-80.compute-1.amazonaws.com:9000/reddit/recommend/data/sr_userCount.parquet"
    rawDF = sqlContext.read.parquet(dirPath).persist(StorageLevel.MEMORY_AND_DISK_SER)
    # argv[1] is the dump of training data in hdfs
    # argv[2] is the user perferences

    # User Hash Lookup stored into cassandra
    user_hash = rawDF.map(lambda (a,b,c): (a,hashFunction(a)))
    distinctUser = user_hash.distinct()
    userHashDF = sqlContext.createDataFrame(distinctUser,["user","hash"])
    userHashDF.write.format("org.apache.spark.sql.cassandra").options(table ="userhash", keyspace =  keyspace).save(mode="append")
    

    # Product Hash Lookup stored into cassandra
    product_hash = rawDF.map(lambda (a,b,c): (b, hashFunction(b)))
    distinctProduct = product_hash.distinct()
    productHashDF = sqlContext.createDataFrame(distinctProduct,["product","hash"])
    productHashDF.write.format("org.apache.spark.sql.cassandra").options(table ="producthash", keyspace =  keyspace).save(mode="append")

    # Ratings for training
    # ALS requires a java hash of string. This function does that and stores it as Rating Object
    # for the algorithm to consume
    ratings = rawDF.map(lambda (a,b,c) : Rating(hashFunction(a),hashFunction(b),float(c)))

    
    model = ALS.trainImplicit(ratings,10,10,alpha=0.01,seed=5)
    model.save(sc, "hdfs://ec2-52-71-113-80.compute-1.amazonaws.com:9000/reddit/recommend/model")

    sc.stop()


if __name__ =="__main__":
    main(sys.argv)
