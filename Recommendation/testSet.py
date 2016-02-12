
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, Row
from pyspark.sql.types import *
#import pyspark_cassandra


from pyspark.sql.types import *
from pyspark.storagelevel import StorageLevel
import sys
from pyspark.mllib.recommendation import ALS, MatrixFactorizationModel, Rating

def main(argv):
    Conf = (SparkConf().setAppName("test"))
    sc = SparkContext(conf=Conf)
    sqlContext = SQLContext(sc)
    dirPath = 'hdfs://ec2-52-71-113-80.compute-1.amazonaws.com:9000/reddit/recommend/model'
    sameModel = MatrixFactorizationModel.load(sc, dirPath)

    
    b= sameModel.recommendProductsForUsers(193667506)
    print b.take(10)

    sc.stop()

if __name__ =="__main__":
    main(sys.argv)
