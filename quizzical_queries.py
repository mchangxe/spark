from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
import argparse

def setup_table(sc, sqlContext, reviews_filename):
    '''
    Args:
        sc: The Spark Context
        sqlContext: The Spark SQL context
        reviews_filename: Filename of the Amazon reviews file to use, where each line represents a review    
    Parse the reviews file and register it as a table in Spark SQL in this function
    '''
    content = sqlContext.read.csv(reviews_filename, header=True, inferSchema=True)
    content.createOrReplaceTempView("content")
    content.printSchema()

def query_1(sc, sqlContext):
    '''
    Args:
        sc: The Spark Context
        sqlContext: The Spark SQL context
        reviews_filename: Filename of the Amazon reviews file to use, where each line represents a review
    Returns:
        An string: the review text of the review with id `22010`
    '''
    ret = sqlContext.sql("SELECT Text FROM content WHERE Id=22010").collect()
    return ret[0][0]


def query_2(sc, sqlContext):
    '''
    Args:
        sc: The Spark Context
        sqlContext: The Spark SQL context
    Returns:
        An int: the number of 5-star ratings the product `B000E5C1YE` has
    '''
    ret = sqlContext.sql("SELECT COUNT(*) FROM content WHERE Score = '5' AND ProductId = 'B000E5C1YE' ").collect()
    return ret[0][0]

def query_3(sc, sqlContext):
    '''
    Args:
        sc: The Spark Context
        sqlContext: The Spark SQL context
    Returns:
        An int: the number unique (distinct) users that have written reviews
    '''
    ret = sqlContext.sql("SELECT COUNT(DISTINCT UserId) FROM content").collect()
    return ret[0][0]

if __name__ == '__main__':
    # Get input/output files from user
    parser = argparse.ArgumentParser()
    parser.add_argument('input', help='File to load Amazon review data from')
    args = parser.parse_args()

    # Setup Spark
    conf = SparkConf().setAppName("quizzical_queries")
    sc = SparkContext(conf=conf)
    sc.setLogLevel("ERROR") 
    sqlContext = SQLContext(sc)

    setup_table(sc, sqlContext, args.input)

    result_1 = query_1(sc, sqlContext)
    result_2 = query_2(sc, sqlContext)
    result_3 = query_3(sc, sqlContext)

    print("-" * 15 + " OUTPUT " + "-" * 15)
    print("Query 1: {}".format(result_1))
    print("Query 2: {}".format(result_2))
    print("Query 3: {}".format(result_3))
    print("-" * 30)
