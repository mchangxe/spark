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
        An int: the number of reviews written by the person with the maximum number of reviews written
    '''
    ret = sqlContext.sql("SELECT COUNT(*) FROM content GROUP BY UserId ORDER BY COUNT(*) DESC LIMIT 1").collect()
    return ret[0][0]

def query_2(sc, sqlContext):
    '''
    Args:
        sc: The Spark Context
        sqlContext: The Spark SQL context
    Returns:
        A list of strings: The product ids of the products with the top 25 highest average
            review scores of the products with more than 25 reviews,
            ordered by average product score, with ties broken by the number of reviews
    '''
    ret = sqlContext.sql(
        "SELECT sub.id "
        "FROM ("
            "SELECT ProductId as id, AVG(Score) as a, COUNT(*) as b "
            "FROM content "
            "GROUP BY ProductId "
            "HAVING b > 25 "
            "ORDER BY a DESC, b DESC LIMIT 25"
            ") sub"
        ).collect()

    a = []
    for values in ret:
        a.append(values['id'])
    return a

def query_3(sc, sqlContext):
    '''
    Args:
        sc: The Spark Context
        sqlContext: The Spark SQL context
    Returns:
        A list of integers: A `Ids` of the reviews with the top 25 highest ratios between `HelpfulnessNumerator`
            and `HelpfulnessDenominator`, which have `HelpfulnessDenominator` greater than 10,
            ordered by that ratio, with ties broken by `HelpfulnessDenominator`.
    '''
    ret = sqlContext.sql(
        "SELECT Id, (HelpfulnessNumerator/HelpfulnessDenominator) as ratio, HelpfulnessDenominator "
        "FROM content "
        "HAVING HelpfulnessDenominator > 10 "
        "ORDER BY ratio DESC, HelpfulnessDenominator DESC LIMIT 25"
        ).collect()

    a = []
    for values in ret:
        a.append(values['Id'])
    return a;

if __name__ == '__main__':
    # Get input/output files from user
    parser = argparse.ArgumentParser()
    parser.add_argument('input', help='File to load Amazon review data from')
    args = parser.parse_args()

    # Setup Spark
    conf = SparkConf().setAppName("aggregation_aggrevation")
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
