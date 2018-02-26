from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
import argparse

def setup_table(sc, sqlContext, users_filename, businesses_filename, reviews_filename):
    '''
    Args:
        sc: The Spark Context
        sqlContext: The Spark SQL context
        users_filename: Filename of the Yelp users file to use, where each line represents a user
        businesses_filename: Filename of the Yelp checkins file to use, where each line represents a business
        reviews_filename: Filename of the Yelp reviews file to use, where each line represents a review
    Parse the users/checkins/businesses files and register them as tables in Spark SQL in this function
    '''
    users = sqlContext.read.json(users_filename)
    businesses = sqlContext.read.json(businesses_filename)
    reviews = sqlContext.read.json(reviews_filename)

    users.createOrReplaceTempView("users")
    businesses.createOrReplaceTempView("businesses")
    reviews.createOrReplaceTempView("reviews")

    users.printSchema()
    businesses.printSchema()
    reviews.printSchema()

def query_1(sc, sqlContext):
    '''
    Args:
        sc: The Spark Context
        sqlContext: The Spark SQL context
    Returns:
        An int: the maximum number of "funny" ratings left on a review created by someone who started "yelping" in 2012
    '''
    # From users
    # Inner join reviews on users.user_id = reviews.user_id 
    ret = sqlContext.sql(
            "SELECT users.user_id, reviews.funny, users.yelping_since "
            "FROM users "
            "INNER JOIN reviews ON users.user_id = reviews.user_id "
            "WHERE users.yelping_since LIKE '2012%' "
            "ORDER BY reviews.funny DESC LIMIT 1"
        ).collect()

    return ret[0]['funny']

def query_2(sc, sqlContext):
    '''
    Args:
        sc: The Spark Context
        sqlContext: The Spark SQL context
    Returns:
        A list of strings: the user ids of anyone who has left a 1-star review, has created more than 250 reviews,
            and has left a review at a business in Champaign, IL
    '''
    part1 = sqlContext.sql(
            "SELECT 1star.user_id, businesses.city, businesses.state "
            "FROM "
                "(SELECT DISTINCT user_id, business_id "
                "FROM reviews "
                "WHERE stars = '1') 1star "
            "INNER JOIN businesses "
                "ON 1star.business_id = businesses.business_id "
                "WHERE businesses.city = 'Champaign' AND businesses.state = 'IL'"
        )
    part1.createOrReplaceTempView("part1")
    ret = sqlContext.sql(
            "SELECT part1.user_id "
            "FROM part1 "
            "INNER JOIN users "
            "ON users.user_id = part1.user_id "
            "WHERE users.review_count > '250'"
        ).collect()

    l = []
    for values in ret:
        l.append(values[0])

    return l

if __name__ == '__main__':
    # Get input/output files from user
    parser = argparse.ArgumentParser()
    parser.add_argument('users', help='File to load Yelp user data from')
    parser.add_argument('businesses', help='File to load Yelp business data from')
    parser.add_argument('reviews', help='File to load Yelp review data from')
    args = parser.parse_args()

    # Setup Spark
    conf = SparkConf().setAppName("jaunting_with_joins")
    sc = SparkContext(conf=conf)
    sc.setLogLevel("ERROR") 
    sqlContext = SQLContext(sc)

    setup_table(sc, sqlContext, args.users, args.businesses, args.reviews)

    result_1 = query_1(sc, sqlContext)
    result_2 = query_2(sc, sqlContext)

    print("-" * 15 + " OUTPUT " + "-" * 15)
    print("Query 1: {}".format(result_1))
    print("Query 2: {}".format(result_2))
    print("-" * 30)
