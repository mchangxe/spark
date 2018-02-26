from pyspark import SparkContext, SparkConf
import argparse
import json


def find_engaging_reviews(sc, reviews_filename):
    '''
    Args:
        sc: The Spark Context
        reviews_filename: Filename of the Yelp reviews JSON file to use, where each line represents a review
    Returns:
        An RDD of tuples in the following format:
            (BUSINESS_ID, REVIEW_ID)
            - BUSINESS_ID: The business being referenced
            - REVIEW_ID: The ID of the review with the largest sum of "useful", "funny", and "cool" responses
                for the given business
    '''

    jsonData = sc.textFile(reviews_filename)
    dataset = jsonData.map(json.loads)
    dataset.persist()

    first = (dataset
        .filter(lambda e: "review_id" in e and "business_id" in e and "useful" in e and "funny" in e and "cool" in e)
        .map(lambda e: ( e["business_id"], (e["review_id"], e["useful"]+e["funny"]+e["cool"]) ) )
        .reduceByKey(lambda x, y: x if x[1] > y[1] else ( y if y[1] > x[1] else (x if x[0] > y[0] else y) ) )
        .collect()
        )

    final = []

    for values in first:
        final.append((values[0], values[1][0]))

    return sc.parallelize(final)

if __name__ == '__main__':
    # Get input/output files from user
    parser = argparse.ArgumentParser()
    parser.add_argument('input', help='File to load Yelp review data from')
    parser.add_argument('output', help='File to save RDD to')
    args = parser.parse_args()

    # Setup Spark
    conf = SparkConf().setAppName("engaging_reviews")
    sc = SparkContext(conf=conf)

    results = find_engaging_reviews(sc, args.input)
    results.saveAsTextFile(args.output)

