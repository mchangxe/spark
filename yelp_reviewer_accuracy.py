from pyspark import SparkContext, SparkConf
import argparse
import json


def find_user_review_accuracy(sc, reviews_filename):
    '''
    Args:
        sc: The Spark Context
        reviews_filename: Filename of the Yelp reviews JSON file to use, where each line represents a review
    Returns:
        An RDD of tuples in the following format:
            (USER_ID, AVERAGE_REVIEW_OFFSET)
            - USER_ID: The ID of the user being referenced
            - AVERAGE_REVIEW_OFFSET: The average difference between a user's review and the average restaraunt rating
    '''
    jsonData = sc.textFile(reviews_filename)
    dataset = jsonData.map(json.loads)
    dataset.persist()

    businessAverages = (dataset
        .filter(lambda e: "business_id" in e and "user_id" in e and "stars" in e)
        .map(lambda e: (e["business_id"], e["stars"]))
        )

    countByKey = sc.broadcast(businessAverages.countByKey())

    businessAverages = (businessAverages
        .reduceByKey(lambda x,y: x+y)
        .map(lambda x: (x[0], round(float(x[1])/float(countByKey.value[x[0]]), 2)))
        .collect()
        )

    
    businessMp = {}

    for values in businessAverages:
        businessMp[values[0]] = values[1]

    reviewerRatings = (dataset
        .filter(lambda e: "business_id" in e and "user_id" in e and "stars" in e)
        .map(lambda e: ((e["business_id"], e["user_id"]), e["stars"]))
        .map(lambda e: (e[0][1], round(e[1]-businessMp[e[0][0]], 2)))
        )

    countByKey = sc.broadcast(reviewerRatings.countByKey())
    reviewerRatings = (reviewerRatings
        .reduceByKey(lambda x,y: x+y)
        .map(lambda x: (x[0], round(float(x[1])/countByKey.value[x[0]], 2)))
        )

    return reviewerRatings

if __name__ == '__main__':
    # Get input/output files from user
    parser = argparse.ArgumentParser()
    parser.add_argument('input', help='File to load Yelp review data from')
    parser.add_argument('output', help='File to save RDD to')
    args = parser.parse_args()

    # Setup Spark
    conf = SparkConf().setAppName("yelp_reviewer_accuracy")
    sc = SparkContext(conf=conf)

    results = find_user_review_accuracy(sc, args.input)
    results.saveAsTextFile(args.output)

