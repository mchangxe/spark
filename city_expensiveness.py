from pyspark import SparkContext, SparkConf
import argparse
import json
import operator


def find_city_expensiveness(sc, business_filename):
    '''
    Args:
        sc: The Spark Context
        business_filename: Filename of the Yelp businesses JSON file to use, where each line represents a business
    Returns:
        An RDD of tuples in the following format:
            (CITY_STATE, AVERAGE_PRICE)
            - CITY_STATE is in the format "CITY, STATE". i.e. "Urbana, IL"
            - AVERAGE_PRICE should be a float rounded to 2 decimal places
    '''
    jsonData = sc.textFile(business_filename)
    dataset = jsonData.map(json.loads)
    dataset.persist()
    
    csr = (dataset
        .filter(lambda e:"city" in e and "state" in e and "attributes" in e)
        .map(lambda e: ((e["city"], e["state"]),e["attributes"]))
        .collect()
        )

    finalcsr = []

    for elements in csr: 
        key = elements[0]
        attrArray = elements[1] 
        if attrArray is not None:
            # traverse if not none
            for attr in attrArray:
                if "RestaurantsPriceRange2: " in attr:
                    both = attr.split()
                    value = both[1]
                    finalcsr.append((key, float(value)))


    actualcsr = sc.parallelize(finalcsr)
    countsByKey = sc.broadcast(actualcsr.countByKey())
    actualcsr = (actualcsr
        .reduceByKey(lambda x,y: x+y)
        .map(lambda x: ( x[0][0]+", "+x[0][1], round(float(x[1])/float(countsByKey.value[x[0]]), 2) ))
        )

    return actualcsr

    


if __name__ == '__main__':
    # Get input/output files from user
    parser = argparse.ArgumentParser()
    parser.add_argument('input', help='File to load Yelp business data from')
    parser.add_argument('output', help='File to save RDD to')
    args = parser.parse_args()

    # Setup Spark
    conf = SparkConf().setAppName("city_expensiveness")
    sc = SparkContext(conf=conf)

    results = find_city_expensiveness(sc, args.input)
    results.saveAsTextFile(args.output)

