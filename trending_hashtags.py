from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
import argparse
import sys
import json

def get_tweet(tweet_json):
    try:
        data = json.loads(tweet_json)
        return [tag for tag in data['text'].split() if tag.startswith("#")]
    except:
        return []

def takeTenSorted(rdd):
    topTen = rdd.sortBy(lambda x: x[1], ascending=False).take(10)
    return rdd.filter(lambda x: x in topTen)

def find_trending_hashtags(sc, input_dstream):
    '''
    Args:
        sc: the SparkContext
        input_dstream: The discretized stream (DStream) of input
    Returns:
        A DStream containing the top 10 trending hashtags, and their usage count
    '''

    # YOUR CODE HERE

    # Stream window parameters
    window_length = 60
    slide_interval = 10
    
    hashtag_counts = input_dstream.flatMap(get_tweet).map(lambda x: (x,1))

    windowed_hashtag_count = hashtag_counts.reduceByKeyAndWindow(
        lambda x, y: x+y,
        lambda x, y: x-y,
        window_length, 
        slide_interval
        )
    
    usorted = windowed_hashtag_count.transform(takeTenSorted)
    usorted = usorted.transform(lambda rdd: rdd.sortBy(lambda x: x[1], ascending=False))    

    return usorted

if __name__ == '__main__':
    # Get input/output files from user
    parser = argparse.ArgumentParser()

    parser.add_argument('--input_stream', help='Stream URL to pull data from')
    parser.add_argument('--input_stream_port', help='Stream port to pull data from')

    parser.add_argument('output', help='Directory to save DStream results to')
    args = parser.parse_args()

    # Setup Spark
    conf = SparkConf().setAppName("trending_hashtags")
    sc = SparkContext(conf=conf)
    sc.setLogLevel("ERROR")
    ssc = StreamingContext(sc, 10)
    ssc.checkpoint('streaming_checkpoints')

    if args.input_stream and args.input_stream_port:
        dstream = ssc.socketTextStream(args.input_stream,
                                       int(args.input_stream_port))
        results = find_trending_hashtags(sc, dstream)
    else:
        print("Need input_stream and input_stream_port")
        sys.exit(1)

    results.saveAsTextFiles(args.output)
    results.pprint()

    ssc.start()
    ssc.awaitTermination()
