from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
import argparse
import sys
import json
import difflib
import string

def get_reddit(reddit):
    try: 
        data = json.loads(reddit)
        return (data['author'], data['text'])
    except:
        return (None, None)

def check_bot(entry):
    allComments = entry[1]
    if len(allComments) > 1:
        acc = 0.0
        numberOfRepeated = 0
        for x in allComments:
            for y in allComments:
                if y is not x: 
                    ibot = difflib.SequenceMatcher(lambda x: x in " .,", x, y)
                    ratio = ibot.quick_ratio()
                    acc += ratio
                    numberOfRepeated += 1
        if numberOfRepeated == 0:
            return None
        else:
            if (acc/numberOfRepeated) >= 0.6:
                return (entry[0], acc/numberOfRepeated)
            else:
                return None
    else:
        return None
    #if acc >= 2.0:
    #    return (entry[0], acc)
    #else:
    #    pass

def detect_reddit_bots(sc, input_dstream):
    '''
    Args:
        sc: the SparkContext
        input_dstream: The discretized stream (DStream) of input
    Returns:
        A DStream containing the list of all detected bot usernames
    '''

    # YOUR CODE HERE

    window_length = 180
    slide_interval = 30

    authComments = input_dstream.map(get_reddit).groupByKeyAndWindow(
                    window_length,
                    slide_interval).mapValues(list).map(check_bot).filter(lambda x: x is not None).transform(lambda rdd: rdd.sortBy(lambda x: x[1], ascending=False))
    
    return authComments

if __name__ == '__main__':
    # Get input/output files from user
    parser = argparse.ArgumentParser()

    parser.add_argument('--input_stream', help='Stream URL to pull data from')
    parser.add_argument('--input_stream_port', help='Stream port to pull data from')

    parser.add_argument('output', help='Directory to save DStream results to')
    args = parser.parse_args()

    # Setup Spark
    conf = SparkConf().setAppName("reddit_bot_detection")
    sc = SparkContext(conf=conf)
    sc.setLogLevel("ERROR")   
    ssc = StreamingContext(sc, 10)
    ssc.checkpoint('streaming_checkpoints')

    if args.input_stream and args.input_stream_port:
        dstream = ssc.socketTextStream(args.input_stream,
                                       int(args.input_stream_port))
        results = detect_reddit_bots(sc, dstream)
    else:
        print("Need input_stream and input_stream_port")
        sys.exit(1)

    results.saveAsTextFiles(args.output)
    results.pprint()

    ssc.start()
    ssc.awaitTermination()
