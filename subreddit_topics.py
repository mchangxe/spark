from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
import argparse
import sys
import json
import re
stopwords_str = 'i,me,my,myself,we,our,ours,ourselves,you,your,yours,yourself,yourselves,he,him,his,himself,she,her,hers,herself,it,its,itself,they,them,their,theirs,themselves,what,which,who,whom,this,that,these,those,am,is,are,was,were,be,been,being,have,has,had,having,do,does,did,doing,a,an,the,and,but,if,or,because,as,until,while,of,at,by,for,with,about,against,between,into,through,during,before,after,above,below,to,from,up,down,in,out,on,off,over,under,again,further,then,once,here,there,when,where,why,how,all,any,both,each,few,more,most,other,some,such,no,nor,not,only,own,same,so,than,too,very,s,t,can,will,just,don,should,now,d,ll,m,o,re,ve,y,ain,aren,couldn,didn,doesn,hadn,hasn,haven,isn,ma,mightn,mustn,needn,shan,shouldn,wasn,weren,won,wouldn,dont,cant'
stopwords = set(stopwords_str.split(','))
WORD_REGEX = re.compile(r"[\w']+")

def get_redit(reddit):
    try: 
        data = json.loads(reddit)
        subreddit = data['text']
        #print(subreddit)
        wordsAndMore = WORD_REGEX.findall(subreddit)
        res = []
        for word in wordsAndMore:
            if word.lower() not in stopwords:
                res.append((data['subreddit'], word.lower()))
        return res
    except:
        return []

def changeKeys(x):
    return (x[0][0], (x[0][1], x[1]))

def operateOnValues(x):
    values = x[1] #should be an array of tuples with each word and its count 
    values.sort(key=lambda tup: tup[1], reverse=True)
    if len(values) > 10:
        del values[10:]
    finalTuple = ()
    for vals in values:
        finalTuple += (vals[0],)
    return (x[0], finalTuple)    

def find_subreddit_topics(sc, input_dstream):
    '''
    Args:
        sc: the SparkContext
        input_dstream: The discretized stream (DStream) of input
    Returns:
        A DStream containing the list of common subreddit words
    '''
    window_length = 900
    slide_interval = 10

    # YOUR CODE HERE
    subAndWords = input_dstream.flatMap(get_redit).map(lambda x: (x,1)).reduceByKeyAndWindow(
                        lambda x, y: x + y, 
                        None, 
                        window_length, 
                        slide_interval).map(changeKeys).groupByKey().mapValues(list).map(operateOnValues)

    return subAndWords

if __name__ == '__main__':
    # Get input/output files from user
    parser = argparse.ArgumentParser()

    parser.add_argument('--input_stream', help='Stream URL to pull data from')
    parser.add_argument('--input_stream_port', help='Stream port to pull data from')

    parser.add_argument('output', help='Directory to save DStream results to')
    args = parser.parse_args()

    # Setup Spark
    conf = SparkConf().setAppName("subreddit_topics")
    sc = SparkContext(conf=conf)
    sc.setLogLevel("ERROR")    
    ssc = StreamingContext(sc, 10)
    ssc.checkpoint('streaming_checkpoints')

    if args.input_stream and args.input_stream_port:
        dstream = ssc.socketTextStream(args.input_stream,
                                       int(args.input_stream_port))
        results = find_subreddit_topics(sc, dstream)
    else:
        print("Need input_stream and input_stream_port")
        sys.exit(1)

    results.saveAsTextFiles(args.output)
    results.pprint(20)

    ssc.start()
    ssc.awaitTermination()
