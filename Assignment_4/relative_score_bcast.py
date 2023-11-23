from pyspark import SparkConf, SparkContext
import sys, json

def pre_average(line):
    subreddit = line["subreddit"]
    score = int(line["score"])
    return (subreddit, (1,score))

def add_pairs(x, y):
    return x[0] + y[0], x[1] + y[1]

def get_average(kv):
    pair = kv[1]
    count_sum = pair[0]
    score_sum = pair[1]
    return (kv[0], score_sum/count_sum)

def get_relative(comment, bcast):
    score = comment['score']
    author = comment['author']
    average = bcast.value.get(comment['subreddit'], None)
    if average is not None:
        relative_score = score / average
        return (relative_score, author)
    else:
        return (None, author)


def main(inputs, output):
    raws = sc.textFile(inputs).map(json.loads).cache()
    average_scores = raws.map(pre_average).reduceByKey(add_pairs).map(get_average).filter(lambda x: x[1] > 0)
    bcast_data = sc.broadcast(dict(average_scores.collect()))
    results = raws.map(lambda comment: get_relative(comment, bcast_data)).filter(lambda x: x[0] is not None).sortBy(lambda x: x[0], ascending=False)
    results.map(json.dumps).saveAsTextFile(output)


if __name__ == '__main__':
    conf = SparkConf().setAppName('relative scores bcast')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
    assert sc.version >= '3.0'  # make sure we have Spark 3.0+
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)
