from pyspark import SparkConf, SparkContext
import sys, json

def json_py(line):
    py_line = json.loads(line)
    subreddit = py_line["subreddit"]
    score = int(py_line["score"])
    yield (subreddit, (1,score))

def add_pairs(x, y):
    return x[0] + y[0], x[1] + y[1]

def get_average(kv):
    pair = kv[1]
    count_sum = pair[0]
    score_sum = pair[1]
    return (kv[0], score_sum/count_sum)

def output_format(kv):
    return json.dumps(kv)


def main(inputs, output):
    raws = sc.textFile(inputs)
    records = raws.flatMap(json_py)
    results = records.reduceByKey(add_pairs).map(get_average)
    outdata = results.map(output_format)
    outdata.saveAsTextFile(output)
    print(outdata.take(10))

if __name__ == '__main__':
    conf = SparkConf().setAppName('reddit averages')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
    assert sc.version >= '3.0'  # make sure we have Spark 3.0+
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)
