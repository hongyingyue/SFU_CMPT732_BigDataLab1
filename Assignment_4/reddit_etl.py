from pyspark import SparkConf, SparkContext
import sys, json

def json_py(line):
    py_line = json.loads(line)
    subreddit = py_line["subreddit"]
    score = int(py_line["score"])
    author = py_line["author"]
    yield {'subreddit': subreddit, 'score': score, 'author': author}


def main(inputs, output):
    raws = sc.textFile(inputs)
    records = raws.flatMap(json_py).filter(lambda comment: 'e' in comment['subreddit']).cache()
    positive_records = records.filter(lambda comment: comment['score'] > 0)
    negative_records = records.filter(lambda comment: comment['score'] <= 0)
    positive_records.map(json.dumps).saveAsTextFile(output + '/positive')
    negative_records.map(json.dumps).saveAsTextFile(output + '/negative')


if __name__ == '__main__':
    conf = SparkConf().setAppName('reddit elt')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
    assert sc.version >= '3.0'  # make sure we have Spark 3.0+
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)
