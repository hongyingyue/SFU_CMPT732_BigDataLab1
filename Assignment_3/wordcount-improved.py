from pyspark import SparkConf, SparkContext
import sys, re, string

def words_once(line):
    wordsep = re.compile(r'[%s\s]+' % re.escape(string.punctuation))
    for w in wordsep.split(line):
        yield (w.lower(), 1)

def add(x, y):
    return x + y

def get_key(kv):
    return kv[0]

def output_format(kv):
    k, v = kv
    return '%s %i' % (k, v)


def main(inputs, output):
    text = sc.textFile(inputs).repartition(16)
    words_wEmptyString = text.flatMap(words_once)
    words = words_wEmptyString.filter(lambda x: x[0] != "")
    wordcount = words.reduceByKey(add)
    outdata = wordcount.sortBy(get_key).map(output_format)
    outdata.saveAsTextFile(output)


if __name__ == '__main__':
    conf = SparkConf().setAppName('word count')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
    assert sc.version >= '3.0'  # make sure we have Spark 3.0+
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)
