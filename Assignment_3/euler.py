from pyspark import SparkConf, SparkContext
import sys, random

def euler(data):
    iterations = 0
    random.seed()
    for _ in data:
        sum = 0.0
        while sum < 1:
            sum += random.random()
            iterations += 1
    return iterations


def main(input):
    samples = int(input)
    batches = 80
    rdd = sc.range(samples, numSlices=batches).glom()
    sample_rdd = rdd.map(euler)
    total_iterations = sample_rdd.reduce(lambda x, y: x + y)
    print(total_iterations / samples)


if __name__ == '__main__':
    conf = SparkConf().setAppName('eular')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
    assert sc.version >= '3.0'  # make sure we have Spark 3.0+
    input = sys.argv[1]
    main(input)
