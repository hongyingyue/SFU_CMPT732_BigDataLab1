from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, functions, types
import sys, re, math
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+


def split_logs(line):
    line_re = re.compile(r'^(\S+) - - \[(\S+) [+-]\d+\] \"[A-Z]+ (\S+) HTTP/\d\.\d\" \d+ (\d+)$')
    match = line_re.match(line)
    if match:
        parts = match.groups()
        host_name, datetime, path, request_bytes = parts
        return (host_name, request_bytes)
    else:
        return (None, None)


def main(inputs):
    raw = sc.textFile(inputs)
    logs = raw.map(split_logs)
    df = logs.toDF(['host_name', 'request_bytes']).withColumn('request_bytes', functions.col('request_bytes').cast(types.IntegerType()))
    grouped_df = df.filter(df.request_bytes.isNotNull()).groupBy('host_name').\
                     agg(functions.count('host_name').alias('count_requests'), \
                         functions.sum('request_bytes').alias('sum_request_bytes')).cache()

    sum_one = grouped_df.count() # n
    sum_x = grouped_df.select(functions.sum('count_requests')).collect()[0][0]  # sum of x
    sum_y = grouped_df.select(functions.sum('sum_request_bytes')).collect()[0][0]  # sum of y
    sum_x_squ = grouped_df.select(functions.sum(functions.pow('count_requests', 2))).collect()[0][0]  # sum of x squ
    sum_y_squ = grouped_df.select(functions.sum(functions.pow('sum_request_bytes', 2))).collect()[0][0]  # sum of y squ
    sum_x_y = grouped_df.select(functions.sum(functions.col('count_requests') * functions.col('sum_request_bytes'))).collect()[0][0]  # sum of x * y
    r = (sum_one * sum_x_y - sum_x * sum_y) / \
        (math.sqrt(sum_one * sum_x_squ - sum_x * sum_x) * math.sqrt(sum_one * sum_y_squ - sum_y * sum_y))
    print('r = ', r)
    print('r^2 = ', r*r)
    # fun_cor = grouped_df.select(functions.corr(functions.col('count_requests'), functions.col('sum_request_bytes'))).collect()[0][0]  # sum of x * y
    # print('corr r = ', fun_cor)


if __name__ == '__main__':
    conf = SparkConf().setAppName('correlate logs')
    sc = SparkContext(conf=conf)
    assert sc.version >= '3.0'  # make sure we have Spark 3.0+
    sc.setLogLevel('WARN')

    spark = SparkSession.builder.appName('correlate logs').getOrCreate()
    assert spark.version >= '3.0'  # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')

    inputs = sys.argv[1]
    main(inputs)
