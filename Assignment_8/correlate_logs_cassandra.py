import sys, math
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
from pyspark.sql import SparkSession, functions


def main(keyspace, table):
    df = spark.read.format("org.apache.spark.sql.cassandra") \
        .options(table=table, keyspace=keyspace).load()
    grouped_df = df.filter(df.bytes.isNotNull()).groupBy('host'). \
        agg(functions.count('host').alias('count_requests'), \
            functions.sum('bytes').alias('sum_request_bytes')).cache()

    sum_one = grouped_df.count()  # n
    sum_x = grouped_df.select(functions.sum('count_requests')).collect()[0][0]  # sum of x
    sum_y = grouped_df.select(functions.sum('sum_request_bytes')).collect()[0][0]  # sum of y
    sum_x_squ = grouped_df.select(functions.sum(functions.pow('count_requests', 2))).collect()[0][0]  # sum of x squ
    sum_y_squ = grouped_df.select(functions.sum(functions.pow('sum_request_bytes', 2))).collect()[0][0]  # sum of y squ
    sum_x_y = grouped_df.select(functions.sum(functions.col('count_requests') * functions.col('sum_request_bytes'))).collect()[0][
        0]  # sum of x * y
    r = (sum_one * sum_x_y - sum_x * sum_y) / \
        (math.sqrt(sum_one * sum_x_squ - sum_x * sum_x) * math.sqrt(sum_one * sum_y_squ - sum_y * sum_y))
    print('r = ', r)
    print('r^2 = ', r * r)


if __name__ == '__main__':
    cluster_seeds = ['node1.local', 'node2.local']
    spark = SparkSession.builder.appName('correlate logs Cassandra Spark') \
        .config('spark.cassandra.connection.host', ','.join(cluster_seeds)).getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    keyspace = sys.argv[1]
    table = sys.argv[2]
    main(keyspace, table)
