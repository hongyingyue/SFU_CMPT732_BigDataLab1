import sys, re, uuid, datetime
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
from pyspark import SparkConf, SparkContext

def split_logs(line):
    ''' Can be optimized:
    re.compile is being called inside the map function, 
    which slows down the execution as it is being called for every row in the rdd
    '''
    line_re = re.compile(r'^(\S+) - - \[(\S+) [+-]\d+\] \"[A-Z]+ (\S+) HTTP/\d\.\d\" \d+ (\d+)$')
    match = line_re.match(line)
    if match:
        parts = match.groups()
        host_name, dtime, path, request_bytes = parts
        rbytes = int(request_bytes)
        dtime = datetime.datetime.strptime(dtime, "%d/%b/%Y:%H:%M:%S")
        return (host_name, str(uuid.uuid4()), rbytes, dtime, path)


def main(input_dir, keyspace, table):
    raw = sc.textFile(input_dir)
    logs = raw.map(split_logs).filter(lambda x: x is not None)

    schema = types.StructType([
        types.StructField('host', types.StringType()),
        types.StructField('id', types.StringType()),
        types.StructField('bytes', types.IntegerType()),
        types.StructField('datetime', types.TimestampType()),
        types.StructField('path', types.StringType()),
    ])
    df = logs.toDF(schema=schema).repartition(16)
    df.write.format("org.apache.spark.sql.cassandra") \
        .mode("append").options(table=table, keyspace=keyspace).save()
    print('---------------- All work is Done!')


if __name__ == '__main__':
    conf = SparkConf().setAppName('load logs')
    sc = SparkContext(conf=conf)
    assert sc.version >= '3.0'  # make sure we have Spark 3.0+
    sc.setLogLevel('WARN')

    cluster_seeds = ['node1.local', 'node2.local']
    spark = SparkSession.builder.appName('load logs Cassandra Spark') \
        .config('spark.cassandra.connection.host', ','.join(cluster_seeds)).getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')

    input_dir = sys.argv[1]
    keyspace = sys.argv[2]
    table = sys.argv[3]
    main(input_dir, keyspace, table)
