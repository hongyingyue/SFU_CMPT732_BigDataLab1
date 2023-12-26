import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
from pyspark.sql import SparkSession, functions


def main(topic):
    messages = spark.readStream.format('kafka') \
        .option('kafka.bootstrap.servers', 'node1.local:9092,node2.local:9092') \
        .option('subscribe', topic).load()   
    values = messages.select(messages['value'].cast('string'))

    split_val = functions.split(values.value, ' ')
    df_xy = values.withColumn('x', split_val[0]).withColumn('y', split_val[1]).drop('value')
    df_xy_v = df_xy.withColumn('xy', df_xy['x']*df_xy['y']).withColumn('x2', df_xy['x']**2).\
                withColumn('1',functions.lit(1))
    df_agg = df_xy_v.agg(functions.sum('1').alias('n'), \
                         functions.sum('x').alias('sum_x'), functions.sum('x2').alias('sum_x2'),\
                         functions.sum('y').alias('sum_y'), functions.sum('xy').alias('sum_xy'))
    
    beta = (df_agg['sum_xy'] - df_agg['sum_x']*df_agg['sum_y']/df_agg['n'])/(df_agg['sum_x2'] - df_agg['sum_x']**2/df_agg['n'])
    alpha = df_agg['sum_y']/df_agg['n'] - beta * df_agg['sum_x']/df_agg['n']
    final_df = df_agg.withColumn('beta', beta).withColumn('alpha', alpha).select('beta', 'alpha')

    stream = final_df.writeStream.format('console').outputMode('update').start()
    # stream = final_df.writeStream.format('console').outputMode('complete').start()
    stream.awaitTermination(600)
    # stream.awaitTermination(60)



if __name__ == '__main__':
    spark = SparkSession.builder.appName('read stream').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    topic = sys.argv[1]
    main(topic)
