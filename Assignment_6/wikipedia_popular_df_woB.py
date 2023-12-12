import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types

@functions.udf(returnType=types.StringType())
def path_to_hour(path):
    return path.split(sep='/')[-1][11:22]


def main(inputs, output):
    df_schema = types.StructType([
        types.StructField('language', types.StringType()),
        types.StructField('title', types.StringType()),
        types.StructField('views', types.LongType()),
        types.StructField('bytes', types.LongType()),
    ])
    df = spark.read.csv(inputs, schema=df_schema, sep=' ').withColumn('filename', functions.input_file_name())
    conditions = (df['language'] == 'en') & (df['title'] != 'Main_Page') & (~df['title'].startswith('Special:'))
    df = df.withColumn('hour', path_to_hour(functions.col('filename'))).select('hour', 'title', 'views').filter(conditions).cache()
    most_visted = df.groupBy('hour').max('views')
    res_df = df.join(most_visted, 'hour').filter(functions.col('views')==functions.col('max(views)')).select('hour', 'title', 'views').sort('hour', 'title')
    res_df.write.json(output, compression='gzip', mode='overwrite')
    res_df.explain()


if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    spark = SparkSession.builder.appName('wikipedia popular df').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs, output)
