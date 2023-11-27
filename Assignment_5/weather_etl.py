import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types


def main(inputs, output):
    observation_schema = types.StructType([
        types.StructField('station', types.StringType()),
        types.StructField('date', types.StringType()),
        types.StructField('observation', types.StringType()),
        types.StructField('value', types.IntegerType()),
        types.StructField('mflag', types.StringType()),
        types.StructField('qflag', types.StringType()),
        types.StructField('sflag', types.StringType()),
        types.StructField('obstime', types.StringType()),
    ])
    weather = spark.read.csv(inputs, schema=observation_schema)
    nulqflag = weather.filter(weather.qflag.isNull())
    ca_nulqflag = nulqflag.filter(nulqflag.station.startswith('CA'))
    max_ca_nulqflag = ca_nulqflag.filter(ca_nulqflag.observation == 'TMAX')
    max_ca_nulqflag_tmax = max_ca_nulqflag.withColumn('tmax',max_ca_nulqflag.value/10)
    etl_data = max_ca_nulqflag_tmax.select('station', 'date', 'tmax')
    etl_data.write.json(output, compression='gzip', mode='overwrite')


if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    spark = SparkSession.builder.appName('weather ETL').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs, output)
