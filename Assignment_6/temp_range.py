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
    raw_df = spark.read.csv(inputs, schema=observation_schema)
    temps = raw_df.filter(raw_df.qflag.isNull()).select('date', 'station', 'observation', 'value').cache()
    tmaxs = temps.filter(functions.col('observation') == 'TMAX').withColumn('TMAX', functions.col('value')).drop('observation', 'value')
    tmins = temps.filter(functions.col('observation') == 'TMIN').withColumn('TMIN', functions.col('value')).drop('observation', 'value')
    rangs = tmaxs.join(tmins, ['date', 'station']).withColumn('range', (functions.col('TMAX')-functions.col('TMIN'))/10).cache()
    max_rangs = rangs.groupBy('date').max('range').withColumnsRenamed({'max(range)': 'rmax'})
    result = rangs.join(max_rangs.hint("broadcast"),'date').filter(functions.col('range') == functions.col('rmax')).select('date', 'station', 'range').sort('date','station')
    result.write.csv(output, mode='overwrite', header=True)


if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    spark = SparkSession.builder.appName('temp range df').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs, output)
