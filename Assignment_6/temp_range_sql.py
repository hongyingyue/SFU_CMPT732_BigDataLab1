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
    raw_df.createOrReplaceTempView('temp_view')
    spark.sql('SELECT date, station, observation, value FROM temp_view WHERE qflag IS NULL').createOrReplaceTempView('temps')
    spark.sql('SELECT date, station, value AS TMAX FROM temps WHERE observation="TMAX"').createOrReplaceTempView('tmaxs')
    spark.sql('SELECT date, station, value AS TMIN FROM temps WHERE observation="TMIN"').createOrReplaceTempView('tmins')
    spark.sql('SELECT date, station, value AS TMIN FROM temps WHERE observation="TMIN"').createOrReplaceTempView('tmins')

    range_sql = '''
        SELECT t1.date,t1.station,(TMAX-TMIN)/10 AS range
        FROM tmaxs t1
        JOIN tmins t2
        ON t1.date=t2.date AND t1.station=t2.station
        '''
    spark.sql(range_sql).createOrReplaceTempView('rangs')

    max_range_sql = '''
        SELECT date, MAX(range) as rmax
        FROM rangs
        GROUP BY date
        '''
    spark.sql(max_range_sql).createOrReplaceTempView('max_rangs')

    result_sql = '''
        SELECT t1.date,t1.station,t1.range
        FROM rangs t1
        JOIN max_rangs t2 
        ON t1.date=t2.date AND t1.range=t2.rmax
        ORDER BY date,station
        '''
    result = spark.sql(result_sql)
    result.write.csv(output, mode='overwrite', header=True)


if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    spark = SparkSession.builder.appName('temp range sql').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs, output)
