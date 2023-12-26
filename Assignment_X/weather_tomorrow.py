import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, types, Row
spark = SparkSession.builder.appName('weather tomorrow').getOrCreate()
assert spark.version >= '3.0' # make sure we have Spark 3.0+
spark.sparkContext.setLogLevel('WARN')
from datetime import date

from pyspark.ml import PipelineModel
from pyspark.ml.evaluation import RegressionEvaluator


tmax_schema = types.StructType([
    types.StructField('station', types.StringType()),
    types.StructField('date', types.DateType()),
    types.StructField('latitude', types.FloatType()),
    types.StructField('longitude', types.FloatType()),
    types.StructField('elevation', types.FloatType()),
    types.StructField('tmax', types.FloatType()),
])


def test_model(model_file):
    # get the data    
    dueday_tmax = spark.createDataFrame([Row('SFU001', date(2023, 11, 17), 49.2771, -122.9146, 330.0, 12.0),
                                         Row('SFU001', date(2023, 11, 18), 49.2771, -122.9146, 330.0, 0.0)], 
                                        schema=tmax_schema)

    # load the model
    model = PipelineModel.load(model_file)
    
    # use the model to make predictions
    predictions = model.transform(dueday_tmax)
    prediction = predictions.select('prediction').collect()[0][0]
    print('Predicted tmax tomorrow:', prediction)
    # predictions.select('tmax','prediction').show()


if __name__ == '__main__':
    model_file = sys.argv[1]
    test_model(model_file)
