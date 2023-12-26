import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, types
spark = SparkSession.builder.appName('tmax model tester').getOrCreate()
assert spark.version >= '3.0' # make sure we have Spark 3.0+
spark.sparkContext.setLogLevel('WARN')

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


def test_model(model_file, inputs):
    # get the data
    test_tmax = spark.read.csv(inputs, schema=tmax_schema)

    # load the model
    model = PipelineModel.load(model_file)
    
    # use the model to make predictions
    predictions = model.transform(test_tmax)
    predictions.select("prediction", "tmax", "yesterday_tmax", "features").show(5) # Select example rows to display
    
    # evaluate the predictions
    r2_evaluator = RegressionEvaluator(predictionCol='prediction', labelCol='tmax',
            metricName='r2')
    r2 = r2_evaluator.evaluate(predictions)
    
    rmse_evaluator = RegressionEvaluator(predictionCol='prediction', labelCol='tmax',
            metricName='rmse')
    rmse = rmse_evaluator.evaluate(predictions)

    print('r2 =', r2)
    print('rmse =', rmse)

    # If you used a regressor that gives .featureImportances, maybe have a look...
    print(model.stages[-1].featureImportances)



if __name__ == '__main__':
    model_file = sys.argv[1]
    inputs = sys.argv[2]
    test_model(model_file, inputs)
