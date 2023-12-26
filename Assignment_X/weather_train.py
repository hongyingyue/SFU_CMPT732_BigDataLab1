import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, types
spark = SparkSession.builder.appName('weather train').getOrCreate()
spark.sparkContext.setLogLevel('WARN')
assert spark.version >= '3.0' # make sure we have Spark 3.0+

from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, SQLTransformer
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator


def main(inputs, model_file):
    tmax_schema = types.StructType([
                types.StructField('station', types.StringType()),
                types.StructField('date', types.DateType()),
                types.StructField('latitude', types.FloatType()),
                types.StructField('longitude', types.FloatType()),
                types.StructField('elevation', types.FloatType()),
                types.StructField('tmax', types.FloatType()),
            ])
    data = spark.read.csv(inputs, schema=tmax_schema)
    train, validation = data.randomSplit([0.75, 0.25])
    train = train.cache()
    validation = validation.cache()
    
    sql_query = '''SELECT today.latitude, today.longitude, today.elevation, today.tmax, 
                        DAYOFYEAR(today.date) AS day_of_year, yesterday.tmax AS yesterday_tmax 
                   FROM __THIS__ as today 
                   INNER JOIN __THIS__ as yesterday 
                   ON date_sub(today.date, 1) = yesterday.date 
                   AND today.station = yesterday.station'''
    sqlTransformer = SQLTransformer(statement=sql_query)
    featureAssembler = VectorAssembler(inputCols=['latitude', 'longitude', 'elevation','day_of_year', 'yesterday_tmax'],
                                       outputCol="features")
    gbt_regressor = GBTRegressor(featuresCol="features", labelCol="tmax", maxIter=10)
    pipeline = Pipeline(stages=[sqlTransformer, featureAssembler, gbt_regressor])

    model = pipeline.fit(train)
    
    predictions = model.transform(validation)
    # predictions.select("prediction", "tmax", "features").show(5) # Select example rows to display

    evaluator_r2 = RegressionEvaluator(labelCol="tmax", predictionCol="prediction", metricName="r2")
    evaluator_rmse = RegressionEvaluator(labelCol="tmax", predictionCol="prediction", metricName="rmse")
    score_r2 = evaluator_r2.evaluate(predictions)
    score_rmse = evaluator_rmse.evaluate(predictions)

    print('Validation score for DecisionTree model: (R square)%g, (RMSE)%g' % (score_r2, score_rmse))

    model.write().overwrite().save(model_file) # Save the model


if __name__ == '__main__':
    inputs = sys.argv[1]
    model_file = sys.argv[2]
    main(inputs, model_file)
