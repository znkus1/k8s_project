from pyspark.sql import SparkSession
from pyspark.ml.regression import LinearRegressionModel
from pyspark.ml import PipelineModel


spark = SparkSession.builder \
    .master("local") \
    .appName("Airline EDA") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .getOrCreate()

# 저장된 모델 불러오기
loaded_lrModel = LinearRegressionModel.load("spark_model/model")
pipeline_model = PipelineModel.load("spark_model/pipeline")
