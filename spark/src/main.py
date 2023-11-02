import logging
import os
import numpy as np
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import when
from pyspark.ml.feature import StringIndexer
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml import Pipeline
from pyspark.ml.regression import LinearRegression

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger("MinioSparkJob")
spark = SparkSession.builder.getOrCreate()


def load_config(spark_context: SparkContext):
    spark_context._jsc.hadoopConfiguration().set("fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID", "admin"))
    spark_context._jsc.hadoopConfiguration().set("fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY", "password"))
    spark_context._jsc.hadoopConfiguration().set("fs.s3a.endpoint", os.getenv("ENDPOINT", "172.31.3.234:9000"))
    spark_context._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "false")
    spark_context._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
    spark_context._jsc.hadoopConfiguration().set("fs.s3a.attempts.maximum", "1")
    spark_context._jsc.hadoopConfiguration().set("fs.s3a.connection.establish.timeout", "5000")
    spark_context._jsc.hadoopConfiguration().set("fs.s3a.connection.timeout", "10000")


load_config(spark.sparkContext)

# Read CSV file from MinIO
df = spark.read.option("header", "true").option("inferSchema", "true").csv(os.getenv("INPUT_PATH", "s3a://csv/*.csv"))

# Spark Code

rnd_seed = 42
np.random.seed = rnd_seed
np.random.set_state = rnd_seed

# preprocessing
df = df.fillna(0)
df = df.withColumn("CarrierDelay", when(df["CarrierDelay"] == "NA", 0).otherwise(df["CarrierDelay"]))
df = df.withColumn("WeatherDelay", when(df["WeatherDelay"] == "NA", 0).otherwise(df["WeatherDelay"]))
df = df.withColumn("NASDelay", when(df["NASDelay"] == "NA", 0).otherwise(df["NASDelay"]))
df = df.withColumn("SecurityDelay", when(df["SecurityDelay"] == "NA", 0).otherwise(df["SecurityDelay"]))
df = df.withColumn("LateAircraftDelay", when(df["LateAircraftDelay"] == "NA", 0).otherwise(df["LateAircraftDelay"]))
df = df.withColumn("CancellationCode", when(df["CancellationCode"] == "None", 0 ).otherwise(df["CancellationCode"]))
df = df.withColumn("ArrDelay", when(df["ArrDelay"] == "NA", 0).otherwise(df["ArrDelay"]))
df = df.withColumn("Distance", when(df["Distance"] == "NA", 0).otherwise(df["Distance"]))
df = df.withColumn("ArrDelay", df["ArrDelay"].cast("int"))
df = df.withColumn("Distance", df["Distance"].cast("int"))

# string indexer
featureCols = ["Month", "DayofMonth", "DayOfWeek", "CRSDepTime", "CRSArrTime", "Distance", "OriginIndex", "DestIndex", "CarrierIndex"]
origin_indexer = StringIndexer(inputCol="Origin", outputCol="OriginIndex")
dest_indexer = StringIndexer(inputCol="Dest", outputCol="DestIndex")
carrier_indexer = StringIndexer(inputCol="UniqueCarrier", outputCol="CarrierIndex")
assembler = VectorAssembler(inputCols=featureCols, outputCol="features")
scaler = StandardScaler(inputCol='features', outputCol='features_scaled')
pipeline = Pipeline(stages=[origin_indexer, dest_indexer, carrier_indexer, assembler, scaler])
pipeline = pipeline.fit(df)
preprocessed_df = pipeline.transform(df)

# ml
train_data, test_data = preprocessed_df.randomSplit([0.8, 0.2], seed=rnd_seed)
lr = LinearRegression(labelCol="ArrDelay", predictionCol='predArrDelay')
linearModel = lr.fit(train_data)
predictions = linearModel.transform(test_data)
pred_labels = predictions.select('predArrDelay', 'ArrDelay')

# Write Data to MinIO
linearModel.save(os.getenv("MODEL_OUTPUT_PATH", "s3a://spark-models/model"))
pipeline.write().overwrite().save(os.getenv("PIPELINE_OUTPUT_PATH", "s3a://spark-models/pipeline"))

# logging
logger.info(f"Total Rows:{df.count()}")
logger.info(f"RMSE:{linearModel.summary.rootMeanSquaredError}")
logger.info(f"MAE:{linearModel.summary.meanAbsoluteError}")
