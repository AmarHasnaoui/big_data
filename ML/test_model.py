from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from pyspark.ml import PipelineModel
 
# Demarrage Spark
spark = SparkSession.builder \
    .appName("FraudPrediction") \
    .config("spark.executor.memory", "2g") \
    .config("spark.driver.memory", "2g") \
    .config("spark.sql.catalogImplementation", "hive") \
    .master("local[*]") \
    .enableHiveSupport() \
    .getOrCreate()
 
model_path = "/projet_bdf3/ML/saved_model_balanced"
model = PipelineModel.load(model_path)

schema = StructType([
    StructField("amount_usd", DoubleType(), True),
    StructField("credit_limit_usd", DoubleType(), True),
    StructField("merchant_state", StringType(), True),
    StructField("card_brand", StringType(), True),
    StructField("card_type", StringType(), True),
    StructField("has_chip", StringType(), True),  
    StructField("description", StringType(), True)
])
 

data_test = spark.createDataFrame([
    (123.45, 5000.0, "NY", "VISA", "DEBIT", "1", "Restaurants"),
    (9850.0, 12000.0, "CA", "MASTERCARD", "CREDIT", "0", "Electronics")
], schema=schema)
 

predictions = model.transform(data_test)
 

predictions.select("amount_usd", "merchant_state", "prediction", "probability").show(truncate=False)
 
spark.stop()