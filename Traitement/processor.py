import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, to_date, when, regexp_replace
from pyspark.sql.types import IntegerType, DoubleType, TimestampType
from datetime import datetime
import time
import os
import re
import gc

logging.basicConfig(
    level=logging.INFO,  
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("/projet_bdf3/logs/pipeline_processor.log"),  
        logging.StreamHandler()  
    ]
)
logger = logging.getLogger(__name__)

class PipelineProcessor(object):

    def __init__(self):
        self.spark = None

    def get_latest_local_path(self, base_path):
        date_paths = []

        for year_dir in os.listdir(base_path):
            year_path = os.path.join(base_path, year_dir)
            if not os.path.isdir(year_path) or not year_dir.startswith("year="):
                continue
            for month_dir in os.listdir(year_path):
                month_path = os.path.join(year_path, month_dir)
                if not os.path.isdir(month_path) or not month_dir.startswith("month="):
                    continue
                for day_dir in os.listdir(month_path):
                    day_path = os.path.join(month_path, day_dir)
                    if not os.path.isdir(day_path) or not day_dir.startswith("day="):
                        continue
                    try:
                        y = int(year_dir.split("=")[1])
                        m = int(month_dir.split("=")[1])
                        d = int(day_dir.split("=")[1])
                        date_paths.append(((y, m, d), day_path))
                    except:
                        continue

        if not date_paths:
            return None

        latest_path = sorted(date_paths, reverse=True)[0][1]
        return latest_path

    def extract_date_parts_from_path(self, path):
        match = re.search(r"year=(\d{4})/month=(\d{1,2})/day=(\d{1,2})", path)
        if match:
            year, month, day = map(int, match.groups())
            return year, month, day
        else:
            return None, None, None

    def init_spark(self):
        logger.info("Initialisation SparkSession avec support Hive")
        self.spark = SparkSession.builder \
            .appName("Traitement transactions base") \
            .config("spark.sql.catalogImplementation", "hive") \
            .config("hive.metastore.uris", "thrift://hive-metastore:9083") \
            .enableHiveSupport() \
            .config("spark.driver.memory", "4g") \
            .config("spark.executor.memory", "4g") \
            .getOrCreate()
        self.spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

    def process_transactions(self):
        start_time_tache = time.time()
        logger.info("Lecture des donnees depuis Bronze (transactions)")
        path = self.get_latest_local_path("/projet_bdf3/Bronze/transactions/")
        if path:
            year_process, month_process, day_process = self.extract_date_parts_from_path(path)
            logger.info("Lecture du chemin local le plus recent : {}".format(path))
            df = self.spark.read.parquet(path)
            df.cache()
        else:
            logger.error("Aucun dossier valide trouve dans : transactions" )
            return

        logger.info("Normalisation des types de colonnes")
        df = df.withColumn("id", col("id").cast(IntegerType())) \
               .withColumn("date", col("date").cast(TimestampType())) \
               .withColumn("client_id", col("client_id").cast(IntegerType())) \
               .withColumn("card_id", col("card_id").cast(IntegerType())) \
               .withColumn("situation_date", to_date(col("situation_date"), "yyyy-MM-dd")) \
               .withColumn("merchant_id", col("merchant_id").cast(IntegerType())) \
               .withColumn("zip", col("zip").cast(DoubleType())) \
               .withColumn("mcc", col("mcc").cast(IntegerType()))

        logger.info("Nettoyage et renommage des colonnes")
        df = df.withColumn("amount", regexp_replace("amount", "[$,]", "")) \
               .withColumn("amount", col("amount").cast(DoubleType())) \
               .withColumnRenamed("amount", "amount_usd")
        df = df.withColumn("year", F.lit(year_process))
        df = df.withColumn("month", F.lit(month_process))
        df = df.withColumn("day", F.lit(day_process))
        df = df.drop('errors')
        df = df.dropDuplicates()

        logger.info("ecriture des donnees dans Hive (table silver.transactions)")
        df.write.mode("overwrite").partitionBy("year", "month", "day").saveAsTable("silver.transactions")

        logger.info("ecriture des donnees normalisees sur Silver (fichiers Parquet)")
        df.write.mode("overwrite").partitionBy("year", "month", "day") \
            .parquet("/projet_bdf3/Silver/transactions/")

        df.unpersist()
        del df
        gc.collect()

        total_duration = time.time() - start_time_tache
        logger.info("Traitement transaction termine avec succes en {} secondes".format(total_duration))

    def process_cards(self):
        start_time_tache = time.time()
        logger.info("Lecture des donnees depuis Bronze (cards)")
        path = self.get_latest_local_path("/projet_bdf3/Bronze/cards/")
        if path:
            year_process, month_process, day_process = self.extract_date_parts_from_path(path)
            logger.info("Lecture du chemin local le plus recent : {}".format(path))
            df = self.spark.read.parquet(path)
            df.cache()
        else:
            logger.error("Aucun dossier valide trouve dans : cards" )
            return

        logger.info("Normalisation des types de colonnes")
        df = df.dropDuplicates()
        df = df.withColumn("id", col("id").cast(IntegerType())) \
               .withColumn("client_id", col("client_id").cast(IntegerType())) \
               .withColumn("card_number", col("card_number").cast(IntegerType())) \
               .withColumn("cvv", col("cvv").cast(IntegerType())) \
               .withColumn("num_cards_issued", col("num_cards_issued").cast(IntegerType())) \
               .withColumn("year_pin_last_changed", col("year_pin_last_changed").cast(IntegerType()))

        logger.info("Nettoyage et renommage des colonnes")
        df = df.withColumn(
                    "has_chip",
                    when(col("has_chip") == "YES", True)  
                    .when(col("has_chip") == "NO", False)  
                    .otherwise(None)
                ) \
               .withColumn(
                    "card_on_dark_web",
                    when(col("card_on_dark_web") == "Yes", True)  
                    .when(col("card_on_dark_web") == "No", False)  
                    .otherwise(None)
                ) \
               .withColumn("credit_limit", regexp_replace("credit_limit", "[$,]", "")) \
               .withColumn("credit_limit", col("credit_limit").cast(DoubleType())) \
               .withColumnRenamed("credit_limit", "credit_limit_usd")

        df = df.withColumn("year", F.lit(year_process))
        df = df.withColumn("month", F.lit(month_process))
        df = df.withColumn("day", F.lit(day_process))

        logger.info("ecriture des donnees dans Hive (table silver.cards)")
        df.write.mode("overwrite").partitionBy("year", "month", "day").saveAsTable("silver.cards")

        logger.info("ecriture des donnees normalisees sur Silver (fichiers Parquet)")
        df.write.mode("overwrite").partitionBy("year", "month", "day").parquet("/projet_bdf3/Silver/cards/")

        total_duration = time.time() - start_time_tache
        logger.info("Traitement cards termine avec succes en {} secondes".format(total_duration))

        df.unpersist()
        del df
        gc.collect()

    def process_users(self):
        start_time_tache = time.time()
        logger.info("Lecture des donnees depuis Bronze (users)")
        path = self.get_latest_local_path("/projet_bdf3/Bronze/users/")
        if path:
            year_process, month_process, day_process = self.extract_date_parts_from_path(path)
            logger.info("Lecture du chemin local le plus recent : {}".format(path))
            df = self.spark.read.parquet(path)
            df.cache()
        else:
            logger.error("Aucun dossier valide trouve dans : users" )
            return

        logger.info("Normalisation des types de colonnes")
        df = df.dropDuplicates()
        df = df.withColumn("id", col("id").cast(IntegerType())) \
               .withColumn("current_age", col("current_age").cast(IntegerType())) \
               .withColumn("retirement_age", col("retirement_age").cast(IntegerType())) \
               .withColumn("birth_year", col("birth_year").cast(IntegerType())) \
               .withColumn("latitude", col("latitude").cast(DoubleType())) \
               .withColumn("longitude", col("longitude").cast(DoubleType()))

        logger.info("Nettoyage et renommage des colonnes")
        df = df.withColumn(
                    "gender",
                    when(col("gender") == "Male", "M")  
                    .when(col("gender") == "Female", "F") 
                    .otherwise(None)
                ) \
               .withColumn("per_capita_income", regexp_replace("per_capita_income", "[$,]", "")) \
               .withColumn("per_capita_income", col("per_capita_income").cast(DoubleType())) \
               .withColumnRenamed("per_capita_income", "per_capita_income_usd")

        df = df.withColumn("year", F.lit(year_process))
        df = df.withColumn("month", F.lit(month_process))
        df = df.withColumn("day", F.lit(day_process))

        logger.info("ecriture des donnees dans Hive (table silver.users)")
        df.write.mode("overwrite").partitionBy("year", "month", "day").saveAsTable("silver.users")

        logger.info("ecriture des donnees normalisees sur Silver (fichiers Parquet)")
        df.write.mode("overwrite").partitionBy("year", "month", "day").parquet("/projet_bdf3/Silver/users/")

        total_duration = time.time() - start_time_tache
        logger.info("Traitement users termine avec succes en {} secondes".format(total_duration))

        df.unpersist()
        del df
        gc.collect()

    def process_mcc_codes(self):
        start_time_tache = time.time()
        logger.info("Lecture des donnees depuis Bronze (mcc_codes)")
        path = self.get_latest_local_path("/projet_bdf3/Bronze/mcc_codes/")
        if path:
            year_process, month_process, day_process = self.extract_date_parts_from_path(path)
            logger.info("Lecture du chemin local le plus recent : {}".format(path))
            df = self.spark.read.parquet(path)
            df.cache()
        else:
            logger.error("Aucun dossier valide trouve dans : mcc_codes" )
            return

        logger.info("Normalisation des types de colonnes")
        df = df.dropDuplicates()
        df = df.withColumn("mcc_code", col("mcc_code").cast(IntegerType()))

        df = df.withColumn("year", F.lit(year_process))
        df = df.withColumn("month", F.lit(month_process))
        df = df.withColumn("day", F.lit(day_process))

        logger.info("ecriture des donnees dans Hive (table silver.mcc_codes)")
        df.write.mode("overwrite").partitionBy("year", "month", "day").saveAsTable("silver.mcc_codes")
        logger.info("ecriture des donnees normalisees sur Silver (fichiers Parquet)")
        df.write.mode("overwrite").partitionBy("year", "month", "day").parquet("/projet_bdf3/Silver/mcc_codes/")

        total_duration = time.time() - start_time_tache
        logger.info("Traitement mcc_codes termine avec succes en {} secondes".format(total_duration))

        df.unpersist()
        del df
        gc.collect()
        
    def process_train_fraud_labels(self):
        start_time_tache = time.time()
        logger.info("Lecture des donnees depuis Bronze (train_fraud_labels)")
        path = self.get_latest_local_path("/projet_bdf3/Bronze/train_fraud_labels/")
        if path:
            year_process, month_process, day_process = self.extract_date_parts_from_path(path)
            logger.info("Lecture du chemin local le plus recent : {}".format(path))
            df = self.spark.read.parquet(path)
            df.cache()
        else:
            logger.error("Aucun dossier valide trouve dans : train_fraud_labels" )
            return

        logger.info("Normalisation des types de colonnes")
        df = df.dropDuplicates()
        df = df.withColumn("id", col("id").cast(IntegerType()))

        df = df.withColumn("year", F.lit(year_process))
        df = df.withColumn("month", F.lit(month_process))
        df = df.withColumn("day", F.lit(day_process))

        logger.info("ecriture des donnees dans Hive (table silver.train_fraud_labels)")
        df.write.mode("overwrite").partitionBy("year", "month", "day").saveAsTable("silver.train_fraud_labels")
        logger.info("ecriture des donnees normalisees sur Silver (fichiers Parquet)")
        df.write.mode("overwrite").partitionBy("year", "month", "day").parquet("/projet_bdf3/Silver/train_fraud_labels/")

        total_duration = time.time() - start_time_tache
        logger.info("Traitement train_fraud_labels termine avec succes en {} secondes".format(total_duration))

        df.unpersist()
        del df
        gc.collect()
        
    
if __name__ == "__main__":
    processor = PipelineProcessor()
    processor.init_spark()
    processor.process_transactions()
    processor.process_cards()
    processor.process_users()
    processor.process_mcc_codes()
    processor.process_train_fraud_labels()

        
