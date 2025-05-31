# coding: utf-8
import time
import gc
import json
import logging
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, year, month, dayofmonth

jdbc_driver_path = "/projet_bdf3/lib/mysql-connector-j-8.3.0.jar"
class SparkSessionManager(object):
    @staticmethod
    def create_session(app_name="bdf3"):    
        spark = (
            SparkSession.builder
            .appName(app_name)
            .master("local[*]")
            .config("spark.driver.memory", "6g")
            .config("spark.executor.memory", "6g")
            .config("spark.driver.extraClassPath", jdbc_driver_path)
            .config("spark.jars", jdbc_driver_path)
            .config("spark.rpc.message.maxSize", "512")
            .config("spark.network.timeout", "3600")
            .config("spark.executor.heartbeatInterval", "3600")
            .getOrCreate()
        )
        spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
        return spark


class LoggerManager(object):
    @staticmethod
    def create_logger():
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s",
            handlers=[
                logging.FileHandler("/projet_bdf3/logs/pipeline_feeder.log"),
                logging.StreamHandler()
            ]
        )
        return logging.getLogger(__name__)


class BaseProcessor(object):
    def __init__(self, spark, logger):
        self.spark = spark
        self.logger = logger
        self.today = datetime.today()
        self.year = self.today.strftime("%Y")
        self.month = self.today.strftime("%m")
        self.day = self.today.strftime("%d")

    def get_partition_path(self, name):
        return "/projet_bdf3/Bronze/{}/year={}/month={}/day={}/".format(
            name, self.year, self.month, self.day
        )


class MySQLProcessor(BaseProcessor):
    def __init__(self, spark, logger, url, properties):
        BaseProcessor.__init__(self, spark, logger)
        self.url = url
        self.properties = properties

    def process_transactions(self, start=6000000, end=25000000, size=3000000):
        self.logger.info("Traitement base de donnees - transactions")
        for i in range(start, end, size):
            try:
                self.logger.info("Lecture des IDs entre {} et {}".format(i, i + size - 1))
                query = "(SELECT * FROM transactions WHERE id >= {} AND id < {}) AS t".format(i, i + size)
                df = self.spark.read.jdbc(url=self.url, table=query, properties=self.properties)

                if df.rdd.isEmpty():
                    self.logger.info("Aucune donnee trouvee pour le batch {} {}".format(i, i + size - 1))
                    continue

                df = df.withColumn("situation_date", to_date(col("situation_date"), "yyyy-MM-dd")) \
                       .withColumn("year", year(col("situation_date"))) \
                       .withColumn("month", month(col("situation_date"))) \
                       .withColumn("day", dayofmonth(col("situation_date")))

                output_path = "/projet_bdf3/Bronze/transactions/"
                df.write.partitionBy("year", "month", "day").mode("append").parquet(output_path)

                self.logger.info("Batch {} {} sauvegarde".format(i, i + size - 1))
                df.unpersist()
                del df
                gc.collect()
            except Exception as e:
                self.logger.error("Erreur lors du traitement de la table transactions", exc_info=True)


class CSVProcessor(BaseProcessor):
    def process_csv(self, name, path):
        self.logger.info("Traitement CSV - {} demarre".format(name))
        df = self.spark.read.option("header", True).option("sep", ",").csv(path)
        self.logger.info("{} - {} lignes lues".format(name, df.count()))
        df.write.mode("overwrite").parquet(self.get_partition_path(name))
        self.logger.info("{} - Sauvegarde en Parquet a {}".format(name, self.get_partition_path(name)))
        df.unpersist()
        del df


class JSONProcessor(BaseProcessor):
    def process_json(self, name, path):
        self.logger.info("Traitement JSON - {} demarre".format(name))
        if name == "mcc_codes":
            with open(path, "r") as f:
                data = json.load(f)
            rows = [(k, v) for k, v in data.items()]
            df = self.spark.createDataFrame(rows, ["mcc_code", "description"])
        elif name == "train_fraud_labels":
            df = self.spark.read.json(path)
        else:
            self.logger.warning("Nom de fichier JSON non reconnu : {}".format(name))
            return

        self.logger.info("{} - {} lignes lues".format(name, df.count()))
        df.write.mode("overwrite").parquet(self.get_partition_path(name))
        self.logger.info("{} - Sauvegarde en Parquet a {}".format(name, self.get_partition_path(name)))
        df.unpersist()
        del df


def main():
    start_time = time.time()
    logger = LoggerManager.create_logger()
    spark = SparkSessionManager.create_session()

    url = "jdbc:mysql://host.docker.internal:3306/Source?useSSL=false&serverTimezone=UTC&allowPublicKeyRetrieval=true"
    properties = {"user": "root", "password": "amar", "driver": "com.mysql.cj.jdbc.Driver"}

    logger.info("Date d'extraction vers Bronze : {}".format(datetime.today().strftime('%Y-%m-%d')))

    mysql_processor = MySQLProcessor(spark, logger, url, properties)
    csv_processor = CSVProcessor(spark, logger)
    json_processor = JSONProcessor(spark, logger)

    mysql_processor.process_transactions()
    csv_processor.process_csv("cards", "/projet_bdf3/Source/cards_data.csv")
    csv_processor.process_csv("users", "/projet_bdf3/Source/users_data.csv")
    json_processor.process_json("mcc_codes", "/projet_bdf3/Source/mcc_codes.json")
    json_processor.process_json("train_fraud_labels", "/projet_bdf3/Source/train_fraud_labels.json")

    total_duration = time.time() - start_time
    logger.info("Traitement de donnees - Bronze - termine en {} secondes".format(total_duration))

    spark.stop()


if __name__ == "__main__":
    main()
