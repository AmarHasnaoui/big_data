from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, when, count, sum, broadcast
import sys

# Configuration MySQL
jdbc_driver_path = "/projet_bdf3/lib/mysql-connector-j-8.3.0.jar"
MYSQL_URL = "jdbc:mysql://host.docker.internal:3306/gold"
MYSQL_PROPERTIES = {
    "user": "root",
    "password": "amar",
    "driver": "com.mysql.cj.jdbc.Driver"
}

spark = SparkSession.builder \
    .appName("Gold Zone Processing") \
    .enableHiveSupport() \
    .config("spark.driver.extraClassPath", jdbc_driver_path) \
    .config("spark.jars", jdbc_driver_path) \
    .config("hive.metastore.uris", "thrift://hive-metastore:9083") \
    .getOrCreate()


class HiveReader:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def read_table(self, table_name: str) -> DataFrame:
        return self.spark.table(table_name)


class Transformer:
    def join_transactions_labels(self, df_trans: DataFrame, df_labels: DataFrame) -> DataFrame:
        return df_trans.join(df_labels, on="id", how="left")

    def enrich_transactions(self, df: DataFrame) -> DataFrame:
        return df.withColumn("is_fraud", when(col("label") == "1", 1).otherwise(0))


class Datamarts:
    def dm_fraude_detection(self, df: DataFrame) -> DataFrame:
        return df.groupBy("merchant_city", "is_fraud") \
                 .agg(count("*").alias("nb_transactions"))

    def dm_clients(self, df_users: DataFrame, df_cards: DataFrame) -> DataFrame:
        return df_users.join(broadcast(df_cards), df_users.id == df_cards.client_id, "inner") \
                       .select(df_users.id.alias("user_id"), "gender", "credit_limit_usd", "num_credit_cards")

    def dm_transactions_par_region(self, df: DataFrame) -> DataFrame:
        return df.groupBy("merchant_state") \
                 .agg(sum("amount_usd").alias("total_usd"))

    def dm_cartes(self, df: DataFrame) -> DataFrame:
        return df.groupBy("card_brand", "card_type") \
                 .agg(count("*").alias("nb_cartes"))

    def dm_mcc_categories(self, df_trans: DataFrame, df_mcc: DataFrame) -> DataFrame:
        return df_trans.join(broadcast(df_mcc), df_trans.mcc == df_mcc.mcc_code, "left") \
                       .groupBy("description") \
                       .agg(count("*").alias("nb_trans"))


class MySQLWriter:
    def write(self, df: DataFrame, table_name: str):
        df.write \
          .mode("overwrite") \
          .jdbc(url=MYSQL_URL, table=table_name, properties=MYSQL_PROPERTIES)


def main():
    print("Lecture des données depuis Hive...")
    reader = HiveReader(spark)

    df_trans = reader.read_table("silver.transactions") \
                     .select("id", "client_id", "amount_usd", "merchant_city", "merchant_state", "mcc", "card_id", "year", "month", "day")

    df_labels = reader.read_table("silver.train_fraud_labels") \
                      .select("id", "label")

    df_users = reader.read_table("silver.users")
    df_cards = reader.read_table("silver.cards")
    df_mcc = reader.read_table("silver.mcc_codes")

    print("Jointure et enrichissement des transactions...")
    transformer = Transformer()
    df_joined = transformer.join_transactions_labels(df_trans, df_labels)
    df_enriched = transformer.enrich_transactions(df_joined)

    print("Création des datamarts...")
    datamarts = Datamarts()
    writer = MySQLWriter()

    print("Datamart fraude...")
    dm_fraud = datamarts.dm_fraude_detection(df_enriched)
    writer.write(dm_fraud.repartition(4), "dm_fraude_detection")

    print("Datamart clients...")
    dm_clients = datamarts.dm_clients(df_users, df_cards)
    writer.write(dm_clients.repartition(4), "dm_clients")

    print("Datamart transactions par région...")
    dm_region = datamarts.dm_transactions_par_region(df_enriched)
    writer.write(dm_region.repartition(4), "dm_transactions_par_region")

    print("Datamart cartes...")
    dm_cartes = datamarts.dm_cartes(df_cards)
    writer.write(dm_cartes.repartition(4), "dm_cartes")

    print("Datamart catégories MCC...")
    dm_mcc = datamarts.dm_mcc_categories(df_enriched, df_mcc)
    writer.write(dm_mcc.repartition(4), "dm_mcc_categories")

    print("Traitement terminé avec succès.")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("Erreur : {}".format(e), file=sys.stderr)
    finally:
        spark.stop()
