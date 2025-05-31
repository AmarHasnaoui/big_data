import sys
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.classification import GBTClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml import Pipeline
 
# Configuration du logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("FraudDetection")
 
def main():
    try:
        logger.info("Demarrage de la session Spark...")
        spark = SparkSession.builder \
            .appName("FraudDetectionBalanced") \
            .config("spark.executor.memory", "4g") \
            .config("spark.driver.memory", "4g") \
            .config("spark.sql.catalogImplementation", "hive") \
            .master("local[*]") \
            .config("hive.metastore.uris", "thrift://hive-metastore:9083") \
            .enableHiveSupport() \
            .getOrCreate()
 
        spark.conf.set("spark.sql.shuffle.partitions", "12")
        logger.info("Session Spark demarree.")
 
        logger.info("Chargement des tables reduites depuis Hive...")
        transactions = spark.sql("SELECT id, card_id, client_id, amount_usd, merchant_state, mcc FROM silver.transactions")
       
        labels = spark.sql("""
            SELECT
                id,
                CASE
                    WHEN label = 'Yes' THEN 1
                    ELSE 0
                END AS fraud_label
            FROM silver.train_fraud_labels
        """)
       
        cards = spark.sql("SELECT id as card_id, card_brand, card_type, has_chip, credit_limit_usd FROM silver.cards")
        mcc = spark.sql("SELECT mcc_code, description FROM silver.mcc_codes")
        logger.info("Tables chargees.")
 
        logger.info("Filtrage des transactions presentes dans les labels...")
        label_ids = labels.select("id").distinct()
        transactions = transactions.join(label_ids, "id", "inner")
 
        logger.info("Jointure des tables...")
        data = transactions.join(labels, "id", "left") \
                           .join(cards, "card_id", "left") \
                           .join(mcc, transactions.mcc == mcc.mcc_code, "left")
        logger.info("Jointures terminees.")
 
        data = data.withColumn("has_chip", col("has_chip").cast("int"))
        data = data.withColumn("fraud_label", col("fraud_label").cast("int"))
 
        logger.info("Echantillonnage equilibre...")
        fraudes = data.filter(col("fraud_label") == 1)
        non_fraudes = data.filter(col("fraud_label") == 0).sample(False, 0.1, seed=42)
        data = fraudes.union(non_fraudes)
 
        logger.info("echantillon final - fraudes: {}, non_fraudes: {}, total: {}".format(fraudes.count(), non_fraudes.count(), data.count()))
 
        logger.info("Construction des indexeurs pour les colonnes categorielles...")
        categorical_cols = ["merchant_state", "card_brand", "card_type", "has_chip", "description"]
        indexers = [StringIndexer(inputCol=c, outputCol=c + "_idx", handleInvalid="keep") for c in categorical_cols]
 
        feature_cols = ["amount_usd", "credit_limit_usd"] + [c + "_idx" for c in categorical_cols]
        assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
        classifier = GBTClassifier(labelCol="fraud_label", featuresCol="features", maxIter=30, maxBins=200)
 
        pipeline = Pipeline(stages=indexers + [assembler, classifier])
        logger.info("Pipeline construite.")
 
        logger.info("Split des donnees train/test...")
        train, test = data.randomSplit([0.8, 0.2], seed=42)
        logger.info("Donnees splitees.")
 
        if train.count() == 0:
            raise ValueError("Le dataset d'entrainement est vide. Verifiez l'echantillonnage ou le filtrage.")
 
        logger.info("Entrainement du modele...")
        model = pipeline.fit(train)
        logger.info("Modele entraine.")
 
        logger.info("evaluation sur le jeu de test...")
        predictions = model.transform(test)
        evaluator = BinaryClassificationEvaluator(labelCol="fraud_label")
        auc = evaluator.evaluate(predictions)
        logger.info("AUC: {}".format(auc))
 
        logger.info("Enregistrement du modele dans '/projet_bdf3/ML/saved_model_balanced'...")
        model.write().overwrite().save("ML/saved_model_balanced")
 
        logger.info("Modele enregistre avec succes.")
 
    except Exception as e:
        logger.error("Erreur rencontree: {}".format(e), exc_info=True)
        raise
    finally:
        if 'spark' in locals():
            spark.stop()
            logger.info("Session Spark arretee.")
 
if __name__ == "__main__":
    main()