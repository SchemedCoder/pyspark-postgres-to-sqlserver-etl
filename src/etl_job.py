import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit
from src.logger import get_logger

logger = get_logger(__name__)

class BurgerCourierETL:
    def __init__(self, config):
        self.config = config
        self.spark = self._init_spark()

    def _init_spark(self):
        logger.info("Starting the engines... (#TheHungryCargoPlane)")
        return SparkSession.builder \
            .appName(self.config['spark']['app_name']) \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.executor.memory", self.config['spark']['executor_memory']) \
            .getOrCreate()

    def extract(self):
        logger.info("Extracting data from PostgreSQL...")
        src_cfg = self.config['source']
        url = f"jdbc:postgresql://{src_cfg['host']}:{src_cfg['port']}/{src_cfg['db']}"
        
        # Parallel reading: Sending 50 vans to fetch data by id
        df = self.spark.read.format("jdbc") \
            .option("url", url) \
            .option("dbtable", src_cfg['table']) \
            .option("user", src_cfg['user']) \
            .option("password", os.getenv("PG_PASSWORD")) \
            .option("driver", src_cfg['driver']) \
            .option("partitionColumn", src_cfg['partition_column']) \
            .option("lowerBound", str(src_cfg['lower_bound'])) \
            .option("upperBound", str(src_cfg['upper_bound'])) \
            .option("numPartitions", str(src_cfg['num_partitions'])) \
            .load()
            
        logger.info(f"Loaded data with {df.rdd.getNumPartitions()} partitions.")
        return df

    def transform(self, df):
        logger.info("Adding audit columns mid-flight... (#TheFlyingChefs)")
        return df.withColumn("migrated_at", current_timestamp()) \
                 .withColumn("source_system", lit("PostgreSQL"))

    def load(self, df):
        logger.info("Landing at the SQL Server Vault...")
        tgt_cfg = self.config['target']
        url = f"jdbc:sqlserver://{tgt_cfg['host']}:{tgt_cfg['port']};databaseName={tgt_cfg['db']};encrypt=true;trustServerCertificate=true"
        
        # Writing in batches to prevent transaction log blocking
        df.write.format("jdbc") \
            .mode("overwrite") \
            .option("url", url) \
            .option("dbtable", tgt_cfg['table']) \
            .option("user", tgt_cfg['user']) \
            .option("password", os.getenv("SQL_PASSWORD")) \
            .option("driver", tgt_cfg['driver']) \
            .option("batchsize", str(tgt_cfg['batch_size'])) \
            .save()
        logger.info("Successfully locked the data in the vault!")

    def run(self):
        try:
            df = self.extract()
            transformed_df = self.transform(df)
            self.load(transformed_df)
        except Exception as e:
            logger.error(f"Plane crashed! Reason: {str(e)}")
            raise
        finally:
            logger.info("Shutting down the engines.")
            self.spark.stop()
