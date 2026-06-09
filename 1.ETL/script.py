from pathlib import Path
import sys

sys.path.append(str(Path(__file__).resolve().parent.parent))

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    TimestampType,
    DateType,
    LongType
)
from pyspark.sql.window import Window
from datetime import datetime
import boto3
from botocore.exceptions import ClientError
from utils.utils import logs


logger = logs(__name__)

class ETLClientes:

    def __init__(self, app_name="ETL_Clientes"):
        self.spark = (
            SparkSession.builder
            .appName(app_name)
            .enableHiveSupport()
            .getOrCreate()
        )

        self.glue = boto3.client("glue")
        self.schema = self._get_cliente_schema()

    def _get_cliente_schema(self):
        return StructType([
            StructField("cod_cliente", LongType(), False),
            StructField("nm_cliente", StringType(), False),
            StructField("nm_pais_cliente", StringType(), True),
            StructField("nm_cidade_cliente", StringType(), True),
            StructField("nm_rua_cliente", StringType(), True),
            StructField("num_casa_cliente", StringType(), True),
            StructField("telefone_cliente", StringType(), True),
            StructField("dt_nascimento_cliente", DateType(), True),
            StructField("dt_atualizacao", TimestampType(), False),
            StructField("tp_pessoa", StringType(), False),
            StructField("vl_renda", DoubleType(), True)
        ])

    def read_csv(self, input_path):
        logger.info(f"Lendo arquivo: {input_path}")

        return (
            self.spark.read
            .option("header", True)
            .schema(self.schema)
            .csv(input_path)
        )

    def process_bronze(self, df):
        logger.info("Processando camada Bronze")

        return (
            df.withColumn(
                "anomesdia",
                F.date_format(F.current_date(), "yyyyMMdd")
            )
            .withColumn(
                "nm_cliente",
                F.upper(F.col("nm_cliente"))
            )
            .withColumnRenamed(
                "telefone_cliente",
                "num_telefone_cliente"
            )
        )

    def process_silver(self, df_bronze):
        logger.info("Processando camada Silver")

        window_spec = (
            Window
            .partitionBy("cod_cliente")
            .orderBy(F.col("dt_atualizacao").desc())
        )

        df_dedup = (
            df_bronze
            .withColumn(
                "row_num",
                F.row_number().over(window_spec)
            )
            .filter(F.col("row_num") == 1)
            .drop("row_num")
        )

        regex_telefone = r"^\(\d{2}\)\d{5}-\d{4}$"

        return (
            df_dedup.withColumn(
                "num_telefone_cliente",
                F.when(
                    F.col("num_telefone_cliente").rlike(regex_telefone),
                    F.col("num_telefone_cliente")
                ).otherwise(None)
            )
        )

    def write_and_update_catalog(
        self,
        df,
        path,
        database,
        table_name,
        partition_col
    ):
        logger.info(f"Escrevendo tabela {table_name}")

        # Partição Física
        (
            df.write
            .mode("append")
            .partitionBy(partition_col)
            .format("parquet")
            .save(path)
        )

        partition_value = datetime.now().strftime("%Y%m%d")

        # Partição Lógica
        try:
            self.glue.create_partition(
                DatabaseName=database,
                TableName=table_name,
                PartitionInput={
                    "Values": [partition_value],
                    "StorageDescriptor": {
                        "Location": (
                            f"{path}/"
                            f"{partition_col}={partition_value}/"
                        )
                    }
                }
            )
        except ClientError as e:

            if e.response["Error"]["Code"] == "AlreadyExistsException":
                logger.warning(
                    f"Partição {partition_value} já existe na tabela {table_name}"
                )
            else:
                raise

    def run(
        self,
        input_path,
        bronze_path,
        silver_path,
        database_name
    ):
        df_raw = self.read_csv(input_path)

        df_bronze = self.process_bronze(df_raw)

        self.write_and_update_catalog(
            df=df_bronze,
            path=bronze_path,
            database=database_name,
            table_name="tabela_cliente_landing",
            partition_col="anomesdia"
        )

        # df_bronze.write \
        #     .mode("overwrite") \
        #     .partitionBy("anomesdia") \
        #     .parquet("datasets/tabela_cliente_landing")

        df_silver = self.process_silver(df_bronze)

        self.write_and_update_catalog(
            df=df_silver,
            path=silver_path,
            database=database_name,
            table_name="tb_cliente",
            partition_col="anomesdia"
        )

        # df_silver.write \
        #     .mode("overwrite") \
        #     .partitionBy("anomesdia") \
        #     .parquet("datasets/tb_cliente")


    def stop(self):
        self.spark.stop()


if __name__ == "__main__":

    etl = ETLClientes()

    try:
        etl.run(
            input_path="datasets/clientes_sinteticos.csv",
            bronze_path="s3://bucket-bronze/tabela_cliente_landing",
            silver_path="s3://bucket-silver/tb_cliente",
            database_name="default"
        )

    except Exception as e:
        logger.error(f"Falha durante execução da ETL: {e}")
        raise
        # Aqui eu colocaria um monitoramento de incidentes tipo PagerDuty ou VictorOps

    finally:
        etl.stop()