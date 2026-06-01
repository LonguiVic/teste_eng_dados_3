from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    TimestampType,
    DateType
)
from pyspark.sql.window import Window
from datetime import datetime
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)


class ETLClientes:

    def __init__(self, app_name="ETL_Clientes"):
        self.spark = (
            SparkSession.builder
            .appName(app_name)
            .getOrCreate()
        )

        self.schema = self._get_cliente_schema()

    def _get_cliente_schema(self):
        return StructType([
            StructField("cod_cliente", StringType(), True),
            StructField("nm_cliente", StringType(), True),
            StructField("nm_pais_cliente", StringType(), True),
            StructField("nm_cidade_cliente", StringType(), True),
            StructField("nm_rua_cliente", StringType(), True),
            StructField("num_casa_cliente", StringType(), True),
            StructField("telefone_cliente", StringType(), True),
            StructField("dt_nascimento_cliente", DateType(), True),
            StructField("dt_atualizacao", TimestampType(), True),
            StructField("tp_pessoa", StringType(), True),
            StructField("vl_renda", DoubleType(), True)
        ])

    def read_csv(self, input_path):
        logging.info(f"Lendo arquivo: {input_path}")

        return (
            self.spark.read
            .option("header", True)
            .schema(self.schema)
            .csv(input_path)
        )

    def process_bronze(self, df):
        logging.info("Processando camada Bronze")

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
        logging.info("Processando camada Silver")

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
        logging.info(f"Escrevendo tabela {table_name}")

        (
            df.write
            .mode("append")
            .partitionBy(partition_col)
            .format("parquet")
            .save(path)
        )

        partition_value = datetime.now().strftime("%Y%m%d")

        alter_query = f"""
        ALTER TABLE {database}.{table_name}
        ADD IF NOT EXISTS PARTITION
        ({partition_col}='{partition_value}')
        LOCATION
        '{path}/{partition_col}={partition_value}/'
        """

        self.spark.sql(alter_query)

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

        df_silver = self.process_silver(df_bronze)

        self.write_and_update_catalog(
            df=df_silver,
            path=silver_path,
            database=database_name,
            table_name="tb_cliente",
            partition_col="anomesdia"
        )

    def stop(self):
        self.spark.stop()


if __name__ == "__main__":

    etl = ETLClientes()

    etl.run(
        input_path="datasets/clientes_sinteticos.csv",
        bronze_path="s3://bucket-bronze/tabela_cliente_landing",
        silver_path="s3://bucket-silver/tb_cliente",
        database_name="default"
    )

    etl.stop()