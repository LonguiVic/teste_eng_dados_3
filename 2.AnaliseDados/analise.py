from pathlib import Path
import sys

sys.path.append(str(Path(__file__).resolve().parent.parent))

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from utils.utils import logs

logger = logs(__name__)

class AnaliseClientes:

    def __init__(self, app_name="Analise_Clientes"):
        self.spark = (
            SparkSession.builder
            .appName(app_name)
            .getOrCreate()
        )

    def top_5_clientes_mais_atualizados(self, bronze_path):
        logger.info("Gerando DF Top 5 clientes")

        df_bronze = self.spark.read.parquet(bronze_path)

        return (
            df_bronze
            .groupBy("cod_cliente")
            .count()
            .orderBy(F.desc("count"))
            .limit(5)
        )

    def media_idade_clientes(self, silver_path):
        logger.info("Gerando DF Média Idade Clientes")

        df_silver = self.spark.read.parquet(silver_path)

        df_idade = df_silver.withColumn(
            "idade",
            F.floor(
                F.months_between(
                    F.current_date(),
                    F.col("dt_nascimento_cliente")
                ) / 12
            )
        )

        media = (
            df_idade
            .agg(
                F.avg("idade").alias("media_idade")
            )
            .collect()[0]["media_idade"]
        )

        return media

    def gerar_relatorio(
        self,
        bronze_path,
        silver_path,
        output_file
    ):

        top_5 = self.top_5_clientes_mais_atualizados(bronze_path)

        media_idade = self.media_idade_clientes(silver_path)

        with open(output_file, "w", encoding="utf-8") as f:

            f.write("ANÁLISE DOS DADOS\n")
            f.write("=" * 50 + "\n\n")

            f.write(
                "1. TOP 5 CLIENTES COM MAIS ATUALIZAÇÕES\n\n"
            )

            for row in top_5.collect():
                f.write(
                    f"Cliente: {row['cod_cliente']} "
                    f"- Atualizações: {row['count']}\n"
                )

            f.write("\n")

            f.write(
                "2. MÉDIA DE IDADE DOS CLIENTES\n\n"
            )

            f.write(
                f"Média de idade: {round(media_idade, 2)} anos\n\n"
            )

            f.write(
                "Observações:\n"
            )

            f.write(
                "- O ranking de atualizações foi calculado "
                "sobre a camada Bronze para preservar o histórico.\n"
            )

            f.write(
                "- A média de idade foi calculada sobre a camada "
                "Silver, que contém apenas a versão mais recente "
                "de cada cliente.\n"
            )

        logger.info(f"Relatório gerado: {output_file}")

    def stop(self):
        self.spark.stop()


if __name__ == "__main__":

    bronze_path = "datasets/tabela_cliente_landing"
    silver_path = "datasets/tb_cliente"

    analise = AnaliseClientes()

    try:
        analise.gerar_relatorio(
            bronze_path=bronze_path,
            silver_path=silver_path,
            output_file="2.AnaliseDados/analise.txt"
        )

    except Exception as e:
        logger.error(f"Falha durante execução da análise: {e}")
        raise
        # Aqui eu colocaria um monitoramento de incidentes tipo PagerDuty ou VictorOps

    finally:
        analise.stop()