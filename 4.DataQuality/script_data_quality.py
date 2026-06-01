from pathlib import Path
import sys

sys.path.append(str(Path(__file__).resolve().parent.parent))

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from utils.utils import logs


logger = logs(__name__)

class DataQuality:

    def __init__(self):
        self.spark = (
            SparkSession.builder
            .appName("DataQuality")
            .getOrCreate()
        )

    def run(self, silver_path):

        df = self.spark.read.parquet(silver_path)

        resultados = []

        # COD_CLIENTE não nulo
        resultados.append({
            "regra": "cod_cliente nao nulo",
            "falhas": df.filter(
                F.col("cod_cliente").isNull()
            ).count()
        })

        # NM_CLIENTE não nulo
        resultados.append({
            "regra": "nm_cliente nao nulo",
            "falhas": df.filter(
                F.col("nm_cliente").isNull()
            ).count()
        })

        # COD_CLIENTE duplicados
        duplicados = (
            df.groupBy("cod_cliente")
            .count()
            .filter(F.col("count") > 1)
            .count()
        )

        resultados.append({
            "regra": "cod_cliente unico",
            "falhas": duplicados
        })

        # Validação formatação telefone
        regex = r"^\(\d{2}\)\d{5}-\d{4}$"

        resultados.append({
            "regra": "telefone valido",
            "falhas": df.filter(
                (~F.col("num_telefone_cliente").rlike(regex))
                &
                (F.col("num_telefone_cliente").isNotNull())
            ).count()
        })

        # Validação Data de Nascimento
        resultados.append({
            "regra": "data nascimento valida",
            "falhas": df.filter(
                F.col("dt_nascimento_cliente") >
                F.current_date()
            ).count()
        })

        # Renda Positiva
        resultados.append({
            "regra": "renda positiva",
            "falhas": df.filter(
                F.col("vl_renda") < 0
            ).count()
        })

        # Tipo de Pessoa
        resultados.append({
            "regra": "tipo pessoa valido",
            "falhas": df.filter(
                ~F.col("tp_pessoa").isin(
                    "PF",
                    "PJ"
                )
            ).count()
        })


        logger.info("\nRELATORIO DE DATA QUALITY\n")

        for resultado in resultados:

            status = (
                "PASSOU"
                if resultado["falhas"] == 0
                else "FALHOU"
            )

            logger.info(
                f"{resultado['regra']} | "
                f"{status} | "
                f"falhas={resultado['falhas']}"
            )


if __name__ == "__main__":

    dq = DataQuality()

    try: 
        dq.run(
            "datasets/tb_cliente"
        )
    except Exception as e:
        logger.info(f"Falha ao executar o Data Quality: {e}")
        raise
        # Aqui eu colocaria um monitoramento de incidentes tipo PagerDuty ou VictorOps

    # Se eu estivesse num ambiente de produção, provavelmente escolheria o AWS Glue Data Quality
    # para fazer a validação dos dados

    # Valores validados:
    # Valores nulos
    # Duplicidade
    # Formatação correta
    # Valores fixos esperados