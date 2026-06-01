from pathlib import Path
import sys

sys.path.append(
    str(
        Path(__file__).resolve().parent.parent / "1.ETL"
    )
)

from datetime import datetime
import pytest

from pyspark.sql import SparkSession
from pyspark.sql.types import *

from script import ETLClientes

from utils.utils import logs

logger = logs(__name__)


@pytest.fixture(scope="session")
def spark():

    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("test")
        .getOrCreate()
    )

    yield spark

    spark.stop()


@pytest.fixture
def etl():

    return ETLClientes()


# 1. Happy path
# - Cliente duplicado
# - Mantém apenas o registro mais recente
def test_process_silver_dedup_happy_path(
    spark,
    etl
):

    schema = StructType([
        StructField(
            "cod_cliente",
            StringType(),
            True
        ),
        StructField(
            "num_telefone_cliente",
            StringType(),
            True
        ),
        StructField(
            "dt_atualizacao",
            TimestampType(),
            True
        )
    ])

    data = [

        (
            "1",
            "(11)99999-9999",
            datetime(
                2025,
                1,
                1
            )
        ),

        (
            "1",
            "(11)88888-8888",
            datetime(
                2025,
                2,
                1
            )
        )

    ]

    df = spark.createDataFrame(
        data,
        schema
    )

    result = etl.process_silver(df)

    rows = result.collect()

    assert len(rows) == 1

    assert (
        rows[0]["num_telefone_cliente"]
        ==
        "(11)88888-8888"
    )


# 2. Validação de telefone
# - Telefones fora do padrão são convertidos para nulo
def test_process_silver_invalid_phone(
    spark,
    etl
):

    schema = StructType([
        StructField(
            "cod_cliente",
            StringType(),
            True
        ),
        StructField(
            "num_telefone_cliente",
            StringType(),
            True
        ),
        StructField(
            "dt_atualizacao",
            TimestampType(),
            True
        )
    ])

    data = [

        (
            "1",
            "11999999999",
            datetime(
                2025,
                1,
                1
            )
        )

    ]

    df = spark.createDataFrame(
        data,
        schema
    )

    result = etl.process_silver(df)

    row = result.collect()[0]

    assert (
        row["num_telefone_cliente"]
        is None
    )


# 3. Caso de borda
# - Cliente com apenas um registro
def test_process_silver_single_record(
    spark,
    etl
):

    schema = StructType([
        StructField(
            "cod_cliente",
            StringType(),
            True
        ),
        StructField(
            "num_telefone_cliente",
            StringType(),
            True
        ),
        StructField(
            "dt_atualizacao",
            TimestampType(),
            True
        )
    ])

    data = [

        (
            "1",
            "(11)99999-9999",
            datetime(
                2025,
                1,
                1
            )
        )

    ]

    df = spark.createDataFrame(
        data,
        schema
    )

    result = etl.process_silver(df)

    assert result.count() == 1


# 4. Dataset vazio
# - Processo executa sem erro e retorna dataframe vazio
def test_process_silver_empty_dataframe(
    spark,
    etl
):

    schema = StructType([
        StructField(
            "cod_cliente",
            StringType(),
            True
        ),
        StructField(
            "num_telefone_cliente",
            StringType(),
            True
        ),
        StructField(
            "dt_atualizacao",
            TimestampType(),
            True
        )
    ])

    df = spark.createDataFrame(
        [],
        schema
    )

    result = etl.process_silver(df)

    assert result.count() == 0
 