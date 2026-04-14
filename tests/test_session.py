import pytest
from pyspark.sql import SparkSession

pytestmark = pytest.mark.slow


def test_get_spark_returns_active_session(spark):
    assert isinstance(spark, SparkSession)
    assert spark.sparkContext.defaultParallelism > 0


def test_shuffle_partitions_configured(spark):
    assert spark.conf.get("spark.sql.shuffle.partitions") == "8"


def test_spark_can_create_and_collect_dataframe(spark):
    df = spark.createDataFrame([(1, "US"), (2, "DE")], ["id", "country"])
    rows = df.orderBy("id").collect()
    assert len(rows) == 2
    assert rows[0].country == "US"
    assert rows[1].country == "DE"
