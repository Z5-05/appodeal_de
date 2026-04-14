import pyspark.sql.functions as f
from pyspark.sql import DataFrame
from pipeline.constants import COUNTRY_CODES_ALPHA2


def preprocess_country_code(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "country_code",
        f.when(f.col("country_code").isin(COUNTRY_CODES_ALPHA2), f.col("country_code"))
        .otherwise(f.lit("Unknown")),
    )
