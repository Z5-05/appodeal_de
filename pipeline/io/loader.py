from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType


def load(
    spark: SparkSession,
    impressions_path: list[str],
    impressions_schema: StructType,
    clicks_path: list[str],
    clicks_schema: StructType,
) -> DataFrame:

    impressions_df = (
        spark.read.option("multiLine", True)
        .format("json")
        .schema(impressions_schema)
        .load(impressions_path)
    )
    clicks_df = (
        spark.read.option("multiLine", True)
        .format("json")
        .schema(clicks_schema)
        .load(clicks_path)
    )

    dedup_loaded_df = (
        impressions_df.join(
            clicks_df, impressions_df.id == clicks_df.impression_id, how="left"
        ).select(
            impressions_df.id.alias("impression_id"),
            impressions_df.user_id,
            impressions_df.app_id,
            impressions_df.country_code,
            impressions_df.advertiser_id,
            clicks_df.id.alias("click_id"),
            clicks_df.revenue,
        )
    ).distinct()

    return dedup_loaded_df
