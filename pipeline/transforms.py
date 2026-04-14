import pyspark.sql.functions as f
from pyspark.sql import DataFrame
from pyspark.sql.window import Window


def metrics_calculation(df: DataFrame) -> DataFrame:
    return df.groupBy("app_id", "country_code").agg(
        f.count("impression_id").alias("impressions"),
        f.count("click_id").alias("clicks"),
        f.sum(f.coalesce(f.col("revenue"), f.lit(0.0))).alias("revenue"),
    )


def top_advertisers(df: DataFrame) -> DataFrame:
    window = Window.partitionBy("app_id", "country_code").orderBy(
        f.col("revenue_per_impression").desc()
    )

    return (
        df.filter(f.col("country_code") != "Unknown")
        .groupBy("app_id", "country_code", "advertiser_id")
        .agg(
            f.count("impression_id").alias("impressions"),
            f.sum("revenue").alias("total_revenue"),
        )
        .filter(f.col("impressions") >= 5)
        .withColumn(
            "revenue_per_impression", f.col("total_revenue") / f.col("impressions")
        )
        .withColumn("rn", f.row_number().over(window))
        .filter(f.col("rn") <= 5)
        .groupBy("app_id", "country_code")
        .agg(
            f.sort_array(
                f.collect_list(f.struct(f.col("rn"), f.col("advertiser_id")))
            ).alias("pairs")
        )
        .withColumn(
            "recommended_advertiser_ids",
            f.transform(f.col("pairs"), lambda x: x["advertiser_id"]),
        )
        .drop("pairs")
    )


def median_spend(df: DataFrame) -> DataFrame:
    return (
        df.groupBy("country_code", "user_id")
        .agg(f.sum(f.coalesce("revenue", f.lit(0.0))).alias("spend"))
        .groupBy("country_code")
        .agg(f.percentile_approx("spend", 0.5, 1000).alias("median_spend"))
    )
