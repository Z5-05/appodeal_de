from pyspark.sql import SparkSession


def get_spark(app_name: str = "Appodeal DE assignment", master: str = "local[*]") -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .master(master)
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
