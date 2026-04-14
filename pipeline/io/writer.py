import os

from pyspark.sql import DataFrame


def write(df: DataFrame, output_path: str) -> None:
    records = df.toJSON().collect()
    os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)
    with open(output_path, "w") as fh:
        fh.write("[\n  " + ",\n  ".join(records) + "\n]\n")
