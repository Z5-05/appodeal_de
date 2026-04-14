import argparse
from pipeline.preprocess import preprocess_country_code
from pipeline.session import get_spark
from pipeline.io.loader import load
from pipeline.io.writer import write
from pipeline.transforms import metrics_calculation, top_advertisers, median_spend
from pipeline.constants import IMPRESSIONS_SCHEMA, CLICKS_SCHEMA


def main():
    args = parse_args()
    spark = get_spark()

    source_df = load(spark, args.impressions, IMPRESSIONS_SCHEMA, args.clicks, CLICKS_SCHEMA)
    source_df = preprocess_country_code(source_df)

    write(metrics_calculation(source_df), "output/metrics_calculation.json")
    write(top_advertisers(source_df), "output/top_advertisers.json")
    write(median_spend(source_df), "output/median_spend.json")


def parse_args():
    parser = argparse.ArgumentParser(description="Appodeal metrics pipeline")
    parser.add_argument(
        "--clicks",
        nargs="+",
        type=str,
        default=["data/input/clicks.json"],
        help="List of click JSON files (default: data/input/clicks.json)",
    )
    parser.add_argument(
        "--impressions",
        nargs="+",
        type=str,
        default=["data/input/impressions.json"],
        help="List of impression JSON files (default: data/input/impressions.json)",
    )
    return parser.parse_args()


if __name__ == "__main__":
    main()
