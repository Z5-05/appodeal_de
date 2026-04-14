import argparse

from pipeline.constants import CLICKS_SCHEMA, IMPRESSIONS_SCHEMA
from pipeline.io.loader import load
from pipeline.io.writer import write
from pipeline.preprocess import preprocess
from pipeline.session import get_spark
from pipeline.transforms import median_spend, metrics_calculation, top_advertisers


def main():
    args = parse_args()
    spark = get_spark()

    source_df = load(
        spark, args.impressions, IMPRESSIONS_SCHEMA, args.clicks, CLICKS_SCHEMA
    )
    source_df = preprocess(source_df)

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
        help="One or more click JSON files (default: data/input/clicks.json)",
    )
    parser.add_argument(
        "--impressions",
        nargs="+",
        type=str,
        default=["data/input/impressions.json"],
        help="One or more impression JSON files (default: data/input/impressions.json)",
    )
    return parser.parse_args()


if __name__ == "__main__":
    main()
