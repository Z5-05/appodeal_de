import pytest
from pyspark.sql.types import (
    FloatType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from pipeline.preprocess import preprocess_country_code
from pipeline.transforms import median_spend, metrics_calculation, top_advertisers

pytestmark = pytest.mark.slow

SOURCE_SCHEMA = StructType(
    [
        StructField("impression_id", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("app_id", IntegerType(), True),
        StructField("country_code", StringType(), True),
        StructField("advertiser_id", IntegerType(), True),
        StructField("click_id", StringType(), True),
        StructField("revenue", FloatType(), True),
    ]
)


def make_source(spark, rows):
    return spark.createDataFrame(rows, SOURCE_SCHEMA)


# ---------------------------------------------------------------------------
# preprocess_country_code
# ---------------------------------------------------------------------------


def test_valid_country_codes_are_kept(spark):
    df = spark.createDataFrame([("US",), ("DE",), ("GB",)], ["country_code"])
    result = {r.country_code for r in preprocess_country_code(df).collect()}
    assert result == {"US", "DE", "GB"}


def test_invalid_country_codes_become_unknown(spark):
    df = spark.createDataFrame([("XX",), ("ZZZ",), ("??",), ("NaN",)], ["country_code"])
    result = preprocess_country_code(df).collect()
    assert all(r.country_code == "Unknown" for r in result)


def test_null_country_code_becomes_unknown(spark):
    df = spark.createDataFrame(
        [(None,)], StructType([StructField("country_code", StringType(), True)])
    )
    result = preprocess_country_code(df).collect()
    assert result[0].country_code == "Unknown"


# ---------------------------------------------------------------------------
# metrics_calculation
# ---------------------------------------------------------------------------


def test_metrics_counts_impressions_clicks_and_revenue(spark):
    rows = [
        ("imp1", "u1", 1, "US", 10, "clk1", 1.0),
        ("imp2", "u2", 1, "US", 10, "clk2", 2.0),
        ("imp3", "u3", 1, "US", 10, None, None),  # impression without a click
    ]
    result = metrics_calculation(make_source(spark, rows)).collect()
    assert len(result) == 1
    r = result[0]
    assert r.impressions == 3
    assert r.clicks == 2
    assert abs(r.revenue - 3.0) < 1e-5


def test_metrics_revenue_is_zero_when_no_clicks(spark):
    rows = [
        ("imp1", "u1", 1, "US", 10, None, None),
        ("imp2", "u2", 1, "US", 10, None, None),
    ]
    result = metrics_calculation(make_source(spark, rows)).collect()
    assert result[0].revenue == 0.0


def test_metrics_groups_by_app_and_country(spark):
    rows = [
        ("imp1", "u1", 1, "US", 10, "clk1", 1.0),
        ("imp2", "u2", 1, "DE", 10, "clk2", 2.0),
        ("imp3", "u3", 2, "US", 10, "clk3", 3.0),
    ]
    result = {
        (r.app_id, r.country_code): r.impressions
        for r in metrics_calculation(make_source(spark, rows)).collect()
    }
    assert result == {(1, "US"): 1, (1, "DE"): 1, (2, "US"): 1}


# ---------------------------------------------------------------------------
# top_advertisers
# ---------------------------------------------------------------------------


def test_top_advertisers_ordered_by_revenue_per_impression(spark):
    # 6 advertisers, 5 impressions each — revenue/impression: 6.0 → 1.0
    rows = []
    for adv_id, rev in [(1, 6.0), (2, 5.0), (3, 4.0), (4, 3.0), (5, 2.0), (6, 1.0)]:
        for i in range(5):
            rows.append(
                (f"imp_{adv_id}_{i}", "u1", 1, "US", adv_id, f"clk_{adv_id}_{i}", rev)
            )

    result = top_advertisers(make_source(spark, rows)).collect()
    assert len(result) == 1
    # top 5 in descending order of revenue/impression; advertiser 6 is dropped
    assert result[0].recommended_advertiser_ids == [1, 2, 3, 4, 5]


def test_top_advertisers_excludes_unknown_country(spark):
    rows = [(f"imp{i}", "u1", 1, "Unknown", 10, f"clk{i}", 1.0) for i in range(5)]
    result = top_advertisers(make_source(spark, rows)).collect()
    assert result == []


def test_top_advertisers_min_impressions_filter(spark):
    # advertiser with only 4 impressions should not appear
    rows = [(f"imp{i}", "u1", 1, "US", 99, f"clk{i}", 1.0) for i in range(4)]
    result = top_advertisers(make_source(spark, rows)).collect()
    assert result == []


# ---------------------------------------------------------------------------
# median_spend
# ---------------------------------------------------------------------------


def test_median_spend_single_country(spark):
    # five users, spend = 1..5 → median = 3
    rows = [(f"imp{i}", f"u{i}", 1, "US", 10, f"clk{i}", float(i)) for i in range(1, 6)]
    result = {
        r.country_code: r.median_spend
        for r in median_spend(make_source(spark, rows)).collect()
    }
    assert abs(result["US"] - 3.0) < 0.1  # percentile_approx tolerance


def test_median_spend_groups_by_country(spark):
    rows = [
        ("imp1", "u1", 1, "US", 10, "clk1", 10.0),
        ("imp2", "u2", 1, "DE", 10, "clk2", 20.0),
    ]
    result = {
        r.country_code: r.median_spend
        for r in median_spend(make_source(spark, rows)).collect()
    }
    assert abs(result["US"] - 10.0) < 0.1
    assert abs(result["DE"] - 20.0) < 0.1


def test_median_spend_null_revenue_treated_as_zero(spark):
    # user with no click (revenue=NULL) should contribute 0.0 to the distribution
    rows = [
        ("imp1", "u1", 1, "US", 10, None, None),
        ("imp2", "u1", 1, "US", 10, None, None),  # same user, still 0 spend
    ]
    result = {
        r.country_code: r.median_spend
        for r in median_spend(make_source(spark, rows)).collect()
    }
    assert result["US"] == 0.0
