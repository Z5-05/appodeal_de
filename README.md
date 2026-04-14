# appodeal-de

PySpark pipeline for the Appodeal Data Engineering assignment.

---

## Project layout

```
appodeal-de/
├── main.py                              # entry point
├── pipeline/
│   ├── constants.py                     # IMPRESSIONS_SCHEMA, CLICKS_SCHEMA, COUNTRY_CODES_ALPHA2
│   ├── session.py                       # creating SparkSession with local[*]
│   ├── preprocess.py                    # preprocess field country_code, all non-lambda2 codes are mapped to "Unknown", remove rows with NULL user_id
│   ├── io/
│   │   ├── loader.py                    # read JSONs and join impressions and clicks
│   │   └── writer.py                    # write DataFrame as JSON array
│   └── transforms.py                    # target methods: metrics_calculation(), top_advertisers(), median_spend()
├── tests/                               # tests
├── notebooks/                           # EDA analysis
├── data/                                # input JSONs
├── .mise.toml                           # mise configuration
└── pyproject.toml                       # pyproject configuration
```

---

## mise commands

### Dependencies

```bash
mise run install              # pip install -e '.[dev]'  — pyspark, pytest, ruff
mise run install-notebooks    # pip install -e '.[dev,notebooks]' + register Jupyter kernel
```

### Main pipeline

```bash
mise run run                                                                             # defaults: data/input/clicks.json and data/input/impressions.json
mise run run -- --clicks a.json b.json --impressions c.json d.json                       # multiple input files -> output/
```

Writes JSON output to `output/` (previous run is deleted automatically):

| File | Contents |
|------|----------|
| `output/metrics_calculation.json` | Impressions, clicks, revenue grouped by `app_id` + `country_code` |
| `output/top_advertisers.json` | Top-5 advertiser IDs by revenue/impressions ratio per `app_id` + `country_code` |
| `output/median_spend.json` | Median user spend per `country_code` |

### EDA

```bash
mise run notebook    # opens Jupyter Lab from project root
```

`pipeline` is importable inside notebooks as a regular package — same code as production. load, preprocess, get_spark are used for analysis.

### Quality control

```bash
mise run test                 # run all tests
mise run lint                 # check code style
mise run format               # auto-format code
```

All tests are generated using Claude Code Agent.

---

## Data Quality & Preprocessing

Findings from EDA on the sample dataset (1 038 impressions, 705 clicks):

- **NULLs**: `user_id` — 4, `country_code` — 5, `revenue` — 1. Revenue NULLs are coalesced to `0.0` in `median_spend`. Rows with NULL `user_id` are removed in `preprocess.py`. Why? Because business value without linking to a user is not actionable.
- **Duplicate rows**: after the left join, the joined dataset contained several identical rows (full-row duplicates). Each impression should represent a single user interaction, so duplicates are treated as bad data. They are removed with `.distinct()` in `loader.py`. After deduplication, 312 impressions still have no matching click.
- **Invalid country codes**: most rows use plausible ISO 3166-1 alpha-2 codes, but some values are not valid alpha-2 (`XX`, `ZZZ`, `??`, `NaN`, etc.). `preprocess.py` maps anything outside the allowed alpha-2 set to `"Unknown"` before aggregation. Rows with `"Unknown"` are still included in `metrics_calculation` and `median_spend` (the underlying events are valid), but dropped from `top_advertisers` (no reliable country for targeting).

---

## Performance & Scalability Notes

1. **Dedup before metrics**  
   The join can create up to four copies of the same full row. Duplicates are removed once in `loader.py` with `.distinct()`. After that, every step works on unique rows.  
   `count()` is used instead of `countDistinct()`. That is cheaper for Spark: partial aggregation on the map side is possible, with fewer shuffles.

2. **Skew monitoring + salting trigger**  
   This sample shows no strong skew. The largest groups are `(app=1, US)=151`, `(app=2, US)=81`, and `(app=1, CA)=75`. The US/CA ratio is about `2:1`. Salting is not used yet.  
   If one `country_code` partition grows to about five times the median partition size, salting should be enabled. Use `salted_key = concat(country_code, '_', hash(app_id) % N)`, where `N` is the number of salt buckets.

3. **Top advertisers: ROW_NUMBER vs collect_list + sort + slice**  
   Both ways give the same answer. The pipeline uses `ROW_NUMBER`, then keeps the top five rows, then `collect_list`. A long list for the whole group is never built first.  
   The other way (`collect_list` on all rows, then `sort_array`, then `slice`) sends many rows to a few nodes. That can use a lot of memory and cause OOM on skewed US groups.

4. **Median spend — percentile_approx instead of exact median**  
   EDA compared `median()` with `percentile_approx(0.5, accuracy)` for accuracy 100, 1 000, and 10 000. With `accuracy=1000`, the result matches the exact median for all real country codes.  
   The only difference is in the `"Unknown"` bucket. There the distribution has many zeros, and a low-accuracy sketch can return `0`. That bucket is not used for business decisions.  
   The code uses `percentile_approx("spend", 0.5, 1000)`. For real countries this is close enough to the exact median. It also scales better on large data: partial aggregation across partitions is possible without sorting everything.

### Testing

All tests start a local Spark session (`@pytest.mark.slow`).

- **`test_session.py`**: checks that `get_spark()` returns a running session, shuffle partitions are `8`, and a small DataFrame can be created and collected.
- **`test_transforms.py`**: checks `preprocess()` (valid codes kept, bad codes → `"Unknown"`, null country → `"Unknown"`, null `user_id` dropped); `metrics_calculation()` (counts, revenue, grouping); `top_advertisers()` (order by revenue per impression, `"Unknown"` excluded, minimum impressions); `median_spend()` (per country, grouping, null revenue as zero).