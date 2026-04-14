# appodeal-de

PySpark pipeline for the Appodeal Data Engineering assignment.

---

## Project layout

```
appodeal-de/
├── main.py                              # entry point — reads args, wires pipeline steps
├── pipeline/
│   ├── constants.py                     # IMPRESSIONS_SCHEMA, CLICKS_SCHEMA, COUNTRY_CODES_ALPHA2
│   ├── session.py                       # get_spark()
│   ├── preprocess.py                    # preprocess_country_code() — normalises invalid codes → "Unknown"
│   ├── io/
│   │   ├── loader.py                    # load() — reads JSONs and left-joins events
│   │   └── writer.py                   # write() — saves DataFrame as JSON/parquet
│   └── transforms.py                    # metrics_calculation(), top_advertisers(), median_spend()
├── tests/
├── notebooks/
│   └── EDA.ipynb                        # exploratory analysis, uses pipeline as a package
├── data/
│   ├── clicks.json
│   └── impressions.json
├── .mise.toml
└── pyproject.toml
```

---

## mise tasks

All commands are run from the project root.

### Dependencies

```bash
mise run install              # pip install -e '.[dev]'  — pyspark, pytest, ruff
mise run install-notebooks    # pip install -e '.[dev,notebooks]' + register Jupyter kernel
```

### Pipeline

```bash
mise run run                                                       # defaults: data/input/*.json → output/
mise run run -- --clicks a.json --impressions b.json               # custom input files
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

`pipeline` is importable inside notebooks as a regular package — same code as production.

### Quality

```bash
mise run test                 # pytest tests/ -v --tb=short
mise run test -k slow         # only tests that start Spark
mise run test -k "not slow"   # fast tests only

mise run lint                 # ruff check .
mise run format               # ruff format .
```

---

## Environment

Variables activated automatically on directory entry via `.mise.toml`:

| Variable | Value |
|----------|-------|
| `PYSPARK_PYTHON` | `python` |
| `PYSPARK_SUBMIT_ARGS` | suppresses Spark UI console progress |
| `PYTHONPATH` | project root — `pipeline` importable without pip install as fallback |
| `DATA_DIR` | `<project_root>/data` |
| `JUPYTER_PLATFORM_DIRS` | `1` |

---

## Data Quality & Preprocessing

Findings from EDA on the sample dataset (1 038 impressions, 705 clicks):

- **NULLs**: `user_id` — 4, `country_code` — 5, `revenue` — 1. Revenue NULLs are coalesced to `0.0` in `median_spend`.
- **Duplicate rows**: after the left join up to 4× full-row duplicates appear per `impression_id`. Confirmed to be exact copies (all fields identical). Removed with `.distinct()` in `loader.py`; 312 impressions remain without a matching click.
- **Invalid country codes**: data contains non-ISO values (`XX`, `ZZZ`, `??`, `NaN`). `preprocess_country_code()` maps anything outside the ISO 3166-1 alpha-2 list to `"Unknown"` before aggregation. `"Unknown"` rows are kept in `metrics_calculation` and `median_spend` (the events are real), but excluded from `top_advertisers` (no targetable country signal).

---

## Performance & Scalability Notes

1. **Dedup before metrics**  
   Join produces up to 4× full-row duplicates. Dedup runs once in `loader.py` with `.distinct()`, so all downstream aggregations see unique events. This lets us use `count()` instead of `countDistinct()`, enabling Spark's map-side partial aggregation and avoiding an extra shuffle.

2. **Skew monitoring + salting trigger**  
   Current sample shows no significant skew — top groups: `(app=1, US)=151`, `(app=2, US)=81`, `(app=1, CA)=75`; US/CA ratio ~`2:1`. No salting applied now.  
   Trigger: if any `country_code` partition exceeds `~5×` the median partition size, enable salting: `salted_key = concat(country_code, '_', hash(app_id) % N)` with `N = 10`.

3. **Top advertisers: ROW_NUMBER vs collect_list + sort + slice**  
   Both approaches produce identical results. We use `ROW_NUMBER` → filter top-5 → `collect_list` because it materialises at most 5 rows per group before building the list.  
   The alternative (`collect_list` all rows → `sort_array` → `slice`) pulls every row onto reducer nodes first, creating large intermediate arrays that risk OOM on skewed US partitions.

4. **Median spend — percentile_approx over exact median**  
   EDA benchmark compared `median()` against `percentile_approx(0.5, accuracy)` at levels 100 / 1 000 / 10 000. At `accuracy=1000` the result matches the exact median for all real country codes.  
   The only divergence is in the `"Unknown"` noise bucket (heavily zero-weighted distribution), where the sketch at low accuracy collapses to `0` — but that bucket is not a target for business decisions.  
   We use `percentile_approx("spend", 0.5, 1000)`: accuracy is equivalent to exact for real data, and the function supports partial aggregation across partitions without a full sort.
