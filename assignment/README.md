# Appodeal Data Engineering Assignment

Welcome to the Appodeal Data Engineering assignment. This exercise reflects the type of tasks you may encounter on the job. As with real-world projects, the scope can vary widely — from a quick proof of concept to a production-ready system built for serious load.

We expect this to take **2–4 hours**. A perfect solution in that time is not realistic, and we take that into account during review. If you don't deliver a complete solution, include a brief write-up of your assumptions and what you would do next to close the gaps or improve the approach.

Good luck and enjoy the task.

## Business model

Users (identified by `user_id`) are shown advertisements from an advertiser (`advertiser_id`) inside a mobile application (`app_id`) in a given country (`country_code`). At that moment an **impression** event is recorded and stored. If the user taps the banner, a **click** event is recorded. Revenue is generated **only** on a click.

## Input data

### Arguments

The application must accept **two arguments**. Each argument is a **list of JSON files**: the first is **click** events, the second is **impression** events.

### Impression event schema

- `id` (string): UUID of the impression.
- `user_id` (string): UUID of the user who was shown the ad.
- `app_id` (integer): identifier of the application in which the impression occurred.
- `country_code` (string): two-letter country code; not required to follow a standard such as ISO 3166.
- `advertiser_id` (integer): identifier of the advertiser who purchased the impression.

Sample data is provided in `impressions.json`.

### Click event schema

- `id` (string): UUID of the click.
- `impression_id` (string): reference to the UUID of the impression that was clicked.
- `revenue` (float): amount paid by the advertiser when the click is counted.

Sample data is provided in `clicks.json`.

## Goals

### 1. Metrics calculation

The business wants to evaluate performance across a set of dimensions — for example, how applications perform by country, to identify new opportunities or weak markets.

**Metrics:**

- number of impressions;
- number of clicks;
- total revenue.

**Dimensions:**

- `app_id`;
- `country_code`.

Write the result to a JSON file in the following format:

```json
[
  {
    "app_id": 1,
    "country_code": "US",
    "impressions": 102,
    "clicks": 12,
    "revenue": 10.2
  },
  ...
]
```

### 2. Top-5 `advertiser_id` per app + country pair

Identify the strongest advertisers for each application and country to focus on the most valuable partners. Performance is measured by the highest **`revenue` / `impressions`** ratio (average revenue per impression). Only consider advertisers with **at least 5 impressions**.

**Output fields:**

- `app_id`;
- `country_code`;
- `recommended_advertiser_ids` (list of up to five advertiser identifiers with the highest revenue-per-impression in a given app + country pair).

JSON format:

```json
[
  {
    "app_id": 1,
    "country_code": "US",
    "recommended_advertiser_ids": [32, 12, 45, 4, 1]
  },
  ...
]
```

### 3. Median user spend

For each country, determine the **median spend per user**. The median is preferred because outliers from individual users heavily skew the mean.

JSON format:

```json
[
  {
    "country_code": "US",
    "median_spend": 2.54
  },
  ...
]
```

## Technical requirements

- Any programming language.
- Instructions for running the solution must be in `README.md`.
- The solution must be easy to run. Tools like [mise] can be used for dependency management.
- Any libraries for JSON parsing and CLI argument handling are allowed.
- Any data processing framework is acceptable (Pandas, Polars, Spark, etc.). We are looking at your thought process, data handling, and coding ability — not familiarity with a specific tool.
- The application will run on a single machine with **8 cores**; **performance** is taken into consideration.

Submit your solution as a **git repository**: as a zip archive or a GitHub link. If the repository is private, grant read access to `mpasa`.

## Data

Sample files for impressions and clicks are included. There is no reference output: as with real-world data, there is no single "correct" answer. The solution is additionally tested against **private datasets**, including edge cases.

[mise]: https://mise.jdx.dev
