# Distributed Multi-Model Analytics for E-Commerce Data

**AUCA — Big Data Analytics Final Project**
Thierry SEBAKUNZI · Student ID 101394 · November 2026

A multi-model analytics system over a synthetic e-commerce dataset, using
**MongoDB** for document-shaped data, **HBase** for time-series and sparse
wide-column data, and **Apache Spark** for distributed processing — then
combining all three in one integrated analysis.

---

## Scale actually processed

| | |
|---|---:|
| Sessions | **1,999,996** |
| Page views | **19,999,594** |
| Transactions | 500,000 |
| Transaction line items | 954,134 |
| Users | 10,000 |
| Products | 5,000 across 25 categories, 109 subcategories |
| Raw JSON | 3.9 GB |
| Parquet after cleaning | 853 MB |

---

## Architecture

```
dataset_generator.py
        |
        v
   raw JSON (3.9 GB)  --->  to_jsonl.py  --->  JSON Lines (splittable)
        |                                            |
        |                                            v
        |                                      batch_clean.py
        |                                            |
        |                                            v
        |                                      Parquet (853 MB)
        |                                        /        \
        v                                       v          v
   MongoDB                HBase          analytics_batch  spark_sql
   - products             - user_sessions   - affinity     - 5 queries
   - users + summary      - product_daily   - cohorts
   - transactions
   - sessions (200k)
        |                     |
        +----------+----------+
                   v
            clv_analysis.py          <- reads BOTH databases
                   v
              results/ + figures/
```

---

## What goes where, and why

**MongoDB — document model.** Products, users, transactions and a 200,000
session subset. A transaction is always read as a whole and its line items
have no life of their own, so they are embedded. Each user carries an embedded
purchase summary computed at load time, which turns "show me this customer"
into a single document read with no joins.

**HBase — wide-column model.** Two tables with row keys designed around the
query, not the data:

| Table | Row key | Why |
|---|---|---|
| `user_sessions` | `user_id # reverse_timestamp` | Reversing the timestamp means a forward scan returns the **newest sessions first**, so "the last 10 sessions" stops after 10 rows instead of reading a user's whole history and sorting it |
| `product_daily` | `product_id # yyyy-mm-dd` | Date **forward** here, because the question is a date range. Metrics are sparse cells: a product with no sales that day stores nothing |

**Spark — distributed processing.** Everything that needs the full 2 million
sessions, plus every join across sources. Neither MongoDB nor HBase can join
across stores, and HBase cannot aggregate at all.

---

## Repository layout

```
src/
  dataset_generator.py        the provided generator, made runnable
  config.py                   shared settings
  mongodb/
    load_mongodb.py           loader + index creation
    aggregations.py           two analytical pipelines
  hbase/
    hbase_schema.txt          table creation commands, with justification
    load_hbase.py             loader for both tables
    query_hbase.py            four queries
  spark/
    to_jsonl.py               JSON arrays -> JSON Lines
    spark_session.py          one place that configures Spark
    batch_clean.py            cleaning and normalisation -> Parquet
    analytics_batch.py        product affinity + cohort analysis
    spark_sql.py              five Spark SQL queries
  integration/
    clv_analysis.py           CLV across MongoDB + HBase
  viz/
    make_figures.py           seven figures
figures/                      PNG output
results/                      JSON output from every job
```

---

## Reproducing the results

### 1. Prerequisites

```bash
brew install mongodb-community hbase
pip install faker pandas pymongo happybase pyspark matplotlib numpy
```

### 2. Start the databases

```bash
brew services start mongodb-community
start-hbase.sh
hbase thrift start -p 9090 &
```

### 3. Generate the dataset

```bash
python src/dataset_generator.py            # full scale, ~4 minutes, 3.9 GB
SCALE=0.01 python src/dataset_generator.py # 1% sample for development
```

### 4. Run the pipeline

```bash
python src/spark/to_jsonl.py               # convert to splittable JSON Lines
python src/spark/batch_clean.py            # clean and normalise -> Parquet

python src/mongodb/load_mongodb.py         # load MongoDB
python src/mongodb/aggregations.py         # two aggregation pipelines

hbase shell src/hbase/hbase_schema.txt     # create the tables
python src/hbase/load_hbase.py             # load both tables
python src/hbase/query_hbase.py            # four queries

python src/spark/spark_sql.py              # five SQL analytics queries
python src/spark/analytics_batch.py        # affinity + cohort analysis

python src/integration/clv_analysis.py     # the integrated query
python src/viz/make_figures.py             # figures
```

**Note on `DATA_DIR`.** Generated data defaults to `~/ecommerce-data`,
deliberately outside any cloud-synced folder. OneDrive renames files it thinks
are conflicts while Spark is writing part files, which silently corrupted an
earlier run. Override with `DATA_DIR=/your/path`.

---

## Key findings

1. **The funnel loses 69% between cart and purchase.** 64.9% of sessions add
   something to a cart; only 20.3% buy. That single step is worth more than
   every other optimisation combined.
2. **Device makes no measurable difference** — mobile 20.25%, desktop 20.25%,
   tablet 20.38%. A real platform would show a large mobile penalty; this
   dataset does not, which is a property of the generator, not of e-commerce.
3. **Higher-value customers buy more often *and* spend more per order.**
   Average order value rises from 777 in the lowest tier to 1,039 in the
   highest — the two effects compound.
4. **Revenue is evenly spread across all 25 categories** (top category 4.64%).
   There is no long tail, which again reflects uniform random generation.
5. **Product affinity is not reliably measurable in this data.** 14,679 pairs
   clear a minimum co-occurrence of 2, but the maximum co-occurrence anywhere
   is 5. Lift values above 1,000 are arithmetic artefacts of sparsity, not
   signal. Reported with both rankings and that caveat.

---

## Honest limitations

- The data is **synthetic and uniformly random**. Cohort retention sits near
  100%, every user is a frequent buyer, and no product is more popular than
  another. Several analyses are therefore demonstrations of method rather
  than discoveries about behaviour.
- **CLV is 5.5× actual spend to date** because the model assumes a 20+ month
  lifetime while the data covers only 90 days. The ratio, not the absolute
  figure, is the usable output.
- HBase runs **standalone on one node**, so region splitting, compaction
  behaviour and real distribution are not demonstrated.
- Spark runs **`local[4]`** on one laptop. The code is unchanged on a cluster,
  but no cluster behaviour is shown.
