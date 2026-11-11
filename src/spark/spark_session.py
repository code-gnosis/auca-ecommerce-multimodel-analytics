"""One place that builds the SparkSession, so every job is configured alike."""
import os
import sys

from pyspark.sql import SparkSession

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def build(app_name):
    """
    Local Spark tuned for this laptop and this dataset.

    The raw session files are JSON ARRAYS, not JSON Lines, so Spark must read
    each file with multiLine=true. A multiLine file cannot be split: one file
    becomes one task holding the whole parsed array. With 188 MB files that
    needs real heap, hence the 10g driver.

    This is exactly why the first job converts everything to Parquet. Parquet
    is columnar, compressed and splittable, so every later job reads a fraction
    of the bytes and parallelises properly.
    """
    return (SparkSession.builder
            .appName(app_name)
            .master("local[4]")
            .config("spark.driver.memory", "10g")
            .config("spark.driver.maxResultSize", "2g")
            # 200 is the default and far too many for this data: the scheduling
            # costs more than the work. 8 is 2x the cores, so a core that
            # finishes early picks up more instead of idling.
            .config("spark.sql.shuffle.partitions", "8")
            .config("spark.sql.adaptive.enabled", "true")
            .getOrCreate())
