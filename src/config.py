"""Settings shared by every script in this project."""
import os

# NOT inside OneDrive. OneDrive syncs file-by-file and renames anything it
# thinks is a conflict, which it does constantly while Spark writes thousands
# of part files - it silently turned data/parquet/users into "users 2" and
# broke every downstream read. Working data lives outside the synced folder.
DATA_DIR = os.environ.get("DATA_DIR", os.path.expanduser("~/ecommerce-data"))

# MongoDB
MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = "ecommerce"

# HBase (Thrift gateway)
HBASE_HOST = os.environ.get("HBASE_HOST", "localhost")
HBASE_PORT = int(os.environ.get("HBASE_PORT", "9090"))

# How much of the dataset each store receives.
# Spark reads EVERYTHING; MongoDB and HBase take a representative subset,
# which is what the brief asks for and what a single laptop can hold.
MONGO_SESSION_LIMIT = int(os.environ.get("MONGO_SESSION_LIMIT", "200000"))
HBASE_SESSION_LIMIT = int(os.environ.get("HBASE_SESSION_LIMIT", "100000"))

RESULTS_DIR = "results"
FIGURES_DIR = "figures"


def local_uri(path):
    """
    Absolute file:// URI for Spark.

    WHY: this machine has HADOOP_CONF_DIR set, so Spark's default filesystem is
    hdfs://localhost:8020. A relative path like "data/users.json" is then looked
    up in HDFS and fails with ConnectionRefused. Naming the scheme explicitly
    removes the ambiguity.
    """
    import os
    return "file://" + os.path.abspath(path)
