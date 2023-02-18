"""Converts a CSV file with header to parquet using ApacheSpark."""
import logging
import os
import re
import sys

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

from config import Config

# source path and file name (default: data.csv)
data_csv = Config.data_csv

# destination path and parquet file name (default: data.parquet)
output_data_parquet = Config.data_parquet

# url of master (default: local mode)
master = Config.master

parameters = list(
    map(
        lambda s: re.sub("$", '"', s),
        map(
            lambda s: s.replace("=", '="'),
            filter(
                lambda s: s.find("=") > -1
                and bool(re.match(r"[A-Za-z0-9_]*=[.\/A-Za-z0-9]*", s)),
                sys.argv,
            ),
        ),
    )
)

for parameter in parameters:
    logging.warning("Parameter: " + parameter)
    exec(parameter)

skip = False

if os.path.exists(output_data_parquet):
    skip = True

os.environ[
    "JAVA_HOME"
] = "/Library/Java/JavaVirtualMachines/adoptopenjdk-8.jdk/Contents/Home"

if not skip:
    sc = SparkContext.getOrCreate(SparkConf().setMaster(master))
    spark = SparkSession.builder.getOrCreate()

if not skip:
    df = spark.read.option("header", "true").csv(data_csv)

if not skip:
    df.write.parquet(output_data_parquet)
