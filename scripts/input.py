"""Pulls the HMP accelerometer sensor data classification data set"""
import fnmatch
import os
import random
import shutil
from pathlib import Path

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from pyspark.sql.types import IntegerType, StructField, StructType

from config import Config


def main():
    """Main function"""

    # path and file name for output (default: data.csv)
    data_csv = Config.data_csv
    # url of master (default: local mode)
    master = Config.master
    # sample on input data to increase processing speed 0..1 (default: 1.0)
    sample = Config.sample

    os.environ[
        "JAVA_HOME"
    ] = "/Library/Java/JavaVirtualMachines/adoptopenjdk-8.jdk/Contents/Home"

    sc = SparkContext.getOrCreate(SparkConf().setMaster(master))

    spark = SparkSession.builder.getOrCreate()

    schema = StructType(
        [
            StructField("x", IntegerType(), True),
            StructField("y", IntegerType(), True),
            StructField("z", IntegerType(), True),
        ]
    )

    data_set = Config.dataset

    # filter list for all folders containing data (folders that don't start with .)
    file_list_filtered = [
        s
        for s in os.listdir(data_set)
        if os.path.isdir(os.path.join(data_set, s)) & ~fnmatch.fnmatch(s, ".*")
    ]

    # create pandas dataframe for all the data

    df = None

    for category in file_list_filtered:
        data_files = os.listdir(f"{data_set}/{category}")

        # create a temporary pandas data frame for each data file
        for data_file in data_files:
            if sample < 1.0:
                if random.random() > sample:
                    print("Skipping: " + data_file)
                    continue
            print(data_file)
            temp_df = (
                spark.read.option("header", "false")
                .option("delimiter", " ")
                .csv(f"{data_set}/{category}/{data_file}", schema=schema)
            )

            # create a column called "source" storing the current CSV file
            temp_df = temp_df.withColumn("source", lit(data_file))

            # create a column called "class" storing the current data folder
            temp_df = temp_df.withColumn("class", lit(category))

            if df is None:
                df = temp_df
            else:
                df = df.union(temp_df)

    if Path(data_csv).exists():
        shutil.rmtree(data_csv)

    df.write.option("header", "true").csv(data_csv)


if __name__ == "__main__":
    main()
