"""Train Logistic Regression classifier with Apache SparkML"""
import logging
import os
import re
import shutil
import site
import sys

import wget
from pyspark import SparkConf, SparkContext, SQLContext
from pyspark2pmml import PMMLBuilder
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import MinMaxScaler, StringIndexer, VectorAssembler

from config import Config


def main():
    if sys.version[0:3] == "3.9":
        url = (
            "https://github.com/jpmml/jpmml-sparkml/releases/download/1.7.2/"
            "jpmml-sparkml-executable-1.7.2.jar"
        )
        wget.download(url)
        shutil.copy(
            "jpmml-sparkml-executable-1.7.2.jar",
            site.getsitepackages()[0] + "/pyspark/jars/",
        )
    elif sys.version[0:3] == "3.8":
        url = (
            "https://github.com/jpmml/jpmml-sparkml/releases/download/1.7.2/"
            "jpmml-sparkml-executable-1.7.2.jar"
        )
        wget.download(url)
        shutil.copy(
            "jpmml-sparkml-executable-1.7.2.jar",
            site.getsitepackages()[0] + "/pyspark/jars/",
        )
    elif sys.version[0:3] == "3.7":
        url = (
            "https://github.com/jpmml/jpmml-sparkml/releases/download/1.5.12/"
            "jpmml-sparkml-executable-1.5.12.jar"
        )
        wget.download(url)
    elif sys.version[0:3] == "3.6":
        url = (
            "https://github.com/jpmml/jpmml-sparkml/releases/download/1.5.12/"
            "jpmml-sparkml-executable-1.5.12.jar"
        )
        wget.download(url)
    else:
        raise Exception(
            "Currently only python 3.6 , 3.7, 3,8 and 3.9 is supported, in case "
            "you need a different version please open an issue at "
            "https://github.com/IBM/claimed/issues"
        )

    data_parquet = Config.data_parquet  # input file name (parquet)
    master = Config.master  # URL to Spark master
    model_target = Config.model  # model output file name

    input_columns = os.environ.get(
        "input_columns", '["x", "y", "z"]'
    )  # input columns to consider

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
        logging.warning("Parameter: %s", parameter)
        exec(parameter)

    os.environ[
        "JAVA_HOME"
    ] = "/Library/Java/JavaVirtualMachines/adoptopenjdk-8.jdk/Contents/Home"

    conf = SparkConf().setMaster(master)
    # if sys.version[0:3] == '3.6' or sys.version[0:3] == '3.7':
    conf.set("spark.jars", "jpmml-sparkml-executable-1.5.12.jar")

    sc = SparkContext.getOrCreate(conf)
    sqlContext = SQLContext(sc)
    spark = sqlContext.sparkSession

    df = spark.read.parquet(data_parquet)

    # register a corresponding query table
    df.createOrReplaceTempView("df")

    from pyspark.sql.types import DoubleType

    df = df.withColumn("x", df.x.cast(DoubleType()))
    df = df.withColumn("y", df.y.cast(DoubleType()))
    df = df.withColumn("z", df.z.cast(DoubleType()))

    splits = df.randomSplit([0.8, 0.2])
    df_train = splits[0]
    df_test = splits[1]

    indexer = StringIndexer(inputCol="class", outputCol="label")

    vectorAssembler = VectorAssembler(
        inputCols=eval(input_columns), outputCol="features"
    )

    normalizer = MinMaxScaler(inputCol="features", outputCol="features_norm")

    lr = LogisticRegression(maxIter=1000, regParam=2.0, elasticNetParam=1.0)

    pipeline = Pipeline(stages=[indexer, vectorAssembler, normalizer, lr])

    model = pipeline.fit(df_train)

    prediction = model.transform(df_train)

    binEval = (
        MulticlassClassificationEvaluator()
        .setMetricName("accuracy")
        .setPredictionCol("prediction")
        .setLabelCol("label")
    )

    binEval.evaluate(prediction)

    pmmlBuilder = PMMLBuilder(sc, df_train, model)
    pmmlBuilder.buildFile(model_target)


if __name__ == "__main__":
    main()
