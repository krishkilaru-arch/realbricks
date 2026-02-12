from pyspark.sql import SparkSession


def get_spark(app_name: str = "realbricks-pipeline") -> SparkSession:
    return SparkSession.builder.appName(app_name).getOrCreate()

