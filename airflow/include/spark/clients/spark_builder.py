import os
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

class SparkBuilder:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance.spark = SparkSession.builder \
                .appName("pipeline-delta-s3") \
                .config("spark.jars.packages",
                        "io.delta:delta-spark_2.13:4.1.0,"
                        "org.apache.hadoop:hadoop-aws:3.4.1,"
                        "com.amazonaws:aws-java-sdk-bundle:1.12.262")\
                .config("spark.sql.extensions",
                        "io.delta.sql.DeltaSparkSessionExtension") \
                .config("spark.sql.catalog.spark_catalog",
                        "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
                .config("spark.hadoop.fs.s3a.access.key", os.environ["ACCESS_KEY"]) \
                .config("spark.hadoop.fs.s3a.secret.key", os.environ["SECRET_KEY"]) \
                .config("spark.hadoop.fs.s3a.impl",
                        "org.apache.hadoop.fs.s3a.S3AFileSystem") \
                .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
                .getOrCreate()
        return cls._instance

    def get_session(self):
        return self.spark