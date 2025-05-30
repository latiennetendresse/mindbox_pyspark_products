import os
import sys
from pyspark.sql import SparkSession

def configure_pyspark_env():

    # Возникла проблема, при которой PySpark не знал, какой Python использовать
    # Windows использую, потому что другой pet-проект требует либу которая только на винде корректно запускается,
    # а так основная рабочая ОС - ubuntu

    if sys.platform == "win32":
        os.environ["PYSPARK_PYTHON"] = os.path.abspath(".venv/Scripts/python.exe")


def get_spark(app_name="ProductCategoryAnalyzer") -> SparkSession:
    configure_pyspark_env()
    return SparkSession.builder \
        .appName(app_name) \
        .master("local[*]") \
        .getOrCreate()


def get_demo_data(spark):
    products_df = spark.createDataFrame([
        (1, "Apple"),
        (2, "Orange"),
        (3, "Milk"),
        (4, "Cucumber")
    ], ["id", "name"])

    categories_df = spark.createDataFrame([
        (1, "Fruits"),
        (2, "Vegetables"),
        (3, "Dairy")
    ], ["id", "name"])

    relations_df = spark.createDataFrame([
        (1, 1),  # Apple -> Fruits
        (2, 1),  # Orange -> Fruits
        (3, 3)   # Milk -> Dairy
    ], ["product_id", "category_id"])

    return products_df, categories_df, relations_df
