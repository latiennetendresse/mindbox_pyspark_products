import pytest
from pyspark.sql import SparkSession
from pyspark_products.analyzer import analyze_product_categories


@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder \
        .appName("TestProductCategoryAnalyzer") \
        .master("local[*]") \
        .getOrCreate()


@pytest.fixture
def test_data(spark):
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
        (1, 1),   # Apple -> Fruits
        (2, 1),   # Orange -> Fruits
        (3, 3)    # Milk -> Dairy
        # Cucumber без категории
    ], ["product_id", "category_id"])

    return products_df, categories_df, relations_df


def test_analyze_product_categories(spark, test_data):
    products_df, categories_df, relations_df = test_data

    joined, without = analyze_product_categories(spark, products_df, categories_df, relations_df)

    # Проверим, что все пары продуктов с категориями найдены
    result_pairs = {(row["product_name"], row["category_name"]) for row in joined.collect()}
    expected_pairs = {("Apple", "Fruits"), ("Orange", "Fruits"), ("Milk", "Dairy")}

    assert result_pairs == expected_pairs

    # Проверим, что Cucumber попал в список без категорий
    result_without = {row["product_name"] for row in without.collect()}
    assert result_without == {"Cucumber"}
