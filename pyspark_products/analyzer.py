from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col


def analyze_product_categories(
    spark: SparkSession,
    products_df: DataFrame,
    categories_df: DataFrame,
    relations_df: DataFrame
) -> tuple[DataFrame, DataFrame]:
    """
    Возвращает:
    1. пары "Имя продукта — Имя категории"
    2. продукты без категорий
    """
    # Продукты с категориями
    joined_df = relations_df \
        .join(products_df, relations_df.product_id == products_df.id, how="left") \
        .join(categories_df, relations_df.category_id == categories_df.id, how="left") \
        .select(products_df["name"].alias("product_name"),
                categories_df["name"].alias("category_name"))

    # Продукты без категорий
    products_without_categories = products_df \
        .join(relations_df, products_df.id == relations_df.product_id, how="left_anti") \
        .select(col("name").alias("product_name"))

    return joined_df, products_without_categories
