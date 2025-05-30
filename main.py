from pyspark_products.analyzer import analyze_product_categories
from pyspark_products.utils import get_spark, get_demo_data


def main():
    spark = get_spark("DemoApp")
    products_df, categories_df, relations_df = get_demo_data(spark)

    pairs_df, no_category_df = analyze_product_categories(spark, products_df, categories_df, relations_df)

    print("📦 Продукты с категориями:")
    pairs_df.show()

    print("🚫 Продукты без категорий:")
    no_category_df.show()


if __name__ == "__main__":
    main()
