from pyspark.sql import SparkSession
import json
import os

if __name__ == "__main__":
    # Create Spark session
    spark = SparkSession.builder.appName("walmart_analysis").getOrCreate()

    # Load dataset
    path_walmart = "Walmart_customer_purchases.csv"
    df_walmart = spark.read.csv(path_walmart, header=True, inferSchema=True)

    # Ensure results directory exists
    os.makedirs("results", exist_ok=True)

    # Save raw data
    df_walmart.write.mode("overwrite").json("results/raw_walmart_data.json")

    # Create temp view
    df_walmart.createOrReplaceTempView("walmart")

    # Top 10 most expensive purchases
    query_expensive = """
        SELECT Customer_ID, Product_Name, Purchase_Amount
        FROM walmart
        WHERE Purchase_Amount IS NOT NULL
        ORDER BY Purchase_Amount DESC
        LIMIT 10
    """
    df_top_expensive = spark.sql(query_expensive)
    df_top_expensive.show()
    with open("results/top_expensive_purchases.json", "w") as f:
        json.dump(df_top_expensive.toJSON().collect(), f)

    # Top 10 most purchased cities by total amount
    query_city_spenders = """
        SELECT City, SUM(Purchase_Amount) AS Total_Spent
        FROM walmart
        WHERE Purchase_Amount IS NOT NULL
        GROUP BY City
        ORDER BY Total_Spent DESC
        LIMIT 10
    """
    df_top_cities = spark.sql(query_city_spenders)
    df_top_cities.show()
    with open("results/top_spending_cities.json", "w") as f:
        json.dump(df_top_cities.toJSON().collect(), f)

    # Top 10 highest-rated product entries
    query_rated = """
        SELECT Product_Name, Rating
        FROM walmart
        WHERE Rating IS NOT NULL
        ORDER BY Rating DESC
        LIMIT 10
    """
    df_top_rated = spark.sql(query_rated)
    df_top_rated.show()
    with open("results/top_rated_products.json", "w") as f:
        json.dump(df_top_rated.toJSON().collect(), f)

    # Clean shutdown
    spark.stop()