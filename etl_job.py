from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder \
        .appName("Simple ETL with PySpark") \
        .getOrCreate()

    # Read CSV file
    df = spark.read.csv("sample_data.csv", header=True, inferSchema=True)
    print("Original Data:")
    df.show()

    # Simple transformation: filter rows with age > 30
    transformed_df = df.filter(df.age > 30)
    print("Filtered Data:")
    transformed_df.show()

    # Save result to output directory
    transformed_df.write.mode("overwrite").csv("output_data")

    spark.stop()

if __name__ == "__main__":
    main()
