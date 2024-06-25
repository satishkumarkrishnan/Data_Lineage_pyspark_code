from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("PySpark OpenLineage Example") \
        .config("openlineage.url", "http://localhost:5000")  \
        .config("openlineage.backend", "io.openlineage.spark.agent.backends.ConsoleLoggingBackend") \
        .config("openlineage.namespace", "test") \
        .getOrCreate()

    # Example PySpark job
    df = spark.read.text("test.csv")
    df.show()

    # Perform some transformation
    third_column = df.columns[2]
    age = 40
    filtered_df = df.filter(df["age"] >= 40)
    filtered_df.show()

    # Write data
    filtered_df.write.mode('overwrite').csv("output.csv")

    spark.stop()