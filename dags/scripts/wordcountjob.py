from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("PythonWordCount") \
    .getOrCreate()

try:
    text = "Hello Spark Hello Python Hello Airflow Hello Docker and Hello Yusuf"
    words = spark.sparkContext.parallelize(text.split(" "))
    wordCounts = words.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)

    print("Starting word count...")
    results = wordCounts.collect()
    print("Word count results:")
    for word, count in results:
        print(f"{word}: {count}")
except Exception as e:
    print(f"Error during word count: {str(e)}")
    raise
finally:
    spark.stop()