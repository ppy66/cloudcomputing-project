from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("WordCount").getOrCreate()

data = [
    "hello spark", "hello kubernetes", "spark is great", "hello world",
    "cloud computing", "spark operator", "kubernetes cce", "hello hello",
    "pyspark on k8s", "spark wordcount test"
]

lines = spark.sparkContext.parallelize(data)

word_counts = (
    lines.flatMap(lambda line: line.split())
    .map(lambda word: (word, 1))
    .reduceByKey(lambda a, b: a + b)
    .sortBy(lambda x: x[1], ascending=False)
)

print("=" * 50)
print("WordCount Results:")
for word, count in word_counts.collect():
    print(f"{word}: {count}")
print("=" * 50)

spark.stop()