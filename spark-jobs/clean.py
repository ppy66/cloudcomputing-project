from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, when, isnan, count, avg, stddev, min, max

# 初始化 SparkSession
spark = SparkSession.builder \
    .appName("DoubanMovieDataCleaning") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .getOrCreate()

print("=" * 70)
print("A-1: 豆瓣电影数据清洗")
print("=" * 70)

# 从华为云 OBS 读取数据
df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("multiLine", "true") \
    .csv("s3a://k8s-test-e174/douban_movies.csv")

print("\n[Step 2] Schema 信息（数据类型）:")
df.printSchema()

print("\n[Step 3] 前5行数据:")
df.show(10, truncate=50)

print("\n[Step 4] 缺失值统计:")
total_count = df.count()
print(f"    {'字段名':<20} {'缺失数量':>12} {'缺失比例':>12}")

for col_name in df.columns:
    null_count = df.filter(col(col_name).isNull() | (trim(col(col_name)) == "")).count()
    ratio = null_count / total_count * 100
    print(f"    {col_name:<20} {null_count:>12,} {ratio:>11.2f}%")

print("\n[Step 5] 缺失值处理策略:")

# 策略1: 删除评分缺失的记录
print("    🔧 策略1: rating_score 是核心分析指标，删除缺失值")
before_count = df.count()
df_clean = df.dropna(subset=["rating_score"])
after_count = df_clean.count()
print(f"       删除前: {before_count:,} 行 → 删除后: {after_count:,} 行 (移除：{before_count - after_count:,} 行)")

# 策略2: 填充评分人数
print("    🔧 策略2: rating_count评分人数缺失的填充为 0")
df_clean = df_clean.fillna({"rating_count": 0})

# 策略3: 填充年份
print("    🔧 策略3: year年份缺失的填充为 0")
df_clean = df_clean.fillna({"year": 0})

# 策略4: 填充类型
print("    🔧 策略4: genres电影类型缺失的填充为 '未知类型'")
df_clean = df_clean.fillna({"genres": "未知类型"})

# 策略5: 填充国家
print("    🔧 策略5: countries国家地区缺失的填充为 '未知地区'")
df_clean = df_clean.fillna({"countries": "未知地区"})

# 策略6: 填充导演
print("    🔧 策略6: directors 导演缺失的填充为 '未知导演'")
df_clean = df_clean.fillna({"directors": "未知导演"})

print("\n[Step 6] 清洗前后行数对比:")
print(f"    📊 清洗前: {total_count:,} 行")
print(f"    📊 清洗后: {df_clean.count():,} 行")
print(f"    📊 总移除: {total_count - df_clean.count():,} 行")

print("\n[Step 7] 清洗后数值字段统计:")
print("    " + "-" * 60)
df_clean.select(
    min("year").alias("year_min"),
    max("year").alias("year_max"),
    avg("year").alias("year_avg"),
    min("rating_score").alias("score_min"),
    max("rating_score").alias("score_max"),
    avg("rating_score").alias("score_avg"),
    min("rating_count").alias("count_min"),
    max("rating_count").alias("count_max"),
    avg("rating_count").alias("count_avg")
).show()

print("\n[Step 8] 清洗后数据样例 (前10行):")
df_clean.show(20, truncate=40)

spark.stop()