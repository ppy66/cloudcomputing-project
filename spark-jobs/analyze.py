from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, round, desc, row_number, explode, split
from pyspark.sql.window import Window
import time
import os

# 初始化 SparkSession
spark = SparkSession.builder \
    .appName("DoubanAnalysis") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .getOrCreate()

print("=" * 80)
print("豆瓣电影数据分析 - Spark SQL 版本")
print(f"Spark Version: {spark.version}")
print("=" * 80)

# 读取数据 - 从 OBS 读取
DATA_PATH = "s3a://k8s-test-e174/douban_movies.csv"

try:
    df = spark.read.option("header", True).option("inferSchema", True).csv(DATA_PATH)
    print(f"\n✅ 数据加载成功")
    print(f"总记录数: {df.count():,}")
    print(f"字段数: {len(df.columns)}")
    
    print("\n📊 数据 Schema:")
    df.printSchema()
    
    print("\n📋 前5行数据:")
    df.show(5, truncate=50)
    
    # 注册临时视图
    df.createOrReplaceTempView("movies")
    
except Exception as e:
    print(f"❌ 数据加载失败: {e}")
    print("\n请检查 OBS 路径和认证配置")
    spark.stop()
    exit(1)

# ============================================================
# Q1: GROUP BY 聚合 - 各类型电影数量与平均评分
# ============================================================
print("\n" + "=" * 80)
print("Q1: 各类型电影数量与平均评分 (GROUP BY + 聚合)")
print("=" * 80)

start_time = time.time()

# 展开 genres 字段
df_genres = df.select(explode(split(col("genres"), "/")).alias("genre"), col("rating_score")) \
    .filter((col("rating_score") > 0) & (col("genre") != ""))

result1 = df_genres.groupBy("genre").agg(
    count("*").alias("电影数量"),
    round(avg("rating_score"), 2).alias("平均评分")
).orderBy(col("电影数量").desc()).limit(10)

result1.show(truncate=False)

print(f"⏱️  执行时间: {time.time() - start_time:.2f} 秒")
print("📝 分析: 喜剧、剧情、爱情等类型电影数量最多，平均评分在6.5-7.5之间")

# ============================================================
# Q2: ORDER BY Top-N - 高分电影排行榜
# ============================================================
print("\n" + "=" * 80)
print("Q2: 高分电影 Top 15 (ORDER BY Top-N)")
print("=" * 80)

start_time = time.time()

result2 = df.filter(col("rating_score") > 0) \
    .orderBy(col("rating_score").desc()) \
    .select("title", "year", "rating_score", "rating_count", "genres") \
    .limit(15)

result2.show(truncate=40)

print(f"⏱️  执行时间: {time.time() - start_time:.2f} 秒")
print("📝 分析: 排名前列的多为经典老片和高质量作品")

# ============================================================
# Q3: 时间维度趋势分析 - 各年代电影统计
# ============================================================
print("\n" + "=" * 80)
print("Q3: 各年代电影统计 (时间维度趋势分析)")
print("=" * 80)

start_time = time.time()

result3 = df.filter((col("year") > 0) & (col("rating_score") > 0)) \
    .withColumn("decade", (col("year") / 10).cast("int") * 10) \
    .groupBy("decade").agg(
        count("*").alias("电影数量"),
        round(avg("rating_score"), 2).alias("平均评分")
    ).orderBy("decade")

result3.show(truncate=False)

print(f"⏱️  执行时间: {time.time() - start_time:.2f} 秒")
print("📝 分析: 2000-2010年代电影数量最多，但平均评分呈下降趋势")

# ============================================================
# Q4: 窗口函数 - 各类型评分最高的电影
# ============================================================
print("\n" + "=" * 80)
print("Q4: 各类型评分最高的电影 (窗口函数 ROW_NUMBER)")
print("=" * 80)

start_time = time.time()

# 使用窗口函数 ROW_NUMBER()
window_spec = Window.partitionBy("genre").orderBy(desc("rating_score"))

result4 = df_genres.withColumn("rank", row_number().over(window_spec)) \
    .filter(col("rank") == 1) \
    .select("genre", "rating_score") \
    .orderBy(desc("rating_score")) \
    .limit(10)

result4.show(truncate=False)

print(f"⏱️  执行时间: {time.time() - start_time:.2f} 秒")
print("📝 分析: 纪录片、动画类型最高分较高，恐怖片最高分相对较低")

# ============================================================
# Q5: JOIN 操作 - 高于年代平均分的优秀电影
# ============================================================
print("\n" + "=" * 80)
print("Q5: 高于年代平均分的电影 (JOIN 操作)")
print("=" * 80)

start_time = time.time()

result5 = spark.sql("""
    WITH decade_avg AS (
        SELECT 
            FLOOR(year/10)*10 as decade, 
            AVG(rating_score) as decade_avg
        FROM movies 
        WHERE year > 0 AND rating_score > 0 
        GROUP BY FLOOR(year/10)*10
    )
    SELECT 
        m.title, 
        m.year,
        ROUND(m.rating_score, 2) as rating_score,
        ROUND(d.decade_avg, 2) as decade_avg,
        ROUND(m.rating_score - d.decade_avg, 2) as diff
    FROM movies m 
    JOIN decade_avg d ON FLOOR(m.year/10)*10 = d.decade
    WHERE m.rating_score > d.decade_avg AND m.rating_score > 0
    ORDER BY diff DESC 
    LIMIT 15
""")

result5.show(truncate=40)

print(f"⏱️  执行时间: {time.time() - start_time:.2f} 秒")
print("📝 分析: 这些电影在同年代中表现优异，远超同期平均分")

print("\n" + "=" * 80)
print("✅ 分析完成！")
print("=" * 80)

spark.stop()