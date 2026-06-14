from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import time
import os

print("=" * 80)
print("Spark 集群性能测试")
print("=" * 80)

# 获取 executor 数量（从环境变量或配置读取）
executor_instances = os.environ.get('SPARK_EXECUTOR_INSTANCES', '1')
print(f"\nExecutor 数量: {executor_instances}")

# 初始化 SparkSession
spark = SparkSession.builder \
    .appName(f"PerformanceTest_{executor_instances}") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

print(f"Spark Version: {spark.version}")

# 数据路径
DATA_PATH = "s3a://k8s-test-e174/douban_movies.csv"
print(f"\n数据路径: {DATA_PATH}")

# 开始计时
start_time = time.time()

print("\n正在读取数据...")
df = spark.read.option("header", True).option("inferSchema", True).csv(DATA_PATH)

print("正在执行查询...")
result = df.filter(col("rating_score") > 0) \
    .orderBy(col("rating_score").desc()) \
    .select("title", "year", "rating_score", "rating_count", "genres") \
    .limit(15)

# 触发计算
result_count = result.count()
execution_time = time.time() - start_time

print(f"\n查询完成，返回 {result_count} 条记录")

print("\n查询结果:")
result.show(truncate=40)

# 输出结果
print("\n" + "=" * 80)
print("Spark 性能测试结果")
print("=" * 80)
print(f"Executor 数量: {executor_instances}")
print(f"总执行时间: {execution_time:.4f} 秒")
print("=" * 80)

# 输出便于解析的格式
print(f"\nPERFORMANCE_RESULT: executor={executor_instances}, time={execution_time:.4f}")

# 停止 Spark
spark.stop()