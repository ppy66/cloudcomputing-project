import pandas as pd
import time

print("=" * 80)
print("本地 Pandas 性能测试")
print("=" * 80)

# 读取数据
print("\n正在读取数据...")
start_time = time.time()
df = pd.read_csv("douban_movies.csv")
read_time = time.time() - start_time
print(f"数据读取完成: {len(df)} 行, {len(df.columns)} 列")
print(f"读取时间: {read_time:.4f} 秒")

# 数据清洗
print("\n正在清洗数据...")
df = df[df['rating_score'].notna()]
df['rating_score'] = pd.to_numeric(df['rating_score'], errors='coerce')
df = df[df['rating_score'] > 0]
df['rating_count'] = pd.to_numeric(df['rating_count'], errors='coerce').fillna(0)

print(f"清洗后数据: {len(df)} 行")

# 执行查询：高分电影 Top 15
print("\n正在执行查询...")
query_start = time.time()
result = df.nlargest(15, 'rating_score')[
    ['title', 'year', 'rating_score', 'rating_count', 'genres']
]
query_time = time.time() - query_start

print(f"\n查询结果:")
print(result.to_string(index=False, max_colwidth=35))

# 总时间
total_time = time.time() - start_time

print("\n" + "=" * 80)
print("Pandas 性能测试结果")
print("=" * 80)
print(f"总执行时间: {total_time:.4f} 秒")
print(f"其中 - 数据读取: {read_time:.4f} 秒")
print(f"     - 查询执行: {query_time:.4f} 秒")
print("=" * 80)

# 保存结果到文件
with open("pandas_time.txt", "w") as f:
    f.write(str(total_time))

print("\n✅ 结果已保存到 pandas_time.txt")