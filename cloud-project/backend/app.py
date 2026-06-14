from flask import Flask, jsonify
import redis
import os
import socket
import time

app = Flask(__name__)

# 连接 Redis（从环境变量读取配置）
redis_client = redis.Redis(
    host=os.getenv('REDIS_HOST', 'localhost'),
    port=int(os.getenv('REDIS_PORT', 6379)),
    password=os.getenv('REDIS_PASSWORD', None),
    decode_responses=True
)

@app.route('/api/ping')
def ping():
    """健康检查接口"""
    return jsonify({"status": "ok"})

@app.route('/api/visits')
def visits():
    """访问计数接口（演示三层通信）"""
    try:
        count = redis_client.incr('visits')
        return jsonify({
            "visits": count,
            "served_by": socket.gethostname()
        })
    except Exception as e:
        return jsonify({
            "visits": 0,
            "served_by": socket.gethostname(),
            "error": str(e)
        })

@app.route('/api/info')
def info():
    """服务信息接口"""
    return jsonify({
        "service": "backend-api",
        "version": "v1",
        "hostname": socket.gethostname(),
        "redis_host": os.getenv('REDIS_HOST', 'not set')
    })

@app.route('/api/compute')
def compute():
    """CPU 压测接口（用于 HPA 弹性伸缩测试）"""
    result = 0
    for i in range(1000000):
        result += i * i
    return jsonify({
        "result": result,
        "served_by": socket.gethostname()
    })

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)