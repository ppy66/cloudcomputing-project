# backend/app.py
from flask import Flask, jsonify
import redis
import os
import socket

app = Flask(__name__)

# 从环境变量获取 Redis 配置
redis_host = os.environ.get('REDIS_HOST', 'localhost')
redis_port = os.environ.get('REDIS_PORT', 6379)
redis_password = os.environ.get('REDIS_PASSWORD', '')

# 连接 Redis
try:
    r = redis.Redis(
        host=redis_host,
        port=int(redis_port),
        password=redis_password if redis_password else None,
        decode_responses=True
    )
    r.ping()
    redis_connected = True
except:
    redis_connected = False

@app.route('/')
def hello():
    return jsonify({
        'message': 'Hello from Cloud Computing Course!',
        'hostname': socket.gethostname(),
        'redis_connected': redis_connected
    })

@app.route('/api/ping')
def ping():
    return jsonify({'status': 'ok', 'hostname': socket.gethostname()})

@app.route('/api/count')
def count():
    if redis_connected:
        count = r.incr('visit_count')
        return jsonify({'visit_count': count})
    return jsonify({'error': 'Redis not connected'})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)