import paho.mqtt.client as mqtt
import redis
import json
import os
import time
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# 配置
MQTT_HOST = os.environ.get('MQTT_HOST', 'mqtt-broker.mqtt.svc.cluster.local')
MQTT_PORT = int(os.environ.get('MQTT_PORT', 1883))
MQTT_TOPIC = os.environ.get('MQTT_TOPIC', 'edge/sensors/#')

REDIS_HOST = os.environ.get('REDIS_HOST', 'redis-svc.default.svc.cluster.local')
REDIS_PORT = int(os.environ.get('REDIS_PORT', 6379))
REDIS_PASSWORD = os.environ.get('REDIS_PASSWORD', 'redis2026')

# 连接 Redis
try:
    r = redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        password=REDIS_PASSWORD if REDIS_PASSWORD else None,
        decode_responses=True
    )
    r.ping()
    logger.info("Connected to Redis")
except Exception as e:
    logger.error(f"Redis connection failed: {e}")
    raise

def on_message(client, userdata, msg):
    try:
        payload = msg.payload.decode('utf-8')
        data = json.loads(payload)
        
        topic_parts = msg.topic.split('/')
        device_id = topic_parts[2] if len(topic_parts) > 2 else "unknown"
        
        # 存储到 Redis Stream
        stream_key = "edge:sensor:stream"
        r.xadd(stream_key, {
            'topic': msg.topic,
            'device_id': device_id,
            'payload': payload,
            'received_at': time.time(),
            'qos': msg.qos
        })
        
        # 更新设备最新状态
        r.set(f"device:latest:{device_id}", payload)
        
        logger.info(f"Stored: device={device_id}, topic={msg.topic}")
        
    except Exception as e:
        logger.error(f"Error processing message: {e}")

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        logger.info(f"Connected to MQTT Broker at {MQTT_HOST}:{MQTT_PORT}")
        client.subscribe(MQTT_TOPIC)
        logger.info(f"Subscribed to {MQTT_TOPIC}")
    else:
        logger.error(f"MQTT connection failed with code {rc}")

# 启动 MQTT 客户端
client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message
client.connect(MQTT_HOST, MQTT_PORT, 60)

logger.info("MQTT-to-Redis Gateway started")
client.loop_forever()