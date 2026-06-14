import paho.mqtt.client as mqtt
import json
import time
import random
import argparse

def run_simulator(host, port, device_id, count, interval, qos):
    client = mqtt.Client()
    client.connect(host, port, 60)
    client.loop_start()
    
    topic = f"edge/sensors/{device_id}/telemetry"
    
    print(f"开始发送传感器数据...")
    print(f"   Broker: {host}:{port}")
    print(f"   设备ID: {device_id}")
    print(f"   主题: {topic}")
    print(f"   数量: {count}")
    print(f"   间隔: {interval}秒")
    print(f"   QoS: {qos}")
    print("-" * 50)
    
    for i in range(count):
        data = {
            "device_id": device_id,
            "seq": i + 1,
            "temperature": round(random.uniform(15.0, 35.0), 1),
            "humidity": round(random.uniform(40.0, 80.0), 1),
            "voltage": round(random.uniform(4.5, 5.5), 2),
            "timestamp": time.time()
        }
        
        payload = json.dumps(data)
        result = client.publish(topic, payload, qos=qos)
        
        if result.rc == mqtt.MQTT_ERR_SUCCESS:
            print(f"[{i+1}] Published: {payload}")
        else:
            print(f"[{i+1}] Publish failed with code: {result.rc}")
        
        time.sleep(interval)
    
    client.loop_stop()
    client.disconnect()
    print("-" * 50)
    print("数据发送完成！")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=1883)
    parser.add_argument("--device-id", default="edge-device-01")
    parser.add_argument("--count", type=int, default=10)
    parser.add_argument("--interval", type=float, default=1)
    parser.add_argument("--qos", type=int, default=1)
    args = parser.parse_args()
    
    run_simulator(args.host, args.port, args.device_id, args.count, args.interval, args.qos)