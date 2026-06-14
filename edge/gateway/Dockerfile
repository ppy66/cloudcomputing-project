FROM python:3.11-slim

WORKDIR /app

RUN pip install paho-mqtt redis -i https://pypi.tuna.tsinghua.edu.cn/simple

COPY mqtt_redis_gateway.py .

CMD ["python", "mqtt_redis_gateway.py"]