import json
from confluent_kafka import Producer, Consumer
from app.config import BROKERS

def consumer(topic, group):
    c = Consumer({"bootstrap.servers": BROKERS, "group.id": group, "auto.offset.reset": "earliest", "enable.auto.commit": False})
    c.subscribe([topic])
    return c

def producer():
    return Producer({"bootstrap.servers": BROKERS, "enable.idempotence": True, "acks": "all"})

def publish(p, topic, payload, key):
    errors = []
    p.produce(topic, key=key, value=json.dumps(payload, ensure_ascii=False, allow_nan=False), on_delivery=lambda err, msg: errors.append(err) if err else None)
    pending = p.flush(30)
    if errors or pending:
        raise RuntimeError(f"Kafka delivery failed: {errors}; pending={pending}")
