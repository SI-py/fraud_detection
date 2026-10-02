import json
import logging
import math
import psycopg
from app.config import DATABASE_URL
from app.kafka_utils import consumer, producer, publish
logging.basicConfig(level=logging.INFO)

def validate_result(r):
    if not isinstance(r, dict) or set(r) != {"transaction_id", "score", "fraud_flag"}:
        raise ValueError("Expected exactly transaction_id, score, fraud_flag")
    if not isinstance(r["transaction_id"], str) or not 0 < len(r["transaction_id"].strip()) <= 200:
        raise ValueError("Invalid transaction_id")
    if type(r["score"]) not in (int, float) or not math.isfinite(r["score"]) or not 0 <= r["score"] <= 1 or type(r["fraud_flag"]) is not int or r["fraud_flag"] not in (0, 1):
        raise ValueError("Invalid score or flag")
    return r

def main():
    c, p = consumer("scores", "fraud-storage-v1"), producer()
    try:
        with psycopg.connect(DATABASE_URL) as conn:
            while True:
                msg = c.poll(1)
                if msg is None:
                    continue
                if msg.error():
                    raise RuntimeError(msg.error())
                try:
                    r = validate_result(json.loads(msg.value()))
                except (ValueError, TypeError, UnicodeDecodeError) as exc:
                    publish(p, "dead-letter", {"source_topic": "scores", "offset": msg.offset(), "error": str(exc)}, str(msg.offset()))
                else:
                    with conn.transaction():
                        conn.execute("INSERT INTO transaction_scores (transaction_id, score, fraud_flag) VALUES (%s, %s, %s) ON CONFLICT (transaction_id) DO NOTHING", (r["transaction_id"], r["score"], r["fraud_flag"]))
                    logging.info("Stored %s", r["transaction_id"])
                c.commit(message=msg, asynchronous=False)
    finally:
        c.close()

if __name__ == "__main__":
    main()
