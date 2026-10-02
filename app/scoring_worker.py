"""Commit input only after the score (or invalid-message DLQ) is delivered."""
import json
import logging
from app.inference import Scorer
from app.kafka_utils import consumer, producer, publish
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

def main():
    scorer, c, p = Scorer(), consumer("transactions", "fraud-scorer-v1"), producer()
    logging.info("CPU model loaded; threshold=%.4f", scorer.threshold)
    try:
        while True:
            msg = c.poll(1)
            if msg is None:
                continue
            if msg.error():
                raise RuntimeError(msg.error())
            try:
                result = scorer.score(json.loads(msg.value()))
            except (ValueError, TypeError, KeyError, AttributeError, UnicodeDecodeError) as exc:
                publish(p, "dead-letter", {"source_topic": msg.topic(), "partition": msg.partition(), "offset": msg.offset(), "error": str(exc), "payload": msg.value().decode(errors="replace")}, str(msg.offset()))
                logging.warning("Invalid transaction at offset %s: %s", msg.offset(), exc)
            else:
                publish(p, "scores", result, result["transaction_id"])
                logging.info("Scored %s: %.4f", result["transaction_id"], result["score"])
            c.commit(message=msg, asynchronous=False)
    finally:
        c.close()

if __name__ == "__main__":
    main()
