"""Integration: real Kafka -> inference -> Kafka -> PostgreSQL, plus bad input."""
import json
import time
import uuid
from pathlib import Path
import psycopg
from confluent_kafka import Consumer
from app.config import DATABASE_URL, BROKERS
from app.kafka_utils import producer, publish

def main():
    run_id = 'smoke-' + uuid.uuid4().hex[:12]
    c = Consumer({'bootstrap.servers': BROKERS, 'group.id': run_id, 'auto.offset.reset':'earliest', 'enable.auto.commit':False})
    c.subscribe(['scores','dead-letter'])
    p = producer()
    ids = []
    for label in ['fraud','legitimate']:
        r = json.loads(Path(f'examples/{label}.json').read_text())
        r['transaction_id'] = run_id + '-' + label
        ids.append(r['transaction_id'])
        publish(p, 'transactions', r, r['transaction_id'])
    # Duplicate delivery must create only one row in PostgreSQL.
    publish(p, 'transactions', r, r['transaction_id'])
    publish(p, 'transactions', {'transaction_id': run_id + '-invalid'}, run_id)
    found, invalid = {}, False
    try:
        deadline = time.monotonic() + 90
        while time.monotonic() < deadline and (len(found) < 2 or not invalid):
            msg = c.poll(1)
            if msg is None:
                continue
            if msg.error():
                raise RuntimeError(msg.error())
            result = json.loads(msg.value())
            if msg.topic() == 'scores' and result.get('transaction_id') in ids:
                assert set(result) == {'transaction_id','score','fraud_flag'}
                found[result['transaction_id']] = result
            elif msg.topic() == 'dead-letter' and run_id in result.get('payload',''):
                invalid = True
        assert len(found) == 2, f'Missing scores: {found}'
        assert invalid, 'Invalid input was not routed to dead-letter'
        assert found[ids[0]]['fraud_flag'] == 1
        assert found[ids[1]]['fraud_flag'] == 0
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            with psycopg.connect(DATABASE_URL) as conn:
                rows = conn.execute('SELECT transaction_id, score, fraud_flag FROM transaction_scores WHERE transaction_id = ANY(%s)', (ids,)).fetchall()
            if len(rows) == 2:
                for tx_id, score, flag in rows:
                    assert score == found[tx_id]['score'] and flag == found[tx_id]['fraud_flag']
                break
            time.sleep(.5)
        else:
            raise AssertionError('Scores not stored in PostgreSQL')
        print('PASS: CPU model, Kafka input/output, PostgreSQL, duplicate and dead-letter; ids=' + str(ids))
    finally:
        c.close()

if __name__ == "__main__":
    main()
