import argparse
import json
import uuid
import pandas as pd
from app.preprocessing import preprocess
from app.kafka_utils import producer


def send_frame(frame, run_id=None):
    preprocess(frame)
    frame = frame.copy()
    run_id = run_id or uuid.uuid4().hex[:12]
    if 'transaction_id' in frame:
        if frame.transaction_id.isna().any():
            raise ValueError('transaction_id contains empty values')
        ids = frame.transaction_id.astype(str)
        if ids.str.strip().eq('').any() or ids.str.len().gt(200).any() or ids.duplicated().any():
            raise ValueError('transaction_id must be nonempty, unique and at most 200 characters')
        frame['transaction_id'] = ids
    else:
        frame['transaction_id'] = [f'{run_id}-{i:06d}' for i in range(len(frame))]
    p = producer()
    errors = []
    def delivered(err, message):
        if err:
            errors.append(str(err))
    def drain():
        pending = p.flush(30)
        if errors or pending:
            raise RuntimeError(f'Kafka delivery failed: {errors}; pending={pending}')
    for index, record in enumerate(frame.to_dict(orient='records')):
        p.produce('transactions', key=record['transaction_id'], value=json.dumps(record, ensure_ascii=False, allow_nan=False), on_delivery=delivered)
        p.poll(0)
        if (index + 1) % 500 == 0:
            drain()
    drain()
    return len(frame)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--csv', default='examples/demo.csv')
    parser.add_argument('--limit', type=int, default=100)
    args = parser.parse_args()
    if args.limit < 1:
        parser.error('--limit must be positive')
    print(f'Sent {send_frame(pd.read_csv(args.csv, nrows=args.limit))} transactions')
