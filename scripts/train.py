"""Offline CPU training. Never invoked by docker compose."""
import argparse
import json
from pathlib import Path
import numpy as np
import pandas as pd
from catboost import CatBoostClassifier
from sklearn.metrics import average_precision_score, roc_auc_score, precision_recall_curve, precision_score, recall_score, f1_score
from app.preprocessing import preprocess, CATEGORICAL


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--data', required=True)
    parser.add_argument('--iterations', type=int, default=650)
    args = parser.parse_args()
    raw = pd.read_csv(args.data)
    raw = raw.sort_values('transaction_time').reset_index(drop=True)
    n = len(raw)
    a, b = int(n * .7), int(n * .85)
    x = preprocess(raw)
    y = raw.target.astype(int)
    model = CatBoostClassifier(iterations=args.iterations, depth=6, learning_rate=.08, loss_function='Logloss', eval_metric='AUC', task_type='CPU', thread_count=6, random_seed=42, verbose=100, allow_writing_files=False)
    model.fit(x.iloc[:a], y.iloc[:a], cat_features=CATEGORICAL, eval_set=(x.iloc[a:b], y.iloc[a:b]), early_stopping_rounds=80)
    val_prob = model.predict_proba(x.iloc[a:b])[:, 1]
    precision, recall, thresholds = precision_recall_curve(y.iloc[a:b], val_prob)
    f1 = 2 * precision[:-1] * recall[:-1] / np.maximum(precision[:-1] + recall[:-1], 1e-12)
    threshold = float(thresholds[np.argmax(f1)])
    prob = model.predict_proba(x.iloc[b:])[:, 1]
    flags = prob >= threshold
    metadata = {'threshold': threshold, 'split': 'chronological 70% train / 15% validation / 15% holdout', 'rows': n, 'train_rows': a, 'validation_rows': b-a, 'holdout_rows': n-b, 'fraud_rate': float(y.mean()), 'tree_count': model.tree_count_, 'features': list(x.columns), 'categorical_features': CATEGORICAL, 'holdout': {'roc_auc': float(roc_auc_score(y.iloc[b:], prob)), 'pr_auc': float(average_precision_score(y.iloc[b:], prob)), 'precision': float(precision_score(y.iloc[b:], flags)), 'recall': float(recall_score(y.iloc[b:], flags)), 'f1': float(f1_score(y.iloc[b:], flags))}, 'feature_importance': dict(sorted(zip(x.columns, map(float, model.feature_importances_)), key=lambda kv: -kv[1])), 'seed': 42}
    Path('models').mkdir(exist_ok=True)
    model.save_model('models/fraud.cbm')
    Path('models/metadata.json').write_text(json.dumps(metadata, indent=2))
    holdout = raw.iloc[b:].copy()
    holdout['probability'] = prob
    for label, row in [('fraud', holdout.loc[holdout.target.eq(1)].sort_values('probability', ascending=False).iloc[0]), ('legitimate', holdout.loc[holdout.target.eq(0)].sort_values('probability').iloc[0])]:
        record = row.drop(['target', 'probability']).to_dict()
        record['transaction_id'] = f'demo-{label}-001'
        Path(f'examples/{label}.json').write_text(json.dumps(record, indent=2))
    demo = pd.concat([holdout.loc[holdout.target.eq(1)].nlargest(15, 'probability'), holdout.loc[holdout.target.eq(0)].sample(85, random_state=42)])
    demo.drop(columns=['target', 'probability']).sample(frac=1, random_state=42).to_csv('examples/demo.csv', index=False)
    print(json.dumps(metadata, indent=2))

if __name__ == '__main__':
    main()
