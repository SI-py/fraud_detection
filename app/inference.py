import json
from pathlib import Path
import pandas as pd
from catboost import CatBoostClassifier
from app.preprocessing import preprocess

class Scorer:
    def __init__(self, directory="models"):
        self.model = CatBoostClassifier(thread_count=2, task_type="CPU")
        self.model.load_model(str(Path(directory) / "fraud.cbm"))
        self.metadata = json.loads((Path(directory) / "metadata.json").read_text())
        self.threshold = self.metadata["threshold"]

    def score(self, transaction):
        tx_id = transaction.get("transaction_id")
        if not isinstance(tx_id, str) or not tx_id.strip() or len(tx_id) > 200:
            raise ValueError("transaction_id must be a nonempty string of at most 200 characters")
        score = float(self.model.predict_proba(preprocess(pd.DataFrame([transaction])))[0, 1])
        return {"transaction_id": tx_id, "score": score, "fraud_flag": int(score >= self.threshold)}
