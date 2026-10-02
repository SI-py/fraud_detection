import json
from pathlib import Path
import pandas as pd
import pytest
from app.preprocessing import preprocess
from app.inference import Scorer
from app.storage_worker import validate_result

def example():
    return json.loads(Path('examples/fraud.json').read_text())

def test_real_model_separates_examples():
    scorer = Scorer()
    fraud = scorer.score(example())
    legitimate = scorer.score(json.loads(Path('examples/legitimate.json').read_text()))
    assert set(fraud) == {'transaction_id','score','fraud_flag'}
    assert 0 <= legitimate['score'] < fraud['score'] <= 1
    assert fraud['fraud_flag'] == 1 and legitimate['fraud_flag'] == 0

def test_identifiers_and_target_never_enter_features():
    record = example()
    x = preprocess(pd.DataFrame([record]))
    record.update(transaction_id='different', target=1, name_1='different', street='different')
    pd.testing.assert_frame_equal(x, preprocess(pd.DataFrame([record])))

@pytest.mark.parametrize('field,value', [('amount',-1), ('lat',91), ('amount',float('inf')), ('transaction_time','bad time')])
def test_invalid_transaction_rejected(field, value):
    record = example()
    record[field] = value
    with pytest.raises(ValueError):
        Scorer().score(record)

def test_missing_field_rejected():
    record = example()
    del record['cat_id']
    with pytest.raises(ValueError):
        Scorer().score(record)

@pytest.mark.parametrize('record', [{'transaction_id':'a','score':1.1,'fraud_flag':1}, {'transaction_id':'a','score':.5,'fraud_flag':2}, {'transaction_id':'a','score':float('nan'),'fraud_flag':0}])
def test_bad_score_rejected(record):
    with pytest.raises(ValueError):
        validate_result(record)
