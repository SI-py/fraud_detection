"""Shared, stateless feature pipeline for training and online inference."""
import numpy as np
import pandas as pd

RAW_COLUMNS = ["transaction_time", "merch", "cat_id", "amount", "name_1", "name_2", "gender", "street", "one_city", "us_state", "post_code", "lat", "lon", "population_city", "jobs", "merchant_lat", "merchant_lon"]
CATEGORICAL = ["merch", "cat_id", "gender", "one_city", "us_state", "jobs"]
NUMERIC = ["amount", "lat", "lon", "population_city", "merchant_lat", "merchant_lon"]

def preprocess(frame: pd.DataFrame) -> pd.DataFrame:
    missing = set(RAW_COLUMNS) - set(frame.columns)
    if missing:
        raise ValueError(f"Missing fields: {sorted(missing)}")
    result = pd.DataFrame(index=frame.index)
    for col in CATEGORICAL:
        result[col] = frame[col].fillna("unknown").astype(str)
    for col in NUMERIC:
        result[col] = pd.to_numeric(frame[col], errors="raise")
        if not np.isfinite(result[col]).all():
            raise ValueError(f"Non-finite value in {col}")
    if (result.amount < 0).any() or (result.population_city < 0).any():
        raise ValueError("Amount and population must be nonnegative")
    for col, limit in [("lat", 90), ("merchant_lat", 90), ("lon", 180), ("merchant_lon", 180)]:
        if (result[col].abs() > limit).any():
            raise ValueError(f"Invalid coordinate: {col}")
    dt = pd.to_datetime(frame.transaction_time, errors="raise")
    if dt.isna().any():
        raise ValueError("Missing transaction_time")
    result["hour"] = dt.dt.hour
    result["weekday"] = dt.dt.dayofweek
    result["month"] = dt.dt.month
    result["is_night"] = (dt.dt.hour < 6).astype(int)
    result["log_amount"] = np.log1p(result.amount)
    result["log_population"] = np.log1p(result.population_city)
    lat1, lat2 = np.radians(result.lat), np.radians(result.merchant_lat)
    dlat = lat2 - lat1
    dlon = np.radians(result.merchant_lon - result.lon)
    a = np.sin(dlat / 2)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2)**2
    result["distance_km"] = 6371 * 2 * np.arcsin(np.sqrt(np.clip(a, 0, 1)))
    return result
