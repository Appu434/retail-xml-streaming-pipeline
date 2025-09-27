import time
import functools
from pyspark.sql import DataFrame

def with_logging(fn):
    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        print(f"[LOG] Calling {fn.__name__} ...")
        result = fn(*args, **kwargs)
        print(f"[LOG] Done {fn.__name__}.")
        return result
    return wrapper

def log_time(fn):
    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        start = time.time()
        result = fn(*args, **kwargs)
        elapsed = time.time() - start
        print(f"[TIMER] {fn.__name__} took {elapsed:.3f}s")
        return result
    return wrapper

def ensure_schema(expected_cols: list[str]):
    def _decorator(fn):
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            df: DataFrame = fn(*args, **kwargs)
            missing = [c for c in expected_cols if c not in df.columns]
            if missing:
                raise ValueError(f"Schema check failed. Missing columns: {missing}")
            return df
        return wrapper
    return _decorator
