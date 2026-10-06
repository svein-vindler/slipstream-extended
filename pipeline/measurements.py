"""In-memory durations only: no payloads, object keys or extra storage calls."""
from __future__ import annotations

import time
from contextlib import contextmanager
from functools import wraps


@contextmanager
def timed(totals, name):
    started = time.perf_counter()
    try:
        yield
    finally:
        totals[name] = totals.get(name, 0) + (time.perf_counter() - started) * 1000


def measured_component(name):
    def decorate(operation):
        @wraps(operation)
        def measured(*args, **kwargs):
            store = kwargs.get("store")
            if store is None:
                return operation(*args, **kwargs)
            if not hasattr(store, "component_timings_ms"):
                store.component_timings_ms = {}
            with timed(store.component_timings_ms, name):
                return operation(*args, **kwargs)
        return measured
    return decorate
