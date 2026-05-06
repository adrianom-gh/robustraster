import os
import json
import ee
import time
import random
import logging
from distributed import WorkerPlugin

def make_robust_ee_call(original_method, method_name=None):
    """
    Wraps an ee.data method with exponential backoff for
    'Too Many Requests' (429) or other retryable errors.
    """
    # Sentinel to avoid double-wrapping
    if getattr(original_method, "_is_robust_wrapper", False):
        return original_method

    method_name = method_name or getattr(original_method, "__name__", "unknown_method")

    def wrapper(*args, **kwargs):
        # Configuration for backoff
        max_retries = 100
        base_delay = 1.0
        max_delay = 60.0
        
        # Signals that indicate we should retry
        retry_signals = [
            "too many requests",
            "request was rejected",
            "request rate",
            "concurrency limit",
            "rate or concurrency",
            "rate limit",
            "quota exceeded",
            "resource exhausted",
            "backend error",
            "internal error",
            "timed out",
            "timeout",
            "throttl",
        ]

        for attempt in range(max_retries):
            try:
                return original_method(*args, **kwargs)
            except getattr(ee, 'EEException', Exception) as e:
                msg = str(e).lower()
                # Check if this is a retryable error
                if not any(s in msg for s in retry_signals):
                    raise  # Not retryable

                # It is retryable; calculate delay
                delay = min(max_delay, base_delay * (2 ** attempt))
                # Add jitter: random factor between 0.5 and 1.5
                delay *= (0.5 + random.random())

                # Log the retry
                logging.warning(
                    f"[robustraster] EE rate limit hit in {method_name} (attempt {attempt+1}/{max_retries}). "
                    f"Retrying in {delay:.2f}s. Error: {e}"
                )
                
                time.sleep(delay)
        
        # If we exit the loop, we ran out of retries
        raise RuntimeError(f"Earth Engine {method_name} failed after {max_retries} retries due to rate limiting.")

    wrapper._is_robust_wrapper = True
    return wrapper

def patch_ee_methods():
    """
    Patches standard ee.data methods with the robust backoff wrapper.
    Can be called on both the client (main process) and workers.
    """
    if not hasattr(ee, "data"):
        logging.warning("[robustraster] ee.data not found, cannot patch methods.")
        return

    methods_to_patch = ["computePixels", "computeValue", "getTile", "getValue"]
    
    for method_name in methods_to_patch:
        if hasattr(ee.data, method_name):
            original = getattr(ee.data, method_name)
            wrapped = make_robust_ee_call(original, method_name=method_name)
            setattr(ee.data, method_name, wrapped)
            logging.info(f"[robustraster] Patched ee.data.{method_name} with backoff.")

class EEPlugin(WorkerPlugin):
    def __init__(self):
        pass

    def setup(self, worker):
        self.worker = worker
        try:
            # Assume credentials already exist at default location
            try:
                ee.Initialize(opt_url='https://earthengine-highvolume.googleapis.com')
            except Exception as e:
                # If already initialized or detailed auth needed, this might fail or be skipped.
                logging.warning(f"[robustraster] ee.Initialize failed on worker: {e}")
            
            # --- PATCH: Monkeypatch ee.data methods ---
            patch_ee_methods()

        except Exception as e:
            # Don't crash the worker, just log the failure to setup EE plugin
            logging.error(f"[robustraster] Failed to setup EEPlugin: {e}")
