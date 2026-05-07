
import sys
import os
import unittest
from unittest.mock import MagicMock, patch
import logging

# Add src to path so we can import robustraster
sys.path.insert(0, os.path.abspath("src"))

import ee
from robustraster import dask_plugins

class TestEERateLimit(unittest.TestCase):
    def setUp(self):
        # Configure logging to see our retry messages
        logging.basicConfig(level=logging.INFO)
        # Mock ee.data if it doesn't exist (running without real EE auth)
        if not hasattr(ee, "data"):
            ee.data = MagicMock()

    @patch("robustraster.dask_plugins.time.sleep", return_value=None)
    def test_backoff_logic(self, mock_sleep):
        # Create a mock function that raises a rate limit error
        mock_compute = MagicMock()
        # Prevent MagicMock's truthy attr access from triggering the
        # double-wrap sentinel check inside make_robust_ee_call
        mock_compute._is_robust_wrapper = False
        
        # Simulate 429 error twice, then success
        error_msg = "Too Many Requests: Request was rejected because the request rate or concurrency limit was exceeded."
        ee_exception = ee.ee_exception.EEException(error_msg)
        
        mock_compute.side_effect = [ee_exception, ee_exception, "Success!"]
        
        # Apply the wrapper manually for testing
        wrapped_compute = dask_plugins.make_robust_ee_call(mock_compute, method_name="computeValue")
        
        # Call the wrapped function
        print("\n[TEST] Calling wrapped computeValue (expecting 2 retries)...")
        result = wrapped_compute(arg="test")
        
        # Assertions
        print(f"[TEST] Result: {result}")
        self.assertEqual(result, "Success!")
        self.assertEqual(mock_compute.call_count, 3)
        print("[TEST] Verified that function was called 3 times (2 retries).")

    def test_patch_ee_methods(self):
        # Mock ee.data.computeValue
        ee.data.computeValue = MagicMock(return_value="original")
        
        # Apply patch
        dask_plugins.patch_ee_methods()
        
        # Check if it's wrapped
        self.assertTrue(getattr(ee.data.computeValue, "_is_robust_wrapper", False))
        print("[TEST] Verified that ee.data.computeValue is wrapped after patching.")

if __name__ == "__main__":
    unittest.main()
