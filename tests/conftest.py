import sys
import os
from unittest.mock import MagicMock

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src")))

# Mock osgeo and gdal globally to avoid GDAL DLL errors in test environment
sys.modules["osgeo"] = MagicMock()
sys.modules["osgeo.gdal"] = MagicMock()
sys.modules["osgeo.ogr"] = MagicMock()
sys.modules["osgeo.osr"] = MagicMock()