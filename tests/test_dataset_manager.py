import os
import numpy as np
import rasterio
import pytest
import ee
import xarray as xr
from robustraster.dataset_manager import RasterDataset, EarthEngineDataset

'''
@pytest.fixture(scope="session", autouse=True)
def setup_earth_engine():
    # Inject the variable if it’s not already set
    if os.getenv("GOOGLE_APPLICATION_CREDENTIALS") is None:
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"R:\SCRATCH\adrianom\credentials\earthengineapi\robust-raster-cecdcc4b5fba.json"

    credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    print(f"CREDENTIALS PATH: {credentials_path}")
    print(f"EXISTS: {os.path.exists(credentials_path)}")
    if credentials_path and os.path.exists(credentials_path):
        ee.Initialize(ee.ServiceAccountCredentials(None, credentials_path),
                      opt_url='https://earthengine-highvolume.googleapis.com')
    else:
        raise RuntimeError("GOOGLE_APPLICATION_CREDENTIALS is not set correctly!")
'''