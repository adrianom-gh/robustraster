from robustraster.raster_export_manager import RasterExportProcessor
from unittest.mock import MagicMock, patch
import xarray as xr
import numpy as np
import pytest

def test_export_processor_initialization():
    processor = RasterExportProcessor(file_format="GTiff", output_folder="myfolder")
    assert processor.kwargs["file_format"] == "GTiff"
    assert processor.kwargs["output_folder"] == "myfolder"
    assert processor.user_function_handler is None

def test_create_output_basename_sets_expected_filename():
    processor = RasterExportProcessor()
    processor._first_dim = "time"
    # processor._time_value is no longer used internally this way, but time_tag is passed

    mock_ds = MagicMock()
    mock_ds.dims = {"time": 1, "x": 2, "y": 2}
    mock_ds.coords = {
        "time": MagicMock(values=["2025-01-01"]),
        "x": MagicMock(values=[0]),
        "y": MagicMock(values=[0])
    }

    result = processor._create_output_basename(mock_ds, "2025_01_01")
    assert result.endswith("time_2025_01_01")

def test_convert_to_multiband_transforms_correctly():
    processor = RasterExportProcessor()
    ds = xr.Dataset({
        "var1": (("x", "y"), np.ones((2, 2))),
        "var2": (("x", "y"), np.zeros((2, 2)))
    })
    multiband = processor._convert_to_multiband(ds)
    assert multiband.sizes["band"] == 2
    assert "spatial_ref" not in multiband.coords  # expected since CRS isn't set

@patch("robustraster.raster_export_manager.rasterio.open")
def test_export_to_geotiff_creates_file(mock_rio_open):
    processor = RasterExportProcessor(file_format="GTiff", output_folder="out")
    processor._output_basename = "testfile"
    
    dummy = xr.DataArray(np.random.rand(1, 2, 2), dims=("band", "y", "x"))
    dummy.rio.write_crs("EPSG:4326", inplace=True)
    dummy.attrs["transform"] = [0.1, 0, 0, 0, -0.1, 0]
    
    with patch("os.makedirs") as _:
        processor._export_to_geotiff(dummy)

    mock_rio_open.assert_called_once()

@patch("robustraster.raster_export_manager.storage.Client")
def test_create_bucket_and_folder_creates_bucket_and_folder(mock_storage_client):
    processor = RasterExportProcessor()
    mock_bucket = MagicMock()
    mock_client = MagicMock()
    mock_client.get_bucket.side_effect = Exception()  # simulate bucket not found
    mock_client.create_bucket.return_value = mock_bucket
    mock_storage_client.from_service_account_json.return_value = mock_client

    prefix = processor._create_bucket_and_folder("fake_creds.json", "mybucket", "myfolder")
    
    assert prefix == "gcs://mybucket/myfolder"
    mock_storage_client.from_service_account_json.assert_called_once_with("fake_creds.json")
    mock_client.create_bucket.assert_called_once_with("mybucket")

def test_run_and_export_results_raises_on_non_callable_function():
    handler = MagicMock()
    handler.user_function = "not_callable"

    processor = RasterExportProcessor(user_function_handler=handler)
    data_source = MagicMock()
    data_source.dataset.dims = {"time": 1}

    with pytest.raises(ValueError, match="must be callable"):
        processor.run_and_export_results(data_source)