import pytest
from robustraster.run_manager import run
import pandas as pd
from unittest.mock import patch, MagicMock
import ee
import re
import xarray as xr
import numpy as np

# Dummy user function
def dummy_function(df: pd.DataFrame) -> pd.DataFrame:
    df["processed"] = True
    return df

# ---- BASIC UNIT TESTS ----
def test_invalid_dataset_type_raises_type_error():
    with pytest.raises(TypeError, match=re.escape("dataset must be an ee.ImageCollection, a string path, or a list of string paths.")):
        run(
            dataset=123,
            user_function_config={"user_function": dummy_function},
            export_config={"mode": "raster", "output_folder": "temp_out"}
        )

@patch("robustraster.run_manager.preview_dataset_hook")
@patch("robustraster.run_manager.RasterDataset")
def test_preview_dataset_hook_invoked(mock_raster, mock_preview):
    # Create a fake xarray-like dataset with a .dims attribute
    mock_dataset = MagicMock()
    mock_dataset.dims = {"time": 1, "x": 2, "y": 2}

    # Patch the RasterDataset's return to use our fake dataset
    mock_raster.return_value.dataset = mock_dataset

    run(
        dataset="dummy.tif",
        user_function_config={"user_function": dummy_function},
        preview_dataset=True,
        export_config={"mode": "raster", "output_folder": "temp_out"},
        dask_mode="test"
    )

    mock_preview.assert_called_once()

@patch("robustraster.run_manager.RasterDataset")
def test_before_after_hooks_called(mock_raster):
    # Create a fake xarray-like dataset with a .dims attribute
    mock_dataset = MagicMock()
    mock_dataset.dims = {"time": 1, "x": 2, "y": 2}

    # Patch the RasterDataset's return to use our fake dataset
    mock_raster.return_value.dataset = mock_dataset

    # Setup mock hooks
    before = MagicMock()
    after = MagicMock()

    # Run the test
    run(
        dataset="file.tif",
        user_function_config={"user_function": dummy_function},
        export_config={"mode": "raster", "output_folder": "temp_out"},
        dask_mode="test",
        hooks={"before_run": before, "after_run": after}
    )

    # Assertions
    before.assert_called_once()
    after.assert_called_once_with(mock_dataset)

@patch("robustraster.run_manager.RasterDataset")
@patch("robustraster.run_manager.DaskClusterManager")
@pytest.mark.parametrize("dask_mode", ["test", "full"])
def test_dask_cluster_initialization(mock_cluster, mock_raster, dask_mode):
    # ✅ Create a mock xarray.Dataset
    mock_xr_dataset = MagicMock()
    mock_xr_dataset.dims = {"band": 1, "x": 256, "y": 256}  # must have `.dims`!

    # Set up mock RasterDataset return value
    mock_raster_instance = MagicMock()
    mock_raster_instance.dataset = mock_xr_dataset
    mock_raster.return_value = mock_raster_instance

    # Set up mock DaskClusterManager
    mock_client = MagicMock()
    mock_cluster_instance = MagicMock()
    mock_cluster_instance.get_dask_client = mock_client
    mock_cluster.return_value = mock_cluster_instance

    # Run the function
    run(
        dataset="dummy.tif",
        user_function_config={"user_function": dummy_function},
        export_config={"mode": "raster", "output_folder": "temp_out"},
        dask_mode=dask_mode
    )

    # Assertions
    mock_cluster.assert_called_once()
    mock_cluster_instance.create_cluster.assert_called_with(mode=dask_mode)

@patch("robustraster.run_manager.DaskClusterManager")
@patch("robustraster.run_manager.RasterDataset")
def test_custom_dask_mode_passes_kwargs(mock_raster, mock_cluster):
    mock_dataset = MagicMock()
    mock_dataset.dims = {"x": 1, "y": 1}
    mock_raster.return_value.dataset = mock_dataset

    mock_client = MagicMock()
    mock_cluster_instance = MagicMock()
    mock_cluster_instance.get_dask_client = mock_client
    mock_cluster.return_value = mock_cluster_instance

    dask_kwargs = {"n_workers": 2, "threads_per_worker": 1}
    run(
        dataset="dummy.tif",
        user_function_config={"user_function": dummy_function},
        export_config={"mode": "raster", "output_folder": "temp_out"},
        dask_mode="custom",
        dask_config=dask_kwargs
    )

    mock_cluster_instance.create_cluster.assert_called_with(mode="custom", **dask_kwargs)

@patch("robustraster.run_manager.RasterDataset")
@patch("robustraster.run_manager.RasterExportProcessor")
@patch("robustraster.run_manager.DaskClusterManager")
def test_run_local_raster_minimal(mock_dask, mock_export, mock_raster):
    # Create a fake xarray-like dataset with a .dims attribute
    mock_dataset = MagicMock()
    mock_dataset.dims = {"time": 1, "x": 2, "y": 2}

    # Patch the RasterDataset's return to use our fake dataset
    mock_raster.return_value.dataset = mock_dataset

    run(
        dataset="dummy.tif",
        user_function_config={"user_function": dummy_function},
        export_config={"mode": "raster", "output_folder": "temp_out"},
        dask_mode="test"
    )

    mock_raster.assert_called_once()
    mock_dask.assert_called_once()
    mock_export.assert_called_once()

@patch("robustraster.run_manager.EarthEngineDataset")
@patch("robustraster.run_manager.DaskClusterManager")
def test_run_with_earth_engine(mock_cluster, mock_ee_dataset):
    # Mock EE ImageCollection input
    mock_ic = MagicMock(spec=ee.ImageCollection)

    # Mock xarray dataset with dims
    mock_xr = MagicMock()
    mock_xr.dims = {"time": 1, "x": 256, "y": 256}

    # Mock EarthEngineDataset return
    mock_ee_instance = MagicMock()
    mock_ee_instance.dataset = mock_xr
    mock_ee_dataset.return_value = mock_ee_instance

    # Mock Dask cluster setup
    mock_client = MagicMock()
    mock_cluster_instance = MagicMock()
    mock_cluster_instance.get_dask_client = mock_client
    mock_cluster.return_value = mock_cluster_instance

    run(
        dataset=mock_ic,
        max_pixels_per_tile=None,
        dataset_config={"geometry": "test.geojson"},
        user_function_config={"user_function": dummy_function},
        export_config={"mode": "raster", "output_folder": "temp_out"},
        dask_mode="test"
    )

    mock_ee_dataset.assert_called_once_with(image_collection=mock_ic, dataset_config={"geometry": "test.geojson"})
    mock_cluster_instance.create_cluster.assert_called_with(mode="test")

@patch("robustraster.run_manager.RasterDataset")
def test_missing_export_kwargs_raises_error(mock_raster):
    mock_dataset = MagicMock()
    mock_dataset.dims = {"x": 1, "y": 1}
    mock_raster.return_value.dataset = mock_dataset

    with pytest.raises(ValueError, match=re.escape("Missing required export configuration: 'mode'")):
        run(
            dataset="dummy.tif",
            user_function_config={"user_function": dummy_function},
            dask_mode="test"
        )


@patch("robustraster.run_manager.RasterDataset")
def test_missing_gcs_bucket_raises_value_error(mock_raster):
    mock_dataset = MagicMock()
    mock_dataset.dims = {"x": 1, "y": 1}
    mock_raster.return_value.dataset = mock_dataset

    with pytest.raises(ValueError, match=re.escape("Missing required GCS export configuration: gcs_bucket")):
        run(
            dataset="dummy.tif",
            user_function_config={"user_function": dummy_function},
            export_config={"mode": "raster", "export_to_gcs": True},
            dask_mode="test"
        )

@patch("robustraster.run_manager.RasterDataset")
def test_no_user_function(mock_raster):
    # Create a fake xarray-like dataset with a .dims attribute
    mock_dataset = MagicMock()
    mock_dataset.dims = {"time": 1, "x": 2, "y": 2}

    # Patch the RasterDataset's return to use our fake dataset
    mock_raster.return_value.dataset = mock_dataset

    with pytest.raises(ValueError, match=re.escape("No user function was specified or user function is not callable! Please provide a function that accepts and returns a pandas DataFrame.")):
        run(
            dataset="file.tif",
            export_config={"mode": "raster", "output_folder": "temp_out"},
            dask_mode="test"
        )

@patch("robustraster.run_manager.RasterDataset")
def test_user_function_not_callable(mock_raster):
    # Create a fake xarray-like dataset with a .dims attribute
    mock_dataset = MagicMock()
    mock_dataset.dims = {"time": 1, "x": 2, "y": 2}

    # Patch the RasterDataset to return our mock dataset
    mock_raster.return_value.dataset = mock_dataset

    # Attempt to pass a non-callable object (like a string) as the user_function
    with pytest.raises(ValueError, match=re.escape("No user function was specified or user function is not callable! Please provide a function that accepts and returns a pandas DataFrame.")):
        run(
            dataset="file.tif",
            user_function_config={"user_function": "not_a_function"},  # <-- Invalid input
            export_config={"mode": "raster", "output_folder": "temp_out"},
            dask_mode="test"
        )

@patch("robustraster.run_manager.UserFunctionHandler.tune_user_function")
@patch("robustraster.run_manager.RasterDataset")
@pytest.mark.skip(reason="Tuning is hard.")
def test_tune_function_triggers_tuning(mock_raster, mock_tune):
    # Create a minimal real xarray.Dataset with valid structure
    data = xr.Dataset({
        "band": (("x", "y"), np.random.rand(10, 10))
    }).chunk({"x": 5, "y": 5})

    mock_raster_instance = MagicMock()
    mock_raster_instance.dataset = data
    mock_raster.return_value = mock_raster_instance

    # Mock tune_user_function to skip actual logic
    mock_tune.return_value = None

    run(
        dataset="dummy.tif",
        user_function_config={"user_function": lambda df: df},
        export_config={"mode": "raster", "output_folder": "temp_out"},
        dask_mode="test",
        function_tuning_config={"tune_function": True}
    )

    mock_tune.assert_called_once()
