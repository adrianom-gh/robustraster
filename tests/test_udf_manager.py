import pytest
import numpy as np
import xarray as xr
import pandas as pd
from unittest.mock import MagicMock, patch
from robustraster.udf_manager import UserFunctionHandler

def sample_function(df):
    return df

def test_user_function_handler_initialization():
    # Arrange
    args = (1, 2)
    kwargs = {"a": 3, "b": 4}

    handler = UserFunctionHandler(sample_function, user_function_args=args, user_function_kwargs=kwargs)

    # Assert
    assert handler.user_function == sample_function
    assert handler.args == args
    assert handler.kwargs == kwargs

def test_user_function_handler_initialization_defaults():
    # Act
    handler = UserFunctionHandler(sample_function)

    # Assert
    assert handler.user_function == sample_function
    assert handler.args == ()
    assert handler.kwargs == {}

# ------ APPLY USER FUNCTION TESTING -------?

# This function adds a 'sum' column but keeps the 'x' and 'y' structure
def dummy_user_function(df: pd.DataFrame) -> pd.DataFrame:
    df["sum"] = df["band"]
    return df

@pytest.mark.skip(reason="Tuning is hard...")
def test_apply_user_function_returns_expected_result():
    # Create dataset with chunked dimensions
    data = xr.Dataset({
        "band": (("x", "y"), np.random.rand(4, 4))
    }).chunk({"x": 2, "y": 2})

    mock_data_source = MagicMock()
    mock_data_source.dataset = data

    handler = UserFunctionHandler(dummy_user_function)

    result = handler.apply_user_function(mock_data_source)

    # Validate that the result is a Dask-backed xarray.Dataset
    assert isinstance(result, xr.Dataset)
    assert "sum" in result.data_vars

    # Confirm it is lazy (dask array)
    assert isinstance(result["sum"].data, da.Array)

    # Optionally check dimensions or shape
    assert result["sum"].shape == (4, 4)



# ----- tune_user_function testing ------
