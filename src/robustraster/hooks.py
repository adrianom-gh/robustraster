from typing import Callable, Optional
import pandas as pd
import xarray as xr


def preview_dataset_hook(
    dataset: xr.Dataset,
    user_function: Optional[Callable[[pd.DataFrame], pd.DataFrame]] = None,
    *user_function_args,
    preview_dims: int = 10,
    preview_rows: int = 5,
    **user_function_kwargs,
    
):
    """
    Hook to preview an xarray dataset and optionally apply the user function to the preview.

    Parameters:
    - dataset (xarray.Dataset): The dataset to preview.
    - user_function (Callable, optional): A user-defined function that accepts and returns a pandas DataFrame.
    - preview_dims (int): Max number of values to slice along each dimension.
    - preview_rows (int): Number of preview rows to print.
    """
    # Dynamically create slicing across dimensions
    # Dynamically create slicing across dimensions
    isel_kwargs = {
        dim: slice(0, min(size, preview_dims))
        for dim, size in dataset.sizes.items()
    }

    # Slice and load a small sample of the dataset
    small_slice = dataset.isel(**isel_kwargs)
    print(small_slice)

    # Check each data var’s "lazy" array dimensionality and chunking
    for name, v in small_slice.data_vars.items():
        print(
            name, "dims:", v.dims, "shape:", v.shape,
            "ndim(lazy):", getattr(v.data, "ndim", None),
            "chunks:", getattr(v.data, "chunks", None)
        )

    import dask.array as da

    def first_block_shape(da_):
        # grab one delayed block and compute just that block
        blk = da_.to_delayed().ravel()[0].compute()
        return blk.shape

    for name in ["band_1","band_2"]:
        print(name, "first-block-shape:", first_block_shape(small_slice[name].data))

    eager_data = small_slice.load()

    # Convert to DataFrame
    df_preview = eager_data.to_dataframe().reset_index()

    print("\nDataset preview:")
    print(df_preview.head(preview_rows))

    # Optionally apply the user function
    if user_function:
        try:
            df_with_output = user_function(df_preview, *user_function_args, **user_function_kwargs)
            print("\nUser function output preview:")
            print(df_with_output.head(preview_rows))
        except Exception as e:
            print("\nError running user function on preview:", e)

    return
