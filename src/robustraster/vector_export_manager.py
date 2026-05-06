from osgeo import gdal
import rasterio
from rasterio.io import MemoryFile
from .dataset_manager import RasterDataset, EarthEngineDataset
from .format_time import create_time_tag
from .reports_setup import setup_dask_reports
from pathlib import Path
from google.cloud import storage
import xarray as xr
import gcsfs
import os
import glob
import posixpath
import io
from dask.distributed import performance_report

class VectorExportProcessor:
    def __init__(self, user_function_handler=None, **kwargs):
        """
        Initialize ExportProcessor.
        - If a UserFunctionHandler instance is provided, use it.
        - If not, create a new instance.
        
        :param user_function_handler: Optional existing UserFunctionHandler object.
        """
        self.user_function_handler = user_function_handler
        self.kwargs = kwargs

        self._first_dim = None
        self._time_value = None
        self._output_basename = None
        self._gcs_prefix = None
        self._tile_id = None
    
    def _create_output_basename(self, slice_2d, time_tag):
        x_dim = 'x' if 'x' in slice_2d.coords else 'X'
        y_dim = 'y' if 'y' in slice_2d.coords else 'Y'

        x0 = float(slice_2d[x_dim].min())
        x1 = float(slice_2d[x_dim].max())
        y0 = float(slice_2d[y_dim].min())
        y1 = float(slice_2d[y_dim].max())

        x0, x1, y0, y1 = map(lambda v: int(round(v)), (x0, x1, y0, y1))

        bbox_tag = f"x{x0}_{x1}_y{y0}_{y1}"

        if self._tile_id:
            return f"{bbox_tag}_tile_{self._tile_id}__{self._first_dim}_{time_tag}"
        else:
            return f"{bbox_tag}__{self._first_dim}_{time_tag}"

    def _create_bucket_and_folder(self, gcs_credentials, gcs_bucket, gcs_folder):
        # Initialize GCS client
        storage_client = storage.Client.from_service_account_json(gcs_credentials)

        # Check if bucket exists, create if not
        try:
            bucket = storage_client.get_bucket(gcs_bucket)
            print(f"Bucket {gcs_bucket} already exists.")
        except Exception:
            bucket = storage_client.create_bucket(gcs_bucket)
            print(f"Created bucket: {gcs_bucket}")

        # Handle optional folder creation
        if gcs_folder:
            folder_blob = f"{gcs_folder}/"
            if not any(blob.name.startswith(folder_blob) for blob in storage_client.list_blobs(gcs_bucket, prefix=folder_blob, max_results=1)):
                blob = bucket.blob(folder_blob)
                blob.upload_from_string('')
                print(f"Created folder: {gcs_folder}")

        # Create GCS path
        if gcs_folder:
            gcs_prefix = f"gcs://{gcs_bucket}/{gcs_folder}"
        else:
            gcs_prefix = f"gcs://{gcs_bucket}"
        return gcs_prefix

    def _export_csv_to_gcs(self, df_output):
        """Export dataset chunk to Google Cloud Storage as a COG."""
        gcs_credentials = self.kwargs.get('gcs_credentials', None) 
        fs = gcsfs.GCSFileSystem(token=gcs_credentials)

        # Compose full GCS path
        gcs_path = posixpath.join(self._gcs_prefix, f"{self._output_basename}.csv")

        # Create in-memory CSV
        buffer = io.StringIO()
        df_output.to_csv(buffer, index=False)
        buffer.seek(0)

        # Upload to GCS
        with fs.open(gcs_path, "w") as f:
            f.write(buffer.read())

        print(f"[robustraster] Exported CSV to GCS: {gcs_path}")
    
    def _export_parquet_to_gcs(self, df_output):
        """Export dataset chunk to Google Cloud Storage as a COG."""
        gcs_credentials = self.kwargs.get('gcs_credentials', None) 
        fs = gcsfs.GCSFileSystem(token=gcs_credentials)

        # Compose full GCS path
        gcs_path = posixpath.join(self._gcs_prefix, f"{self._output_basename}.parquet")

        # Create in-memory CSV
        buffer = io.StringIO()
        df_output.to_parquet(buffer, index=False, engine="fastparquet")
        buffer.seek(0)

        # Upload to GCS
        with fs.open(gcs_path, "w") as f:
            f.write(buffer.read())

        print(f"[robustraster] Exported parquet to GCS: {gcs_path}")

    def _format_dataset(self, ds, ds_output):
        # Format dataset by renaming, transposing, and ensuring CRS.
        crs = ds.attrs.get('crs', None)
         # Rename dimensions only if needed
        rename_dims = {}
        if 'X' in ds_output.dims:
            rename_dims['X'] = 'x'
        if 'Y' in ds_output.dims:
            rename_dims['Y'] = 'y'
        ds_renamed = ds_output.rename(rename_dims)
        ds_transposed = ds_renamed.transpose(self._first_dim, 'y', 'x').rio.write_crs(crs)
        return ds_transposed.sortby("y", ascending=False)
    
    def _user_function_export_csv_wrapper(self, ds, *args):
        """
        Wrapper function that applies either `tune_user_function` or `apply_user_function`.
        to the user's dataset. This will convert the user's dataset to a pandas DataFrame
        first before running the user's function.
        
        Parameters:
        - user_func: the user-defined function to apply.
        - args: positional arguments to pass to the function.
        - kwargs: keyword arguments to pass to the function.
        
        Returns:
        - result: the result of applying the function to the dataframe.
        """
        df_input = ds.to_dataframe().reset_index()
        df_output = self.user_function_handler.user_function(df_input, *self.user_function_handler.args, **self.user_function_handler.kwargs)
        import pandas as pd
        if isinstance(df_output, pd.Series):
            if df_output.name is None:
                df_output.name = 'output'
            df_output = df_output.to_frame()

        df_output = df_output.copy()

        dims = list(ds.dims)
        for dim in dims:
            if dim not in df_output.columns and dim in df_input.columns:
                df_output[dim] = df_input[dim]
        
        del df_input
        import gc
        gc.collect()
                
        df_output = df_output.set_index(dims)
        ds_output = df_output.to_xarray()
        
        del df_output
        gc.collect()

        for i, time_val in enumerate(ds_output[self._first_dim].values):
            self._time_value = time_val
            time_tag = create_time_tag(time_val)
            slice_2d = ds_output.isel({self._first_dim: i})
            self._output_basename = self._create_output_basename(slice_2d, time_tag)
            df_slice = slice_2d.to_dataframe().reset_index()
            
            if self.kwargs.get('export_to_gcs'):
                self._export_csv_to_gcs(df_slice)
            else:
                output_folder = self.kwargs.get('output_folder', 'tiles')
                os.makedirs(output_folder, exist_ok=True)
                output_path = os.path.join(output_folder, f"{self._output_basename}.csv")
                df_slice.to_csv(output_path, index=False)
            
            del df_slice
            gc.collect()
        
        return ds_output
    
    def _user_function_export_parquet_wrapper(self, ds, *args):
        """
        Wrapper function that applies either `tune_user_function` or `apply_user_function`.
        to the user's dataset. This will convert the user's dataset to a pandas DataFrame
        first before running the user's function.
        
        Parameters:
        - user_func: the user-defined function to apply.
        - args: positional arguments to pass to the function.
        - kwargs: keyword arguments to pass to the function.
        
        Returns:
        - result: the result of applying the function to the dataframe.
        """
        df_input = ds.to_dataframe().reset_index()
        df_output = self.user_function_handler.user_function(df_input, *self.user_function_handler.args, **self.user_function_handler.kwargs)
        import pandas as pd
        if isinstance(df_output, pd.Series):
            if df_output.name is None:
                df_output.name = 'output'
            df_output = df_output.to_frame()

        df_output = df_output.copy()

        dims = list(ds.dims)
        for dim in dims:
            if dim not in df_output.columns and dim in df_input.columns:
                df_output[dim] = df_input[dim]
        
        del df_input
        import gc
        gc.collect()
                
        df_output = df_output.set_index(dims)
        ds_output = df_output.to_xarray()
        
        del df_output
        gc.collect()

        for i, time_val in enumerate(ds_output[self._first_dim].values):
            self._time_value = time_val
            time_tag = create_time_tag(time_val)
            slice_2d = ds_output.isel({self._first_dim: i})
            self._output_basename = self._create_output_basename(slice_2d, time_tag)
            df_slice = slice_2d.to_dataframe().reset_index()
            
            if self.kwargs.get('export_to_gcs'):
                self._export_parquet_to_gcs(df_slice)
            else:
                output_folder = self.kwargs.get('output_folder', 'tiles')
                os.makedirs(output_folder, exist_ok=True)
                output_path = os.path.join(output_folder, f"{self._output_basename}.parquet")
                df_slice.to_parquet(output_path, index=False, engine="pyarrow") #could also use pyarrow
            
            del df_slice
            gc.collect()
        
        return ds_output
    
    def run_and_export_results(self, data_source: RasterDataset | EarthEngineDataset):
        # Keyword arguments:
        # flag: str
        # chunks: Optional[dict | str]
        # output_folder: str
        # gcs_bucket: str
        # gcs_folder: str
        """Main function to apply user function and export results."""
        
        if not callable(self.user_function_handler.user_function):
            raise ValueError("The provided function must be callable.")

        self._first_dim = list(data_source.dataset.dims)[0]
        ds = self.user_function_handler._create_apply_chunk(data_source.dataset)

        # Generate template xarray
        template_xarray = self.user_function_handler._generate_template_xarray(ds)

        if self.kwargs.get("export_to_gcs"):
            self._gcs_prefix = self._create_bucket_and_folder(self.kwargs.get("gcs_credentials"), self.kwargs.get("gcs_bucket"), self.kwargs.get("gcs_folder", None))

        if self.kwargs.get("file_format") == "CSV":
            result = xr.map_blocks(self._user_function_export_csv_wrapper,
                                    ds,
                                    template=template_xarray)
            if self.kwargs.get("report") is True:
                output_folder = Path(self.kwargs.get("output_folder", "tiles"))
                report_path = setup_dask_reports(output_folder, tile_id=self._tile_id, slice_tag=None)
                with performance_report(filename=report_path):
                    result.compute()
            else:
                result.compute()


        if self.kwargs.get("file_format") == "parquet":
            result = xr.map_blocks(self._user_function_export_parquet_wrapper,
                                    ds,
                                    template=template_xarray)
            if self.kwargs.get("report") is True:
                output_folder = Path(self.kwargs.get("output_folder", "tiles"))
                report_path = setup_dask_reports(output_folder, tile_id=self._tile_id, slice_tag=None)
                with performance_report(filename=report_path):
                    result.compute()
            else:
                result.compute()