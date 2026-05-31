from osgeo import gdal
import rasterio
from rasterio.io import MemoryFile
from .dataset_manager import RasterDataset, EarthEngineDataset
from google.cloud import storage
import xarray as xr
import gcsfs
import os
import glob
import posixpath

class ExportProcessor:
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
    
    def _generate_vrt(self, input_files: list, output_vrt: str):
        """Generate a VRT file from a list of GeoTIFF files."""
        if not input_files:
            print("No GeoTIFF files found to create VRT.")
            return
        
        vrt_dataset = gdal.BuildVRT(output_vrt, input_files)

        if vrt_dataset:
            vrt_dataset.FlushCache()  # Save changes
            vrt_dataset = None  # Close dataset
            print(f"VRT file created successfully: {output_vrt}")
        else:
            print("Failed to create VRT file.")

    def _generate_vrt_from_tifs(self, time_val):
        """Generate a VRT from all GeoTIFF files in a given directory."""
        output_folder = self.kwargs.get('output_folder', 'tiles')
        time_str = str(time_val).replace(":", "_").replace("-", "_").replace(" ", "_")
        vrt_file_name = f"{self._first_dim}_{time_str}.vrt"
        output_vrt_path = os.path.join(output_folder, vrt_file_name)
        tif_files = glob.glob(os.path.join(output_folder, f"*{self._first_dim}_{time_str}*.tif"))
        self._generate_vrt(tif_files, output_vrt_path)
    
    def _create_output_basename(self, ds_block):
        time_str = str(self._time_value).replace(":", "_").replace("-", "_").replace(" ", "_")
        chunk_hash = hash(tuple(ds_block.coords[dim].values[0] for dim in ds_block.dims))

        return f"chunk_{chunk_hash}_{self._first_dim}_{time_str}"

    def _convert_to_multiband(self, chunk_dataset):
        """Convert chunk dataset into a multi-band xarray DataArray."""
        stacked = chunk_dataset.to_array(dim="band")
        stacked = stacked.assign_coords(band=list(chunk_dataset.data_vars))

        if "spatial_ref" not in stacked.coords:
            print("CRS IS NOT SET! SET IT IN YOUR EARTH ENGINE CODE!")
        return stacked

    def _create_bucket_and_folder(self, gcs_credentials, gcs_bucket, gcs_folder, gcs_project=None):
        # Initialize GCS client
        if gcs_credentials:
            storage_client = storage.Client.from_service_account_json(gcs_credentials)
        else:
            try:
                storage_client = storage.Client(project=gcs_project)
            except EnvironmentError as e:
                raise ValueError("Could not determine Google Cloud project. Please provide 'gcs_project' in export_config or set the GOOGLE_CLOUD_PROJECT environment variable.") from e

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

    def _user_function_export_wrapper(self, ds, *args):
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

        dims = list(ds.dims)
        for dim in dims:
            if dim not in df_output.columns and dim in df_input.columns:
                df_output[dim] = df_input[dim]
                
        df_output = df_output.set_index(dims)
        ds_output = df_output.to_xarray()
        
        ds_transposed = self._format_dataset(ds, ds_output)

        for i, time_val in enumerate(ds_transposed[self._first_dim].values):
            self._time_value = time_val
            slice_2d = ds_transposed.isel({self._first_dim: i})
            self._output_basename = self._create_output_basename(slice_2d)
            self._compute_chunks_and_export(slice_2d)
        
        return ds_output
    
    def _user_function_export_vector_wrapper(self, ds, *args):
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

        dims = list(ds.dims)
        for dim in dims:
            if dim not in df_output.columns and dim in df_input.columns:
                df_output[dim] = df_input[dim]
                
        df_output = df_output.set_index(dims)
        ds_output = df_output.to_xarray()
        
        for i, time_val in enumerate(ds[self._first_dim].values):
            self._time_value = time_val
            slice_2d = ds.isel({self._first_dim: i})
            self._output_basename = self._create_output_basename(slice_2d)
            df_output.to_csv(f"{self._output_basename}.csv")
        #ds_transposed = self._format_dataset(ds, ds_output)

        '''for i, time_val in enumerate(ds_transposed[self._first_dim].values):
            self._time_value = time_val
            slice_2d = ds_transposed.isel({self._first_dim: i})
            self._output_basename = self._create_output_basename(slice_2d)
            self._compute_chunks_and_export(slice_2d)'''
        
        return ds_output
    
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
    
    def _compute_chunks_and_export(self, ds_transposed):
        """Export a single block (already chunked by Dask) using the appropriate method."""
        stacked = self._convert_to_multiband(ds_transposed)

        if self.kwargs.get('flag') == "GTiff":
            self._export_to_geotiff(stacked)

        elif self.kwargs.get('flag') == "GCS":
            self._export_to_gcs(stacked)

    def _export_to_geotiff(self, stacked):
        output_folder = self.kwargs.get('output_folder', 'tiles')
        os.makedirs(output_folder, exist_ok=True)
        output_path = os.path.join(output_folder, f"{self._output_basename}.tif")

        band_names = list(stacked.band.values)

        with rasterio.open(
            output_path,
            "w",
            driver="COG",
            height=stacked.rio.height,
            width=stacked.rio.width,
            count=len(band_names),
            dtype=str(stacked.dtype),
            crs=stacked.rio.crs,
            transform=stacked.rio.transform(),
        ) as dst:
            for idx, name in enumerate(band_names, start=1):
                dst.write(stacked[idx - 1].values, indexes=idx)
                dst.set_band_description(idx, str(name))

        print(f"Exported: {output_path} with bands {band_names}")

    def _export_to_gcs(self, stacked):
        """Export dataset chunk to Google Cloud Storage as a COG."""
        gcs_credentials = self.kwargs.get('gcs_credentials', None) 
        token = gcs_credentials if gcs_credentials else 'google_default'
        fs = gcsfs.GCSFileSystem(token=token)
        gcs_path = posixpath.join(self._gcs_prefix, f"{self._output_basename}.tif")
        
        with MemoryFile() as memfile:
            with memfile.open(
                driver="COG",
                width=stacked.rio.width,
                height=stacked.rio.height,
                count=len(stacked.band),
                dtype=stacked.dtype,
                crs=stacked.rio.crs,
                transform=stacked.rio.transform(),
            ) as dataset:
                dataset.write(stacked.values)

            with fs.open(gcs_path, "wb") as f:
                f.write(memfile.read())

        print(f"Exported to GCS: {gcs_path} with bands {list(stacked.band.values)}")

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
        chunks = self.kwargs.get('chunks', None)
        ds = self.user_function_handler._create_apply_chunk(data_source.dataset, chunks)
        
        if self.kwargs.get("flag") == "GCS":
            self._gcs_prefix = self._create_bucket_and_folder(self.kwargs.get("gcs_credentials"), self.kwargs.get("gcs_bucket"), self.kwargs.get("gcs_folder", None), self.kwargs.get("gcs_project", None))

        template_xarray = self.user_function_handler._generate_template_xarray(ds)
        result = xr.map_blocks(self._user_function_export_vector_wrapper,
                                   ds,
                                   template=template_xarray)
        result.compute()

        if self.kwargs.get('vrt'):
            self.export_vrt(data_source)

    def export_vrt(self, data_source: RasterDataset | EarthEngineDataset):
        for i, time_val in enumerate(data_source.dataset[self._first_dim].values):
            self._generate_vrt_from_tifs(time_val)