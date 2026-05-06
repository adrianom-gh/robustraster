import rasterio
from rasterio.io import MemoryFile
from .dataset_manager import RasterDataset, EarthEngineDataset
from .reports_setup import setup_dask_reports
from .format_time import create_time_tag
from google.cloud import storage
from pathlib import Path
import xarray as xr
import gcsfs
import os
import posixpath
from dask.distributed import performance_report, print

import os
import numpy as np
import rasterio
from rasterio.transform import from_origin

# For exponential backoff
import time
import random
import ee


class RasterExportProcessor:
    def __init__(self, user_function_handler=None, ee_semaphore=None, **kwargs):
        """
        Initialize ExportProcessor.
        - If a UserFunctionHandler instance is provided, use it.
        - If not, create a new instance.
        
        :param user_function_handler: Optional existing UserFunctionHandler object.
        """
        self.user_function_handler = user_function_handler
        self.ee_semaphore = ee_semaphore
        self.kwargs = kwargs

        self._first_dim = None
        self._output_basename = None
        self._gcs_prefix = None

        self._template_xarray = None
        self._data_source = None

        # For the naming scheme of my output files
        self._tile_id = None

    def _is_ee_retryable_error(self, exc: Exception) -> bool:
        msg = str(exc).lower()

        retry_signals = [
            "too many requests",                 # ✅ your exact error (HTTP 429)
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

        return any(s in msg for s in retry_signals)

    def ee_call_with_backoff(self, fn, max_retries=100, base_delay=1.0, max_delay=60.0):
        """
        Run fn() with exponential backoff + jitter on retryable EE errors.
        """
        for attempt in range(max_retries):
            try:
                return fn()
            except Exception as e:
                # Retry ONLY if this looks like an EE quota / throttling error
                print("Error: ", e)
                if not self._is_ee_retryable_error(e):
                    raise

                delay = min(max_delay, base_delay * (2 ** attempt))
                delay *= (0.5 + random.random())  # jitter in [0.5x, 1.5x]

                print(
                    f"[robustraster][EE backoff] "
                    f"attempt {attempt+1}/{max_retries}, sleeping {delay:.2f}s: {e}"
                )
                time.sleep(delay)


        raise RuntimeError(f"Earth Engine call failed after {max_retries} retries")

    def _iter_chunk_batches(
        self,
        ds: xr.Dataset,
        split_dim: str,
        target_blocks_per_run: int,
    ):
        """Yield (indexers, tag) batches aligned to *existing* Dask chunks.

        Parameters
        ----------
        ds:
            Chunked xarray Dataset (Dask-backed).
        split_dim:
            Dimension to batch along (e.g., "X", "Y", "time").
        target_blocks_per_run:
            Approximate upper bound on number of *chunks* (blocks) processed per run.
            Task count is typically a multiple of block count, so this keeps graphs manageable.
        """
        if ds.chunks is None:
            raise ValueError("Dataset is not chunked; cannot batch by chunk groups.")

        if split_dim not in ds.dims:
            raise ValueError(f"split_dim={split_dim!r} not in ds.dims={list(ds.dims)}")

        # Find a representative dask-backed variable
        rep_var = None
        for v in ds.data_vars:
            a = ds[v].data
            if hasattr(a, "numblocks"):
                rep_var = v
                break
        if rep_var is None:
            raise ValueError("No dask-backed variables found; nothing to batch.")

        rep = ds[rep_var]
        numblocks_by_dim = dict(zip(rep.dims, rep.data.numblocks))

        # blocks_per_run ≈ (split_chunks_in_run) * product(other_dim_chunks)
        other_blocks = 1
        for d, nb in numblocks_by_dim.items():
            if d != split_dim:
                other_blocks *= int(nb)

        k = max(1, int(target_blocks_per_run) // max(1, other_blocks))

        split_chunks = ds.chunks[split_dim]
        if split_chunks is None:
            raise ValueError(f"No chunking info available for split_dim={split_dim!r}")

        # cumulative element indices at chunk boundaries
        boundaries = [0]
        for sz in split_chunks:
            boundaries.append(boundaries[-1] + int(sz))

        n_split_chunks = len(split_chunks)
        for ci0 in range(0, n_split_chunks, k):
            ci1 = min(n_split_chunks, ci0 + k)
            i0 = boundaries[ci0]
            i1 = boundaries[ci1]
            indexers = {split_dim: slice(i0, i1)}
            tag = f"{split_dim}_{ci0:06d}_{ci1:06d}"
            yield indexers, tag

    def _pick_default_split_dim(self, ds: xr.Dataset) -> str:
        """Choose a reasonable batching dimension if the user doesn't specify one."""
        for cand in ("X", "x", "Y", "y"):
            if cand in ds.dims:
                return cand
        # Fall back: choose the last dimension (often spatial)
        return list(ds.dims)[-1]
    
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
        
        # We do this check to ensure _user_function_export_wrapper formats the data
        # properly for the map_blocks return statement. 
        if isinstance(data_source , RasterDataset):
            self._data_source = "local"
        elif isinstance(data_source, EarthEngineDataset):
            self._data_source = "ee"

        self._first_dim = list(data_source.dataset.dims)[0]
        ds = self.user_function_handler._create_apply_chunk(data_source.dataset)
        
        # Generate template xarray (for non-batched runs)
        self._template_xarray = self.user_function_handler._generate_template_xarray(ds)

        if self.kwargs.get("export_to_gcs"):
            self._gcs_prefix = self._create_bucket_and_folder(
                self.kwargs.get("gcs_credentials"),
                self.kwargs.get("gcs_bucket"),
                self.kwargs.get("gcs_folder", None),
            )

        # Optional batching to avoid enormous Dask graphs when chunk sizes must stay small
        target_blocks_per_run = self.kwargs.get("target_blocks_per_run", None)
        split_dim = self.kwargs.get("batch_split_dim", None)

        do_batching = (target_blocks_per_run is not None) or (split_dim is not None)

        if do_batching:
            if target_blocks_per_run is None:
                # reasonable default if user only provided split_dim
                target_blocks_per_run = 20000
            target_blocks_per_run = int(target_blocks_per_run)

            if split_dim is None:
                split_dim = self._pick_default_split_dim(ds)

            for indexers, tag in self._iter_chunk_batches(
                ds, split_dim=split_dim, target_blocks_per_run=target_blocks_per_run
            ):
                ds_part = ds.isel(indexers)

                template_part = self._template_xarray.isel(indexers)

                result = xr.map_blocks(
                    self._user_function_export_wrapper,
                    ds_part,
                    template=template_part,
                )

                if self.kwargs.get("visualize_task_graph"):
                    import dask
                    output_folder = Path(self.kwargs.get("output_folder", "tiles"))
                    reports_dir = output_folder / "reports"
                    reports_dir.mkdir(parents=True, exist_ok=True)
                    
                    graph_path = self.kwargs.get("visualize_task_graph")
                    p = Path(graph_path)
                    if self._tile_id:
                        batch_graph_path = reports_dir / f"{p.stem}_tile_{self._tile_id}_{tag}{p.suffix}"
                    else:
                        batch_graph_path = reports_dir / f"{p.stem}_{tag}{p.suffix}"
                    dask.visualize(result, filename=str(batch_graph_path))

                if self.kwargs.get("report") is True:
                    report_path = setup_dask_reports(output_folder, tile_id=self._tile_id, slice_tag=tag)
                    # Avoid overwriting the same report file per batch
                    with performance_report(filename=report_path):
                        result.compute()
                else:
                    result.compute()
        else:
            result = xr.map_blocks(
                self._user_function_export_wrapper,
                ds,
                template=self._template_xarray,
            )

            if self.kwargs.get("visualize_task_graph"):
                import dask
                output_folder = Path(self.kwargs.get("output_folder", "tiles"))
                reports_dir = output_folder / "reports"
                reports_dir.mkdir(parents=True, exist_ok=True)
                
                graph_path = self.kwargs.get("visualize_task_graph")
                p = Path(graph_path)
                if self._tile_id:
                    graph_path = reports_dir / f"{p.stem}_tile_{self._tile_id}{p.suffix}"
                else:
                    graph_path = reports_dir / p.name
                dask.visualize(result, filename=str(graph_path))

            if self.kwargs.get("report") is True and self._tile_id:
                output_folder = Path(self.kwargs.get("output_folder"))
                report_path = setup_dask_reports(output_folder, tile_id=self._tile_id, slice_tag=None)
                with performance_report(filename=report_path):
                    result.compute() 

            elif self.kwargs.get("report") is True and self._tile_id is None:
                output_folder = Path(self.kwargs.get("output_folder"))
                report_path = setup_dask_reports(output_folder, tile_id=None, slice_tag=None)
                with performance_report(filename=report_path):
                    result.compute() 
            else:
                result.compute()

        #if self.kwargs.get("vrt"):
        #    self._export_vrt(data_source)
    
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
        #df_input = ds.to_dataframe().reset_index()
        def _pull_df():
            return ds.to_dataframe().reset_index()

        if self._data_source == "ee":
            df_input = self.ee_call_with_backoff(_pull_df)
        else:
            # local data sources just pull normally
            df_input = _pull_df()

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
                
        df_output = df_output.set_index(dims)
        ds_output = df_output.to_xarray()
        ds_transposed = self._format_dataset(ds, ds_output)
        for i, time_val in enumerate(ds_transposed[self._first_dim].values):
            time_tag = create_time_tag(time_val)
            slice_2d = ds_transposed.isel({self._first_dim: i})
            self._output_basename = self._create_output_basename(slice_2d, time_tag)
            self._compute_chunks_and_export(slice_2d)
        
        if self._data_source == "local":
            ds_final = self._format_back(ds_transposed)
            return ds_final

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
    
    def _format_back(self, ds_output):
        rename_dims = {}
        if 'x' in ds_output.dims:
            rename_dims['x'] = 'X'
        if 'y' in ds_output.dims:
            rename_dims['y'] = 'Y'
        ds_renamed = ds_output.rename(rename_dims)
        return ds_renamed

    def _create_output_basename(self, slice_2d, time_tag):
        """
        Short deterministic basename:
        tile_<tile_id>__<first_dim>_<time>
        """
        x0 = float(slice_2d.x.min())
        x1 = float(slice_2d.x.max())
        y0 = float(slice_2d.y.min())
        y1 = float(slice_2d.y.max())

        x0, x1, y0, y1 = map(lambda v: int(round(v)), (x0, x1, y0, y1))

        bbox_tag = f"x{x0}_{x1}_y{y0}_{y1}"

        # If the user needs to tile the data and loop through each Dask computation by tile
        # then a unique naming scheme for the files will be generated by tile.
        # Otherwise, export like normal w/o tile naming.
        if self._tile_id:
            return f"{bbox_tag}_tile_{self._tile_id}__{self._first_dim}_{time_tag}"
        else:
            return f"{bbox_tag}__{self._first_dim}_{time_tag}"

    def _compute_chunks_and_export(self, ds_transposed):
        """Export a single block (already chunked by Dask) using the appropriate method."""
        stacked = self._convert_to_multiband(ds_transposed)

        if self.kwargs.get('file_format') == "GTiff" and not self.kwargs.get("export_to_gcs"):
            self._export_to_geotiff(stacked)

        elif self.kwargs.get('file_format') == "GTiff" and self.kwargs.get('export_to_gcs'):
            self._export_to_gcs(stacked)
    
    def _convert_to_multiband(self, chunk_dataset):
        """Convert chunk dataset into a multi-band xarray DataArray."""
        stacked = chunk_dataset.to_array(dim="band")
        stacked = stacked.assign_coords(band=list(chunk_dataset.data_vars))

        if "spatial_ref" not in stacked.coords:
            print("CRS IS NOT SET! SET IT IN YOUR EARTH ENGINE CODE!")
        return stacked


    def _export_to_geotiff(self, stacked):
        original_output_folder = self.kwargs.get('output_folder', 'tiles')
        
        # Check for override (e.g. from Docker environment)
        override_output_folder = os.environ.get("ROBUSTRASTER_OVERRIDE_OUTPUT")
        
        if override_output_folder:
            output_folder = override_output_folder
            # print(f"[robustraster] Using override output folder: {output_folder}")
        else:
            output_folder = original_output_folder

        os.makedirs(output_folder, exist_ok=True)
        output_path = os.path.join(output_folder, f"{self._output_basename}.tif")

        band_names = list(stacked.band.values)

        # ------------------------------------------------------------------
        # ✅ FIX: Compute a snapped transform instead of using stacked.rio.transform()
        # ------------------------------------------------------------------

        # Pull x/y coordinate arrays (these are typically pixel centers)
        x = stacked.coords["x"].values
        y = stacked.coords["y"].values
        
        
        nx = stacked.sizes.get("x", 0)
        ny = stacked.sizes.get("y", 0)

        # Compute pixel resolution from coordinate spacing
        # (Use abs because y usually decreases)
        xres = float(np.abs(x[1] - x[0]))
        yres = float(np.abs(y[1] - y[0]))

        # If you want to force 30m always, uncomment this:
        # xres = yres = 30.0

        # Compute bounds using pixel-center coords -> convert to pixel-edge bounds
        xmin_center = float(x.min())
        xmax_center = float(x.max())
        ymin_center = float(y.min())
        ymax_center = float(y.max())

        # Convert from pixel centers to pixel edges
        xmin = xmin_center - (xres / 2.0)
        ymax = ymax_center + (yres / 2.0)

        # Snap origin to global grid (multiples of resolution)
        xmin_snapped = np.floor(xmin / xres) * xres
        ymax_snapped = np.ceil(ymax / yres) * yres

        # Build snapped affine transform
        transform = from_origin(xmin_snapped, ymax_snapped, xres, yres)

        # ------------------------------------------------------------------
        # ✅ Export GeoTIFF with snapped transform
        # ------------------------------------------------------------------

        with rasterio.open(
            output_path,
            "w",
            driver="GTiff",
            height=stacked.rio.height,
            width=stacked.rio.width,
            count=len(band_names),
            dtype=str(stacked.dtype),
            crs=stacked.rio.crs,
            transform=transform,
        ) as dst:
            for idx, name in enumerate(band_names, start=1):
                dst.write(stacked[idx - 1].values, indexes=idx)
                dst.set_band_description(idx, str(name))

            # Optional: update tags with the final snapped bounds (pixel edges)
            # Compute xmax/ymin based on snapped origin + raster dimensions
            xmax_snapped = xmin_snapped + (stacked.rio.width * xres)
            ymin_snapped = ymax_snapped - (stacked.rio.height * yres)

            dst.update_tags(
                x_min=float(xmin_snapped),
                x_max=float(xmax_snapped),
                y_min=float(ymin_snapped),
                y_max=float(ymax_snapped),
            )

        print(f"Exported: {output_path} with bands {band_names}")

    
    def _export_to_gcs(self, stacked):
        """Export dataset chunk to Google Cloud Storage as a COG."""
        gcs_credentials = self.kwargs.get('gcs_credentials', None) 
        fs = gcsfs.GCSFileSystem(token=gcs_credentials)
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