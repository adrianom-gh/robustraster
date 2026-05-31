# `run()` Function

The `run()` function is the main entry point for processing raster data with your custom logic, whether you're working with Earth Engine or local GeoTIFFs (local rasters is broken at the moment. I will update this once it's fixed!).

It handles:
- Dataset loading
- Dask cluster setup
- Running your user-defined function
- Exporting results
- Optional performance tuning

---

## Required Parameters

### 1. `dataset: str | list[str] | ee.ImageCollection`  
The input raster source.  

- For **local rasters**: A file path string or list of file path strings  
- For **Earth Engine**: An `ee.ImageCollection` object

---

### 2. `user_function_config: dict[str, Any]`
A dictionary of user function configuration options. 

- `user_function: Callable[[pd.DataFrame], pd.DataFrame]`: Name of the user function to be used for processing the dataset.  
- `user_function_args: tuple`: Tuple of user function arguments to pass into `user_function`.
- `user_function_kwargs: dict[str, Any]`: Dictionary of user function keyword arguments to pass into `user_function`.
- `is_r_function: bool`: Set to `True` if you are executing R code instead of Python. Requires `dask_use_docker` to be `True`.
- `r_function_code: str`: The R script code as a string (required if `is_r_function=True`).
- `r_function_name: str`: The name of the main R function to execute (required if `is_r_function=True`).

---

### 3. `export_config: dict[str, Any]`  
A dictionary of export configuration options.   

- `mode: str`: `raster` for raster export and `vector` for vector export.  
- `file_format: str`: `GTiff` for GeoTIFF export. `CSV` and `parquet` for vector export. 
- `output_folder: str`: Folder name to save outputs. 
- `vrt: bool`: `True` to generate a VRT mosaic. Only available for `raster` mode. Cannot be used if exporting results to the cloud.

- `export_to_gcs: bool`: True to export results to GCS. 
  - `gcs_bucket: str`: Name of the GCS bucket  
  - `gcs_folder: str`: (Optional) Folder within the bucket to store outputs
  - `gcs_project: str`: (Optional) Google Cloud Project ID. Required if it cannot be determined automatically.
  - `gcs_credentials: str`: (Optional) Path to service account credentials JSON. By default, `robustraster` uses Application Default Credentials.

- `upload_results_to_gee: bool`: True to automate generating an Earth Engine manifest and uploading the results from GCS to Earth Engine. Requires `export_to_gcs` to be True.
  - `gee_asset_path: str`: The full path to the Earth Engine Folder or ImageCollection to upload into (e.g., `projects/my-project/assets/my_ndvi_collection`). The engine will automatically append the time tag (e.g., the year) to the asset path for each generated image.

> [!IMPORTANT]
> **Prerequisites for GCS & GEE Uploads**
> To securely upload results, `robustraster` uses Application Default Credentials (ADC). Before running your code, you must:
> 1. Install the Google Cloud SDK (gcloud CLI).
> 2. Run `gcloud auth application-default login` in your terminal.
> 3. Ensure your Google Cloud account has the required permissions (e.g., Storage Object Admin) to upload to your specified bucket.
> 4. Authenticate to Earth Engine using `ee.Authenticate()` as normal.

---

## Optional Parameters

### 4. `function_tuning_config: dict[str, Any]`
A dictionary of function tuning configuration options.

- `tune_function: bool`: Set to `True` to automatically find an appropriate chunk size for optimized processing. Defaults to `False`. See [`05_tuning.md`](./05_tuning.md) for details.
- `chunks: tuple | dict`: Tuple or dictionary of chunk sizes for optimized processing. See [`03_what_is_dask.md`](./03_what_is_dask.md) for details.
- `max_iterations: int | None`: If `tune_function=True`, you can set the amount of times the tuning process iterates to find an optimal chunk size. Defaults to `None`. See [`05_tuning.md`](./05_tuning.md) for details.
- `output_column_names: list[str]`: (Optional) List of column names that will be in the output result. As of recent updates, this is now inferred automatically!


### 5. `dataset_config: dict[str, Any]`  
Required only for Earth Engine datasets. Includes:

- `geometry: str | ee.Geometry | ee.FeatureCollection`: Path to `.geojson`, `.shp`, `.gpkg`, zipped shapefiles, an ee.Geometry() or an ee.FeatureCollection() 
- `crs: str`: Coordinate reference system (e.g., `"EPSG:4326"`)  
- `scale: int | float`: Spatial resolution (e.g., `30` for 30 meters)  

---

### 6. `max_pixels_per_tile: int`
The maximum number of pixels to process per chunk/tile when pulling data from Earth Engine.
Defaults to `1_000_000`.

---

### 7. `preview_dataset: bool`
Set to `True` to display a small preview of the dataset before and after excecuting your function.
This allows users to inspect the structure and content of the data to ensure it behaves as expected prior to running a full computation.
Useful for debugging.
Defaults to `False`.

---

### 8. `dask_mode: str`  
Defines how to initialize the Dask cluster.  
Defaults to `"full"`.

- `"full"`: Use all available cores and memory  
- `"test"`: Single-threaded/single worker mode for debugging  
- `"custom"`: Requires `dask_config`

---

### 9. `dask_config: dict[str, Any]`  
Used only when `dask_mode="custom"`.  

- `n_workers: int`: Number of workers  
- `threads_per_worker: int`: Threads per worker. Defaults to 1. I HIGHLY RECOMMEND NOT CHANGING THIS UNLESS YOU ARE ABSOLUTELY SURE YOU KNOW WHAT YOU ARE DOING!   
- `memory_limit: str`: RAM per worker (e.g., `"2g"`)

---

### 10. `docker_image: str`
The Docker image to use for the Dask workers if you want to run the Dask cluster using Docker containers. Required for executing R code.

---

### 11. `dask_docker_kwargs: dict[str, Any]`
Additional keyword arguments to pass to the Docker container initialization (e.g., configurations specific to the docker-py package or passing environment configs).

---

### 12. `hooks: dict[str, Callable]`  
Allows you to inject functions at various stages of the run lifecycle.  

Hook keys include:

- `before_run: Callable`: Runs before any processing begins  
- `after_dataset_loaded: Callable`: Runs after dataset is loaded, but before your function runs 
- `after_run: Callable`: Runs after everything is complete  


`preview_dataset` will allow you to preview a portion of the data before and after the run to ensure the general structure of the data looks good before doing a full computation. `preview_dataset` will terminate `run()` once it gives you the preview. Remove `preview_dataset` from your `run()` parameters if you ready to run your function on the dataset.

---