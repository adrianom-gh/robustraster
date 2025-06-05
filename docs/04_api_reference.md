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

### 2. `source: str`  
A string that defines the dataset source.  

- Use `"local"` for raster files on disk  
- Use `"ee"` for Earth Engine collections

---

### 3. `user_function: Callable[[pd.DataFrame], pd.DataFrame]`  
Your custom processing function.  
This function must:  

- Accept a `pandas.DataFrame`  
- Return a modified `DataFrame` with `x` and `y` columns preserved

---

### 4. `export_params: dict[str, Any]`  
A dictionary of export configuration options.  

Based on the value of `"flag"`:  

- `"GTiff"` (GeoTIFF export):  
  - `output_folder`: Path to save output tiles  
  - `vrt`: `True` to generate a VRT mosaic  

- `"GCS"` (Google Cloud Storage):  
  - `gcs_credentials`: Path to service account credentials JSON  
  - `gcs_bucket`: Name of the GCS bucket  
  - `gcs_folder`: (Optional) Folder within the bucket to store outputs  
  - `chunks`: (Optional) Custom Dask chunk size. If you are familar with Dask chunking and don't want to tune your function with `tune_function`, you can pass in your own chunk size

---

## Optional Parameters

### 5. `dataset_params: dict[str, Any]`  
Required only for Earth Engine datasets. Includes:

- `geometry`: Path to `.geojson`, `.shp`, `.gpkg`, zipped shapefiles, an ee.Geometry() or an ee.FeatureCollection() 
- `crs`: Coordinate reference system (e.g., `"EPSG:4326"`)  
- `scale`: Spatial resolution (e.g., `30` for 30 meters)  
- `projection`: An `ee.Projection` object  

---

### 6. `user_function_args: tuple`  
Positional arguments to be passed to your user-defined function.  
Defaults to `()`.
See Example 3 in [`02_quickstart.md`](./02_quickstart.md) for an example.

---

### 7. `user_function_kwargs: dict[str, Any]`  
Keyword arguments for your user-defined function.  
Defaults to `None`.
See Example 4 in [`02_quickstart.md`](./02_quickstart.md) for an example.

---

### 8. `preview_dataset: bool`
Set to `True` to display a small preview of the dataset before and after excecuting your function.
This allows users to inspect the structure and content of the data to ensure it behaves as expected prior to running a full computation.
Useful for debugging.
Defaults to `False`.

---

### 9. `tune_function: bool`  
Set to `True` to automatically find an appropriate chunk size for optimized processing.  
Defaults to `False`.
See [`05_tuning.md`](./05_tuning.md) for details.

---

### 10. `max_iterations: int`
If `tune_function=True`, you can set the amount of times the tuning process iterates to find an optimal chunk size.
Defaults to `None`.
See [`05_tuning.md`](./05_tuning.md) for details.

---

### 11. `dask_mode: str`  
Defines how to initialize the Dask cluster.  
Defaults to `"full"`.

- `"full"`: Use all available cores and memory  
- `"test"`: Single-threaded/single worker mode for debugging  
- `"custom"`: Requires `dask_kwargs`

---

### 12. `dask_kwargs: dict[str, Any]`  
Used only when `dask_mode="custom"`.  

Includes:

- `n_workers`: Number of workers  
- `threads_per_worker`: Threads per worker. Defaults to 1. I HIGHLY RECOMMEND NOT CHANGING THIS UNLESS YOU ARE ABSOLUTELY SURE YOU KNOW WHAT YOU ARE DOING!   
- `memory_limit`: RAM per worker (e.g., `"2GB"`)

---

### 13. `hooks: dict[str, Callable]`  
Allows you to inject functions at various stages of the run lifecycle.  

Hook keys include:

- `before_run`: Runs before any processing begins  
- `after_dataset_loaded`: Runs after dataset is loaded, but before your function runs 
- `after_run`: Runs after everything is complete  


`hooks` comes with one prebuilt `after_run` function: `preview_dataset_hook`. This will allow you to preview a portion of the data before and after the run to ensure the general structure of the data looks good before doing a full computation. `preview_dataset_hook` will terminate `run()` once it gives you the preview. Remove `preview_dataset_hook` from your `run()` parameters if you ready to run your function on the dataset.

See Example 2 in [`02_quickstart.md`](./02_quickstart.md) for an example of how to use a hook function.

---