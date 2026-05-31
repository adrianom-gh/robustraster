# Quick Start

## Try Out the Demo!

I highly recommend trying the demo in the GitHub repo. You don’t need a full understanding of the code yet, but briefly familiarizing yourself with it will make the rest of the documentation easier to follow.
---

## Example 1: Running a custom Pythonic function to compute NDVI using data from Google Earth Engine  and Exporting to Local Machine

In this example, we’ll export NDVI tiles to your local machine using Google Earth Engine data and a user-defined function.

1. **Define a custom function** called `compute_ndvi()` that accepts and returns a pandas DataFrame.  
   - **Note** Custom functions must accept and return pandas DataFrames! This is required for all user-defined functions in `robustraster`.

2. **Query Earth Engine for Landsat imagery**:
   - **Note** Don't forget to authenticate to Google Earth Engine first!
   - Use `.filterDate()` to select a time range.
   - Apply a cloud-masking function (`prep_sr_l8()`).
   - Select bands `SR_B4` and `SR_B5`.

3. **Specify the dataset parameters** such as geometry, CRS, and scale.
   - The geometry in this example is a GEOJSON file, but you can also use a SHAPEFILE, an Earth Engine FeatureColllection, or an Earth Engine Geometry. For more details on dataset parameters, see [`04_run_function.md`](./04_run_function.md)

4. **Call `run()`** with all parameters:
   - Set `dataset` to the Earth Engine ImageCollection.
   - Set our dataset parameters (export region, CRS, and scale) with `dataset_config`.
   - Set an optional `"tune_function": True` inside `function_tuning_config` to auto-optimize your function to your dataset in preparation for the full run. For more details on function tuning, see [`05_tuning.md`](./05_tuning.md) and [`03_what_is_dask.md`](./03_what_is_dask.md)
   - Set our export parameters (where to export, output folder name, export a VRT) using `export_config`.

---

```python
# 1. Define a custom function
def compute_ndvi(df):
    df["ndvi"] = (df["SR_B5"] - df["SR_B4"]) / (df["SR_B5"] + df["SR_B4"])
    return df

# 2. Query Earth Engine for Landsat imagery
import ee
ee.Initialize(opt_url='https://earthengine-highvolume.googleapis.com')

# Basic cloud masking algorithm
def prep_sr_l8(image):
    # Bit 0 - Fill
    # Bit 1 - Dilated Cloud
    # Bit 2 - Cirrus
    # Bit 3 - Cloud
    # Bit 4 - Cloud Shadow
    qa_mask = image.select('QA_PIXEL').bitwiseAnd(int('11111', 2)).eq(0)
    saturation_mask = image.select('QA_RADSAT').eq(0)

    # Apply scaling
    optical_bands = image.select('SR_B.*').multiply(0.0000275).add(-0.2)
    thermal_bands = image.select('ST_B.*').multiply(0.00341802).add(149.0)

    return (image.addBands(optical_bands, None, True)
                 .addBands(thermal_bands, None, True)
                 .updateMask(qa_mask)
                 .updateMask(saturation_mask))

ic = (
    ee.ImageCollection('LANDSAT/LC08/C02/T1_L2')
    .filterDate('2018-05-08', '2018-05-10')
    .map(prep_sr_l8)
    .select(['SR_B4', 'SR_B5'])
)

from robustraster import run

list_of_column_names = ["ndvi"]

run(
    dataset=ic,
    dataset_config={
        "geometry": r"path\to\geojson-file.geojson",
        "crs": "EPSG:3310",
        "scale": 30
    },
    user_function_config={
        "user_function": compute_ndvi,
        "user_function_args": (),
        "user_function_kwargs": {},
    },
    function_tuning_config={
        "output_column_names": list_of_column_names
    },
    export_config={
        "mode": "raster",
        "file_format": "GTiff",
        "output_folder": "output_folder_name",
        "vrt": True
    },
)
```

## Example 2: Preview the Dataset Before Doing a Full Run (Via Hooks)

In this example, we use `preview_dataset=True` to preview the dataset **before** performing a full run.  
This is useful to inspect your data structure and see what the DataFrame will look like before and after your custom function is applied.
For more information on hooks, see [`04_run_function.md`](./04_run_function.md)

```python
from robustraster import run

list_of_column_names = ["ndvi"]

def compute_ndvi(df):
    df["ndvi"] = (df["SR_B5"] - df["SR_B4"]) / (df["SR_B5"] + df["SR_B4"])
    return df

run(
    dataset=ic,
    preview_dataset=True,
    dataset_config={
    "geometry": r"path\to\geojson-file.geojson",
    "crs": "EPSG:3310",
    "scale": 30
},
    user_function_config={
        "user_function": compute_ndvi,
        "user_function_args": (),
        "user_function_kwargs": {},
    },
    function_tuning_config={
        "output_column_names": list_of_column_names
    },
    export_config={
        "mode": "raster",
        "file_format": "GTiff", 
        "output_folder": "output_folder_name", 
        "vrt": True
    }
)
```

**Sample output preview:**

```text
Dataset preview:
                     time              X             Y  SR_B4  SR_B5
0 2018-05-09 18:38:10.899 -145860.087241  151568.60228    NaN    NaN
1 2018-05-09 18:38:10.899 -145860.087241  151598.60228    NaN    NaN
2 2018-05-09 18:38:10.899 -145860.087241  151628.60228    NaN    NaN
3 2018-05-09 18:38:10.899 -145860.087241  151658.60228    NaN    NaN
4 2018-05-09 18:38:10.899 -145860.087241  151688.60228    NaN    NaN

User function output preview:
                     time              X             Y  SR_B4  SR_B5  ndvi
0 2018-05-09 18:38:10.899 -145860.087241  151568.60228    NaN    NaN   NaN
1 2018-05-09 18:38:10.899 -145860.087241  151598.60228    NaN    NaN   NaN
2 2018-05-09 18:38:10.899 -145860.087241  151628.60228    NaN    NaN   NaN
3 2018-05-09 18:38:10.899 -145860.087241  151658.60228    NaN    NaN   NaN
4 2018-05-09 18:38:10.899 -145860.087241  151688.60228    NaN    NaN   NaN
```

## Example 3: Exporting to Local Machine + Using a Function with Positional Arguments Using a Shapefile For `"geometry"`

I modified my `compute_ndvi()` function to include a positional argument.
If your function requires positional arguments, this example shows how to use them.
I have also modified `"geometry"` to a path to a shapefile instead of a GEOJSON.

```python
list_of_column_names = ["ndvi"]

def compute_ndvi(df, number_to_add):
    df["ndvi"] = (df["SR_B5"] - df["SR_B4"] + number_to_add) / (df["SR_B5"] + df["SR_B4"])
    return df

run(
    dataset=ic,
    dataset_config={
        "geometry": r"path\to\shapefile.shp",
        "crs": "EPSG:3310",
        "scale": 30
},
    user_function_config={
        "user_function": compute_ndvi,
        "user_function_args": (666,),
        "user_function_kwargs": {},
    },
    function_tuning_config={
        "output_column_names": list_of_column_names
    },
    export_config={
        "mode": "raster",
        "file_format": "GTiff",
        "output_folder": "output_folder_name"
    },
)
```
## Example 4: Exporting to Local Machine + Using a Function with Keyword Arguments + Using a FeatureCollection For `"geometry"`

I show how to use keyword arguments here.
I have also modified `"geometry"` to pass in an EarthEngine FeatureCollection object.

```python
list_of_column_names = ["ndvi"]

def compute_ndvi(df, number_to_add):
    df["ndvi"] = (df["SR_B5"] - df["SR_B4"] + number_to_add) / (df["SR_B5"] + df["SR_B4"])
    return df

run(
    dataset=ic,
    dataset_config={
        "geometry": ee.FeatureCollection("path/to/fc"),
        "crs": "EPSG:3310",
        "scale": 30
},
    user_function_config={
        "user_function": compute_ndvi,
        "user_function_args": (),
        "user_function_kwargs": {"number_to_add": 666},
    },
    function_tuning_config={
        "output_column_names": list_of_column_names
    },
    export_config={
        "mode": "raster",
        "file_format": "GTiff",
        "output_folder": "output_folder_name"
    },
)
```
---

## Example 5: Exporting to Google Cloud Storage with Custom Dask Configuration

In this example, we export the NDVI results to **Google Cloud Storage** instead of the local disk.  
We also manually configure a custom Dask cluster. More info on Dask and clusters can be found in [`03_what_is_dask.md`](./03_what_is_dask.md).

---

### Steps

1. **Re-use the same `compute_ndvi()` function** from Example 1.

2. **Use the same Earth Engine image collection** setup and `dataset_config` from Example 1.

3. **Set up `export_config` for GCS**:
   - `export_to_gcs = True` tells the exporter to use Google Cloud Storage.

   - Specify the bucket and folder to export to (`gcs_bucket`, `gcs_folder`).

4. **Configure Dask manually using `dask_config`**:
   - Define number of workers, threads, and memory per worker.
   - Set `dask_mode = "custom"` to activate these settings.

5. **Run the pipeline** using the `run()` function with all the values above.

---

```python
from robustraster import run

run(
    dataset=ic,
    dataset_config={
        "geometry": r"path\to\geojson-file.geojson",
        "crs": "EPSG:3310",
        "scale": 30
    },
    user_function_config={
        "user_function": compute_ndvi,
        "user_function_args": (),
        "user_function_kwargs": {},
    },
    export_config={
        "mode": "raster",
        "file_format": "GTiff",
        "export_to_gcs": True,

        "gcs_bucket": "test-bucket",
        "gcs_folder": "test-folder"
    },
    dask_mode="custom",
    dask_config={
        "n_workers": 6,
        "threads_per_worker": 1,
        "memory_limit": "3g"
    }
)
```

---

## Example 6: Using Local Raster Data, running an NDVI function, and exporting to Google Cloud Storage

In this example, we export the NDVI results to **Google Cloud Storage** instead of the local disk.  
We also manually configure a custom Dask cluster. More info on Dask and clusters can be found in [`03_what_is_dask.md`](./03_what_is_dask.md).

---

### Steps

1. **Re-use the same `compute_ndvi()` function** from Example 1.

2. **Use the same Earth Engine image collection** setup and `dataset_config` from Example 1.

3. **Set up `export_config` for GCS**:
   - `export_to_gcs = True` tells the exporter to use Google Cloud Storage.

   - Specify the bucket and folder to export to (`gcs_bucket`, `gcs_folder`).

4. **Configure Dask manually using `dask_config`**:
   - Define number of workers, threads, and memory per worker.
   - Set `dask_mode = "custom"` to activate these settings.

5. **Run the pipeline** using the `run()` function with all the values above.

---

```python
from robustraster import run

run(
    dataset=ic,
    dataset_config={
        "geometry": r"path\to\geojson-file.geojson",
        "crs": "EPSG:3310",
        "scale": 30
    },
    user_function_config={
        "user_function": compute_ndvi,
        "user_function_args": (),
        "user_function_kwargs": {},
    },
    export_config={
        "mode": "raster",
        "file_format": "GTiff",
        "export_to_gcs": True,

        "gcs_bucket": "test-bucket",
        "gcs_folder": "test-folder"
    },
    dask_mode="custom",
    dask_config={
        "n_workers": 6,
        "threads_per_worker": 1,
        "memory_limit": "3g"
    }
)
```

---

## Example 7: Process Local Raster Data and Upload Results to a Google Earth Engine Asset

In this example, we'll process a year's worth of Landsat imagery (2018) saved directly on your local machine using our Python `compute_ndvi` function. Then, we use the `upload_results_to_gee` flag to automatically export the processed results directly to your Google Cloud Storage bucket and convert them into a Google Earth Engine Asset for visualization!

```python

# Import necessary libraries
from robustraster import run

# Your UDF goes here. It must accept a pandas DataFrame and return a pandas DataFrame.
def compute_ndvi(df):
    df["ndvi"] = (df["SR_B5"] - df["SR_B4"]) / (df["SR_B5"] + df["SR_B4"])
    return df

# Path to your local 2018 Landsat VRT file
2018_vrt_file = r"path\to\local\landsat\2018\2018.vrt"
list_of_column_names = ["ndvi"]

run(
    dataset=2018_vrt_file,
    user_function_config={
        "user_function": compute_ndvi,
        "user_function_args": (),
        "user_function_kwargs": {},
    },
    function_tuning_config={
        "output_column_names": list_of_column_names
    },
    export_config={
        "mode": "raster",
        "file_format": "GTiff",
        "output_folder": "output_folder_name",
        "export_to_gcs": True,

        "gcs_bucket": "test-bucket",
        "gcs_folder": "ndvi-2018-exports",
        "upload_results_to_gee": True,
        "gee_asset_path": "projects/my-earth-engine-project/assets/ndvi_yearly_collection"
    }
)
```

---

## Example 8: Process Earth Engine Data using R and Docker, Exporting to Google Cloud Storage

In this example, we mimic the previous scenario but this time query Earth Engine directly for 2018 Landsat imagery cropped to the Plumas National Forest. We demonstrate how to execute **R code** within a Docker container to perform processing at scale via Dask. Results are exported gracefully to Google Cloud Storage.

```python
import ee
from robustraster import run

ee.Initialize(opt_url='https://earthengine-highvolume.googleapis.com')

# Basic cloud masking algorithm 
def prep_sr_l8(image):
    qa_mask = image.select('QA_PIXEL').bitwiseAnd(int('11111', 2)).eq(0)
    saturation_mask = image.select('QA_RADSAT').eq(0)
    
    optical_bands = image.select('SR_B.*').multiply(0.0000275).add(-0.2)
    thermal_bands = image.select('ST_B.*').multiply(0.00341802).add(149.0)
    
    return (image.addBands(optical_bands, None, True)
                 .addBands(thermal_bands, None, True)
                 .updateMask(qa_mask)
                 .updateMask(saturation_mask))

# Target the Plumas National Forest Boundaries for 2018
Plumas_Boundaries = ee.FeatureCollection("projects/robust-raster/assets/boundaries/Plumas_National_forest")
ic = (
    ee.ImageCollection('LANDSAT/LC08/C02/T1_L2')
    .filterDate('2018-01-01', '2018-12-31')
    .map(prep_sr_l8)
    .select(['SR_B4', 'SR_B5'])
)

# 1. Define the R-based User Function
r_code = """\
compute_ndvi_r <- function(df) {
    df$ndvi <- (df$SR_B5 - df$SR_B4) / (df$SR_B5 + df$SR_B4)
    return(df[, c("time", "X", "Y", "ndvi")])
}
"""

list_of_column_names = ["ndvi"]
chunks = {"time": 1, "X": 2048, "Y": 2048}

# 2. Run configured strictly over Docker with R Integration
run(
    dataset=ic,
    dataset_config={
        'geometry': Plumas_Boundaries,
        'crs': 'EPSG:3310',
        'scale': 30,
    },
    user_function_config={
        "is_r_function": True,
        "r_function_code": r_code,
        "r_function_name": "compute_ndvi_r",
    },
    function_tuning_config={
        "chunks": chunks,
        "output_column_names": list_of_column_names
    },
    export_config={
        "mode": "raster",
        "file_format": "GTiff",
        "output_folder": "Plumas_NDVI_Tiles_R_Docker",
        "vrt": True,
        "report": True,
        "export_to_gcs": True,

        "gcs_bucket": "test-bucket",
    },
    dask_mode="custom",
    dask_config={
        "n_workers": 6,
        "threads_per_worker": 1,
        "memory_limit": "3g",
    },

    docker_image="adrianomdocker/r042"
)
```