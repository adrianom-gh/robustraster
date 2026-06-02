from __future__ import annotations
from pathlib import Path
from typing import Sequence, Mapping, Optional
import pandas as pd
import xarray as xr
import rioxarray as rxr


from abc import ABC, abstractmethod
import xarray as xr
import rasterio
import rioxarray
import ee
import numpy as np
import pandas as pd
import geopandas as gpd
import json

class DataReaderInterface(ABC):
    @abstractmethod
    def _read_data(self):
        """
        Abstract method to read raster data from the source.
        This method should be implemented in the derived classes.
        """
        def test_read_data_not_implemented_error(self):
            # Test that instantiating DataReaderInterface directly raises a TypeError
            with self.assertRaises(TypeError):
                reader = DataReaderInterface()  # This should raise a TypeError

class RasterDataset(DataReaderInterface):
    """
    A reader for local raster files.

    This class provides functionality to read raster data from a specified file path
    using xarray and rioxarray. It is intended for handling raster data stored locally
    and converts it into an xarray.Dataset for further analysis.
    
    Any private attributes or methods (indicated with an underscore at the start of its name) 
    are not intended for use by the user. Documentation is provided should the user want to 
    delve deeper into how the class works, but it is not a requirement.

    Public Methods (these are functions that are openly available for users to use):
    - dataset: A property that obtains the dataset's metadata.

    Private Methods (these are functions that the user will NOT use that are called behind the scenes):
    - _read_data: A private method that reads the user's dataset into an xarray Dataset once the user 
                  instantiates the class.

    To instantiate an object of type RasterDataset:
    >>> reader = RasterDataset("/path/to/raster.tif")

    If you would like to pass in multiple raster files:
    >>> from robustraster import input_driver
    >>> raster_path_list = ['./raster1.tif', './raster2.tif']
    >>> local_raster = input_driver.RasterDataset(raster_path_list)
    """
    def __init__(self, file_path: str, dataset_config: dict) -> None:
        """
        Initialize a RasterDataset instance.

        Parameters:
        - file_path (str): The absolute path to the raster file.

        Example:
        >>> reader = RasterDataset("/path/to/raster.tif")
        """
        self._file_path = file_path
        self._xarray_data = self._read_data(dataset_config)
       # self._xarray_data = self._read_timeseries_geotiffs_to_dataset(dataset_config)
    
    @property
    def dataset(self):
        """
        A property meant to retrieve the xarray Dataset stored in _xarray_data.

        Example:
        >>> local_raster = dataset_manager.RasterDataset('./raster.tif')
        >>> dataset = local_raster.dataset
        >>> print(dataset)
        """
        return self._xarray_data

    def _read_data(self, dataset_config=None) -> xr.Dataset:
        """
        Read one or many rasters and return an xarray.Dataset with variables on (time, X, Y).

        dataset_config (optional):
        - chunks: dict, e.g. {"X": 1024, "Y": 1024}
        - time_dim: str, default "time"
        - band_names: {int: str} mapping (1-based band index -> var name)
        - prefer_desc_over_bandnames: bool, default True
        """
        import numpy as np
        import xarray as xr
        import rasterio
        import rioxarray as rxr

        dataset_config = dataset_config or {}

        # Inputs -> list
        raster_paths = [self._file_path] if isinstance(self._file_path, str) else list(self._file_path)
        if not raster_paths:
            raise ValueError("No raster paths were provided.")
            
        # Normalize paths to use forward slashes for cross-platform compatibility (e.g. Windows -> Docker Linux workers)
        raster_paths = [p.replace('\\', '/') for p in raster_paths]

        # Config
        chunks = dataset_config.get("chunks", {})
        CHUNK_X = int(chunks.get("X", 1024))
        CHUNK_Y = int(chunks.get("Y", 1024))
        time_dim = dataset_config.get("time_dim", "time")
        band_names_override = dataset_config.get("band_names", {})
        prefer_desc = bool(dataset_config.get("prefer_desc_over_bandnames", True))

        # Try to guess timestamps from file names
        import re
        from pathlib import Path
        
        def _guess_timestamp_from_name(fp: str):
            bn = Path(fp).name
            for pat in [
                r"(?P<y>\d{4})(?P<m>\d{2})(?P<d>\d{2})T(?P<H>\d{2})(?P<M>\d{2})(?P<S>\d{2})", # YYYYMMDDTHHMMSS
                r"(?P<y>\d{4})(?P<m>\d{2})(?P<d>\d{2})", # YYYYMMDD
                r"(?P<y>\d{4})[-_](?P<m>\d{2})[-_](?P<d>\d{2})", # YYYY-MM-DD or YYYY_MM_DD
            ]:
                m = re.search(pat, bn)
                if m:
                    gd = m.groupdict()
                    if 'H' in gd:
                        return f"{gd['y']}-{gd['m']}-{gd['d']}T{gd['H']}:{gd['M']}:{gd['S']}"
                    else:
                        return f"{gd['y']}-{gd['m']}-{gd['d']}"
            return None

        guessed = [_guess_timestamp_from_name(fp) for fp in raster_paths]
        if all(g is not None for g in guessed):
            ts_list = [np.datetime64(g) for g in guessed]
        else:
            ts_list = list(range(len(raster_paths)))

        # Open one file and package as Dataset on (time, X, Y)
        def _open_package(fp: str, tstamp) -> xr.Dataset:
            # Inspect band metadata
            with rasterio.open(fp) as src:
                count = src.count
                descs = tuple(src.descriptions or ())
                crs = src.crs
                #print(crs)

            # Open lazily with Dask-friendly chunks
            da_or_ds = rxr.open_rasterio(fp, masked=True, chunks={"x": CHUNK_X, "y": CHUNK_Y})

            # Normalize to per-band variables and drop the scalar 'band' coord to avoid MergeError
            if isinstance(da_or_ds, xr.DataArray):
                if "band" in da_or_ds.dims:
                    vars_ = {}
                    for b in range(1, count + 1):
                        band_da = da_or_ds.sel(band=b)
                        if "band" in band_da.coords:
                            band_da = band_da.drop_vars("band")
                        band_da = band_da.squeeze(drop=True)
                        vars_[f"band_{b}"] = band_da
                    ds = xr.Dataset(vars_)
                else:
                    ds = xr.Dataset({"band_1": da_or_ds})
            else:
                ds = da_or_ds
                for v in list(ds.data_vars):
                    if "band" in ds[v].coords:
                        ds[v] = ds[v].drop_vars("band").squeeze(drop=True)

            # Name bands: overrides > descriptions
            name_map = {}
            for b in range(1, count + 1):
                key = f"band_{b}"
                if key in ds:
                    if b in band_names_override:
                        name_map[key] = str(band_names_override[b])
                    elif prefer_desc and len(descs) >= b and descs[b - 1]:
                        name_map[key] = descs[b - 1].strip().replace(" ", "_")
            if name_map:
                ds = ds.rename(name_map)

            # Ensure CRS on each variable (helps keep georeferencing intact)
            for v in ds.data_vars:
                try:
                    if getattr(ds[v].rio, "crs", None) is None and crs is not None:
                        ds[v] = ds[v].rio.write_crs(crs, inplace=False)
                except Exception:
                    pass

            # Rename spatial dims to X/Y and add integer time
            rename_dims = {}
            if "x" in ds.dims: rename_dims["x"] = "X"
            if "y" in ds.dims: rename_dims["y"] = "Y"
            if rename_dims:
                ds = ds.rename(rename_dims)
            
            # Sort coordinate values to fix Panda issue "ValueError"
            if "X" in ds.dims:
                x = ds["X"]
                if x[0] > x[-1]:
                    ds = ds.sortby("X", ascending=True)
            if "Y" in ds.dims:
                y = ds["Y"]
                if y[0] < y[-1]:
                    ds = ds.sortby("Y", ascending=False)

            ds = ds.expand_dims({time_dim: [tstamp]})

            # Chunk spatial dims
            for v in ds.data_vars:
                ds[v] = ds[v].chunk({k: {"X": CHUNK_X, "Y": CHUNK_Y}.get(k, -1) for k in ds[v].dims if k in ("X", "Y")})

            ds.attrs.update({
            "crs": crs,
            })
            return ds

        # Process all files
        per_time = []
        for fp, ts in zip(raster_paths, ts_list):
            try:
                per_time.append(_open_package(fp, ts))
            except rasterio.errors.RasterioIOError as e:
                raise RuntimeError(f"Failed to read raster: {fp}") from e

        # Defensive cleanup: purge any stray 'band' coords before concat
        for i in range(len(per_time)):
            for v in per_time[i].data_vars:
                if "band" in per_time[i][v].coords:
                    per_time[i][v] = per_time[i][v].drop_vars("band").squeeze(drop=True)

        # Concatenate along time and sort
        combined = xr.concat(per_time, dim=time_dim)
        if np.issubdtype(combined[time_dim].dtype, np.integer) or np.issubdtype(combined[time_dim].dtype, np.datetime64):
            combined = combined.sortby(time_dim)

        # Synthesize X/Y coords if missing (from affine)
        for coord_dim in ("X", "Y"):
            if coord_dim not in combined.coords and coord_dim in combined.dims:
                sample = combined[list(combined.data_vars)[0]]
                try:
                    transform = sample.rio.transform()
                    ny, nx = sample.sizes["Y"], sample.sizes["X"]
                    xs = np.arange(nx) * transform.a + transform.c + transform.a / 2.0
                    ys = np.arange(ny) * transform.e + transform.f + transform.e / 2.0
                    combined = combined.assign_coords(X=("X", xs), Y=("Y", ys))
                except Exception:
                    pass
    
        return combined

class EarthEngineDataset(DataReaderInterface):
    """
    A reader for Google Earth Engine data.

    This class is an extension of xee (link to the package: https://github.com/google/Xee) that reads data
    from Google Earth Engine into an xarray object. It is intended to make reading data from Google Earth
    Engine to your machine a bit easier, without necessarily having to learn the xarray data structure.

    Any private attributes or methods (indicated with an underscore at the start of its name) 
    are not intended for use by the user. Documentation is provided should the user want to 
    delve deeper into how the class works, but it is not a requirement.

    Attributes:
    - _xarray_data: A private attribute of the user's queried Earth Engine data stored into an xarray object.

    - _max_chunks_limit: A private attribute that requires no user interference. This is meant to determine the
                         maximum amount of data we can pull from Google Earth Engine per request.

    Public Methods (these are functions that are openly available for users to use):

    - dataset: A property meant to retrieve the xarray Dataset stored in _xarray_data.

    - get_max_chunks_limit: A property that requires no user interference. This is called if the user wants to use
                            the tuning functionality of this package. However, the user does not need to understand
                            how to use this function (unless they choose to set a chunk size themselves).

    Private Methods (these are functions that the user will NOT use that are called behind the scenes):

    - _get_data_type_in_bytes: A private method that obtains the data type of the Earth Engine bands (stored as "data variables" in
                               the xarray object).
    
    - _auto_compute_max_chunks: A private method that stores the maximum amount of data we can pull from Google Earth Engine per
                                request into "_max_chunks_limit".
    
    - _construct_ee_collection: A private method that constructs an Earth Engine ee.ImageCollection based on the user's specified
                                parameters (see the Example below and the docstring for __init__ on how to specify parameters).
    
    - _read_data: A private method that uses xee to read the data query from Earth Engine into an xarray object.
    
    To instantiate an EarthEngineDataset object, the user must pass in a dictionary object of parameters. Below is an example
    `parameters` variable. 

    >>> parameters = {
    >>>     'collection': 'LANDSAT/LC08/C02/T1_L2',
    >>>     'bands': ['SR_B4', 'SR_B5'],
    >>>     'start_date': '2020-05-01',
    >>>     'end_date': '2020-08-31',
    >>>     'geometry': WSDemo.geometry(),
    >>>     'crs': 'EPSG:3310',
    >>>     'scale': 30,
    >>>     'map_function': prep_sr_l8
    >>> }

    Where:
    `collection` is the Earth Engine path to the image collection of interest.
    `bands` is the bands the user would like to export from Earth Engine.
    `start_date` / `end_date` is the date range to filter the image collection.
    `geometry` is the geometry object that will be used to clip the image collection.
    `crs` is the coordinate system to project the image collection to.
    `scale` is the spatial resolution the user would like the image collection to be.

    For more information on these parameters, see the documentation for Earth Engine's export
    functions (link to one here: https://developers.google.com/earth-engine/apidocs/export-image-todrive)

    `map_function` is the name of the function the user would like to run on an image 
    collection before exporting the data. See the example usage below to see how this 
    is used.

    Example usage for integrating Earth Engine with a custom cloud masking algorithm:
    1. Import required libraries and modules: 
    >>> from robustraster import dataset_manager
    >>> import ee
    >>> import json

    2. Authenticate and initialize Earth Engine:
    >>> with open(json_key, 'r') as file:
    >>>     data = json.load(file)
    >>> credentials = ee.ServiceAccountCredentials(data["client_email"], json_key)
    >>> ee.Initialize(credentials=credentials, opt_url='https://earthengine-highvolume.googleapis.com')

    3. Define a cloud masking algorithm for Landsat 8 Surface Reflectance:
    >>> def prep_sr_l8(image):
    >>>     # Bit 0 - Fill
    >>>     # Bit 1 - Dilated Cloud
    >>>     # Bit 2 - Cirrus
    >>>     # Bit 3 - Cloud
    >>>     # Bit 4 - Cloud Shadow
    >>>     qa_mask = image.select('QA_PIXEL').bitwiseAnd(int('11111', 2)).eq(0)
    >>>     saturation_mask = image.select('QA_RADSAT').eq(0)

    >>>     # Apply scaling factors to appropriate bands
    >>>     optical_bands = image.select('SR_B.*').multiply(0.0000275).add(-0.2)
    >>>     thermal_bands = image.select('ST_B.*').multiply(0.00341802).add(149.0)

    >>>     # Return the processed image
    >>>     return (image.addBands(optical_bands, None, True)
    >>>                 .addBands(thermal_bands, None, True)
    >>>                 .updateMask(qa_mask)
    >>>                 .updateMask(saturation_mask))

    4. Prepare parameters for data processing:
    >>> WSDemo = ee.FeatureCollection("projects/robust-raster/assets/boundaries/WSDemoSHP_Albers")
    >>> test_parameters = {
    >>>     'collection': 'LANDSAT/LC08/C02/T1_L2',
    >>>     'bands': ['SR_B4', 'SR_B5'],
    >>>     'start_date': '2020-05-01',
    >>>     'end_date': '2020-08-31',
    >>>     'geometry': WSDemo.geometry(),
    >>>     'crs': 'EPSG:3310',
    >>>     'scale': 30,
    >>>     'map_function': prep_sr_l8
    >>> }

    5. Create the EarthEngineDataset object:
    >>> earth_engine = dataset_manager.EarthEngineDataset(parameters=test_parameters)

    6. Print the contents of the data:
    >>> print(earth_engine.dataset)
    """

    def __init__(self, image_collection: ee.imagecollection.ImageCollection, dataset_config: dict) -> None:
        """
        Instantiate the EarthEngineDataset class. To instantiate an EarthEngineDataset object, 
        the user must pass in a dictionary object of parameters. Below is an example
        `test_parameters` variable. 

        >>> test_parameters = {
        >>>     'collection': 'LANDSAT/LC08/C02/T1_L2',
        >>>     'bands': ['SR_B4', 'SR_B5'],
        >>>     'start_date': '2020-05-01',
        >>>     'end_date': '2020-08-31',
        >>>     'geometry': WSDemo.geometry(),
        >>>     'crs': 'EPSG:3310',
        >>>     'scale': 30,
        >>>     'map_function': prep_sr_l8
        >>> }

        Where:
        `collection` is the Earth Engine path to the image collection of interest.
        `bands` is the bands the user would like to export from Earth Engine.
        `start_date` / `end_date` is the date range to filter the image collection.
        `geometry` is the geometry object that will be used to clip the image collection.
        `crs` is the coordinate system to project the image collection to.
        `scale` is the spatial resolution the user would like the image collection to be.

        For more information on these parameters, see the documentation for Earth Engine's export
        functions (link to one here: https://developers.google.com/earth-engine/apidocs/export-image-todrive)

        `map_function` is the name of the function the user would like to run on an image 
        collection before exporting the data. See the example usage below to see how this 
        is used.
        
        Example usage for integrating Earth Engine with a custom cloud masking algorithm:
        1. Import required libraries and modules: 
        >>> from robustraster import dataset_manager
        >>> import ee
        >>> import json

        2. Authenticate and initialize Earth Engine:
        >>> with open(json_key, 'r') as file:
        >>>     data = json.load(file)
        >>> credentials = ee.ServiceAccountCredentials(data["client_email"], json_key)
        >>> ee.Initialize(credentials=credentials, opt_url='https://earthengine-highvolume.googleapis.com')

        3. Define a cloud masking algorithm for Landsat 8 Surface Reflectance:
        >>> def prep_sr_l8(image):
        >>>     # Bit 0 - Fill
        >>>     # Bit 1 - Dilated Cloud
        >>>     # Bit 2 - Cirrus
        >>>     # Bit 3 - Cloud
        >>>     # Bit 4 - Cloud Shadow
        >>>     qa_mask = image.select('QA_PIXEL').bitwiseAnd(int('11111', 2)).eq(0)
        >>>     saturation_mask = image.select('QA_RADSAT').eq(0)

        >>>     # Apply scaling factors to appropriate bands
        >>>     optical_bands = image.select('SR_B.*').multiply(0.0000275).add(-0.2)
        >>>     thermal_bands = image.select('ST_B.*').multiply(0.00341802).add(149.0)

        >>>     # Return the processed image
        >>>     return (image.addBands(optical_bands, None, True)
        >>>                 .addBands(thermal_bands, None, True)
        >>>                 .updateMask(qa_mask)
        >>>                 .updateMask(saturation_mask))

        4. Prepare parameters for data processing:
        >>> WSDemo = ee.FeatureCollection("projects/robust-raster/assets/boundaries/WSDemoSHP_Albers")
        >>> test_parameters = {
        >>>     'collection': 'LANDSAT/LC08/C02/T1_L2',
        >>>     'bands': ['SR_B4', 'SR_B5'],
        >>>     'start_date': '2020-05-01',
        >>>     'end_date': '2020-08-31',
        >>>     'geometry': WSDemo.geometry(),
        >>>     'crs': 'EPSG:3310',
        >>>     'scale': 30,
        >>>     'map_function': prep_sr_l8
        >>> }

        5. Create the EarthEngineDataset object:
        >>> earth_engine = dataset_manager.EarthEngineDataset(parameters=test_parameters)

        6. Print the contents of the data:
        >>> print(earth_engine.dataset)

        Parameters:
        - parameters (dict): A dictionary containing user parameters to query Earth Engine.
        """

        self._xarray_data = self._read_data(image_collection, dataset_config)
        self._max_chunks_limit = self._auto_compute_max_chunks()
    
    @property
    def dataset(self) -> xr.Dataset:
        """
        A property meant to retrieve the xarray Dataset stored in _xarray_data.

        Example:
        >>> earth_engine = dataset_manager.EarthEngineDataset(parameters)
        >>> dataset = earth_engine.dataset
        >>> print(dataset)
        """
        return self._xarray_data
    
    @property
    def dataframe(self) -> pd.DataFrame:
        # Convert Xarray to a Pandas DataFrame (Defaults to long format)
        df = self._xarray_data.to_dataframe().head(5).reset_index()
        return df
    
    @property
    def get_max_chunks_limit(self) -> dict:
        """
        A property not intended for user use. This is called if the user wants to use
        the tuning functionality of this package. However, the user does not need to understand
        how to use this function (unless they choose to set a chunk size themselves).
        """
        return self._max_chunks_limit

    def _get_data_type_in_bytes(self):
        """
        A private method not intended for user use. Using an xarray Dataset object derived from Google Earth Engine, 
        obtain the data type of a single data variable. Because an ee.Image object must have the same data type 
        for all bands when exporting, it does not matter which data variable we extract the data type from 
        (I arbitrarily choose the first data variable for no particular reason).
        """

        first_data_var = list(self._xarray_data.data_vars)[0]
        return self._xarray_data[first_data_var].dtype.itemsize
 
    def _auto_compute_max_chunks(self, request_byte_limit=2**20 * 48) -> dict:
        """
        A private method not intended for user use. Computes the appropriate chunk sizes for all three 
        dimension given Earth Engine's request payload size limit. Ensures the chunk size gets as close 
        to Earth Engine's request payload size without exceeding it.
        
        Parameters:
        request_byte_limit (float): The target chunk size in megabytes. Defaults to 50.331648 MB (the max
                                    you can pull from Earth Engine in a single request).
        
        Returns:
        dict: A dictionary containing the sizes for the first, second, and third dimensions.
        """

        # Get the name of the first dimension
        first_dim_name = list(self._xarray_data.dims)[0]

        # Get the size of the first dimension
        index = self._xarray_data.sizes[first_dim_name]

        # Given the data type size, a fixed index size, and request limit, calculate optimal chunks.
        dtype_bytes = self._get_data_type_in_bytes()
         
        # Calculate the byte size used by the given index
        index_byte_size = index * dtype_bytes
        
        # Check if the index size alone exceeds the request_byte_limit
        if index_byte_size >= request_byte_limit:
            raise ValueError("The given index size exceeds or nearly exhausts the request byte limit.")

        # Calculate the remaining bytes available for width and height dimensions
        remaining_bytes = request_byte_limit - index_byte_size
        
        # Logarithmic splitting of remaining bytes into width and height, adjusted for dtype size
        log_remaining = np.log2(remaining_bytes / dtype_bytes)  # Directly account for dtype_bytes

        # Divide log_remaining between width and height
        d = log_remaining / 2
        wd, ht = np.ceil(d), np.floor(d)

        # Convert width and height from log space to actual values
        width = int(2 ** wd)
        height = int(2 ** ht)

        # Recheck if the final size exceeds the request_byte_limit and adjust
        total_bytes = index * width * height * dtype_bytes
        while total_bytes > request_byte_limit:
            # If the total size exceeds, scale down width and height by reducing one of them
            if width > height:
                width //= 2
            else:
                height //= 2
            total_bytes = index * width * height * dtype_bytes

        actual_bytes = index * width * height * dtype_bytes
        if actual_bytes > request_byte_limit:
            raise ValueError(
                f'`chunks="auto"` failed! Actual bytes {actual_bytes!r} exceeds limit'
                f' {request_byte_limit!r}.  Please choose another value for `chunks` (and file a'
                ' bug).'
            )
    
        return {f'{first_dim_name}': index, 'X': width, 'Y': height}

    def _vector_to_geometry(self, vector: str | ee.FeatureCollection):
        if isinstance(vector, ee.featurecollection.FeatureCollection):
            return vector.geometry()
        else:
            # Read local vector file (supports .shp, .geojson, .kml, etc.)
            gdf = gpd.read_file(vector)  # or .geojson, etc.
            # Convert to GeoJSON dict (not string)
            geojson_dict = json.loads(gdf.to_json())
            # Create an ee.FeatureCollection
            fc = ee.FeatureCollection(geojson_dict)
            return fc.geometry()

    def _construct_ee_collection(self, parameters: dict) -> ee.ImageCollection:
        """
        A private method not intended for user use. Construct an Earth Engine image collection 
        query based on user parameters.

        Parameters:
        - parameters (dict): A dictionary containing parameters for the Earth Engine data.

        Returns:
        - ee.ImageCollection: Earth Engine image collection object.
        """
        # Extract parameters with defaults
        collection = parameters.get('collection', None)
        bands = parameters.get('bands', None)
        start_date = parameters.get('start_date', None)
        end_date = parameters.get('end_date', None)
        #vector_path = parameters.get('vector_path', None)
        map_function = parameters.get('map_function', None)

        if collection is None:
            raise ee.EEException("Earth Engine collection was not provided.")
        
        try:
            ee_collection = ee.ImageCollection(collection)

            # Optional filters
            if start_date and end_date:
                ee_collection = ee_collection.filterDate(start_date, end_date)
            #if vector_path:
            #    fc = self._vector_to_fc(vector_path)
            #    ee_collection = ee_collection.filterBounds(fc.geometry())
            if map_function and callable(map_function):
                ee_collection = ee_collection.map(map_function)
            if bands:
                ee_collection = ee_collection.select(bands)
            
            return ee_collection
        except ee.EEException:
            raise ee.EEException(f"Unrecognized argument type {type(collection)} to convert to an ImageCollection.")

    def _read_data(self, image_collection, dataset_config) -> xr.Dataset:
        """
        A private method not intended for user use. Read Earth Engine data and 
        convert it to xarray format.

        Parameters:
        - parameters (dict): A dictionary containing parameters for the Earth Engine data to be pulled.

        Returns:
        - xarray.Dataset: The dataset containing the Earth Engine data.
        """

        # Take in an ee.ImageCollection
        # Sort by "system:time_start"
        sorted_ic = image_collection.sort("system:time_start")

        # Obtain all of the user's optional dataset parameters
        # Use xr.open_dataset

        if dataset_config['geometry'] and isinstance(dataset_config['geometry'], ee.geometry.Geometry):
            sorted_ic = sorted_ic.filterBounds(dataset_config['geometry'])
            clipped_ic = clip_ic(sorted_ic, dataset_config['geometry'])
        elif dataset_config['geometry'] and not isinstance(dataset_config['geometry'], ee.geometry.Geometry):
            dataset_config['geometry'] = self._vector_to_geometry(dataset_config['geometry'])
            sorted_ic = sorted_ic.filterBounds(dataset_config['geometry'])
            clipped_ic = clip_ic(sorted_ic, dataset_config['geometry'])
        xarray_data = xr.open_dataset(
            clipped_ic,
            engine='ee',
            #executor_kwargs = {"max_workers": 1},
            **dataset_config
        )

        if is_degenerate(xarray_data):
            raise DegenerateTileError(f"Degenerate tile dims={dict(xarray_data.sizes)}")
            # mark tile as skipped and return early from your tile loop
        return xarray_data
    
def clip_ic(ic, geom):
    """Clips every image in an ImageCollection to a geometry."""
    #geom = ee.Geometry(geom)

    def _clip(img):
        return ee.Image(img).clip(geom)

    return ee.ImageCollection(ic).map(_clip)

# If a tile only has one pixel.
def is_degenerate(ds, x_dim=("x", "X"), y_dim=("y", "Y")):
    xd = next((d for d in x_dim if d in ds.dims), None)
    yd = next((d for d in y_dim if d in ds.dims), None)
    if xd is None or yd is None:
        return True  # conservative
    return (ds.sizes[xd] < 2) or (ds.sizes[yd] < 2)

class DegenerateTileError(Exception):
    pass