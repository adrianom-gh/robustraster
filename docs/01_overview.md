# robustraster

**robustraster** is a Python package that allows users to run their own custom functions on large geospatial datasets without having to write their own code for parallelization. The idea here is to make large-scale geospatial data analysis more accessible, lowering the barrier of entry for users not familiar with advanced computing techniques.

---

## Key Features

- Supports Google Earth Engine and (eventually) local rasters
- Auto-tunes your function to the resources available on your machine (if necessary)
- Simple one-line API via `run()`
- Export to GeoTIFF or to Google Cloud Storage

## Why robustraster? Who Is It For?

In recent years, the amount of data collected from satellites has grown dramatically. This data can help us understand our planet better, but its sheer size makes it difficult to analyze. Traditional methods struggle to keep up, leading to a need for advanced technological solutions. Google Earth Engine (GEE) is one formidable solution as it provides access to vast amounts of satellite data and various analysis tools without the need for a computer powerful enough to store and process all that data. However, GEE has limitations in the types of analyses it can perform on data. Other data-intensive computing solutions, such as xarray–an extension of Python’s NumPy arrays–and Dask, a Python library for parallel and distributed computing, have expanded the types of analyses performed on large datasets. Yet, these tools can be complex and difficult to use, especially for those unfamiliar with parallel computing or multidimensional data structures. To address these challenges and limitations, I developed **robustraster**, a Python software package that allows users to run custom analyses on large data using their own computers. Designed with accessibility in mind, **robustraster** removes the need to understand parallelization or array-based data models. It aims to empower data scientists, particularly those without advancing computing backgrounds, to work with large-scale data more effectively.