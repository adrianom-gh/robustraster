# Installation

Welcome to `robustraster`! This package is distributed via Anaconda. 

## Requirements

Before installing `robustraster`, there are a couple of system requirements you should set up:

### 1. GDAL
You will need to install GDAL separately into your Anaconda environment. This is a crucial dependency for geospatial raster processing. We recommend installing it via the `conda-forge` channel:

```bash
conda install -c conda-forge gdal
```

### 2. Docker (Optional but Recommended)
While the `docker-py` Python library is automatically installed alongside `robustraster`, you must have the actual **Docker Engine / Docker Desktop** installed natively on your machine's OS if you intend to use the Docker-based Dask worker functionalities (e.g., executing custom R code, providing a `docker_image`).

- **Windows / Mac**: Download and install [Docker Desktop](https://www.docker.com/products/docker-desktop/).
- **Linux**: Install Docker Engine via your system's package manager.

Ensure the Docker daemon is running in the background before executing any localized workflows that rely on it.

## Installing `robustraster`

Once GDAL is installed and your environment is ready, you can install `robustraster` directly from the `adrianom` channel:

```bash
conda install adrianom::robustraster
```

### Verifying the Installation

To verify that `robustraster` was installed correctly, you can run Python in your terminal and import the package:

```python
import robustraster
```
