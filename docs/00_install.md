# Installation & Authentication

Welcome to `robustraster`! This package is distributed via Anaconda. 

## Requirements

`robustraster` automatically installs its core dependencies, including GDAL. However, there is one optional system requirement you may need depending on your workflow:

### Docker (Optional but Recommended)
While the `docker-py` Python library is automatically installed alongside `robustraster`, you must have the actual **Docker Engine / Docker Desktop** installed natively on your machine's OS if you intend to use the Docker-based Dask worker functionalities (e.g., executing custom R code, providing a `docker_image`).

- **Windows / Mac**: Download and install [Docker Desktop](https://www.docker.com/products/docker-desktop/).
- **Linux**: Install Docker Engine via your system's package manager.

Ensure the Docker daemon is running in the background before executing any localized workflows that rely on it.

## Installing `robustraster`

You can install `robustraster` directly from the `adrianom` channel:

```bash
conda install adrianom::robustraster
```

### Verifying the Installation

To verify that `robustraster` was installed correctly, you can run Python in your terminal and import the package:

```python
import robustraster
```

## Google Cloud & Earth Engine Authentication

`robustraster` tightly integrates with Google Earth Engine and Google Cloud Storage (GCS). To use these features seamlessly, you must authenticate your machine.

### 1. Application Default Credentials (ADC) for Google Cloud Storage
When exporting tiles to a GCS bucket, `robustraster` leverages Google Cloud's Application Default Credentials. You need to authenticate your machine using the Google Cloud CLI.

If you don't have the Google Cloud SDK installed, [install it first](https://cloud.google.com/sdk/docs/install). Then, run the following command in your terminal:

```bash
gcloud auth application-default login
```
This will open a browser window and authenticate your local environment so `robustraster` can write to your GCS buckets without needing explicit service account keys in your code.

### 2. Enabling Earth Engine for your Cloud Project
In order to upload assets to Google Earth Engine or use Earth Engine computations, the Google Cloud Project you are using must have the **Earth Engine API** enabled.

1. Go to the [Google Cloud Console](https://console.cloud.google.com/).
2. Select your project.
3. Search for "Earth Engine API" and click **Enable**.
4. Make sure your account is registered for Earth Engine access if you haven't done so previously.

### 3. Authenticating with Earth Engine (`ee.Authenticate`)
Finally, in your Python script or Jupyter Notebook, you must authenticate the Earth Engine library before running `robustraster` with Earth Engine inputs/outputs.

You generally only need to run this once per machine/environment:
```python
import ee
ee.Authenticate()
```
This will provide a link for you to sign in and generate an authorization token. 

After authenticating, you must initialize the API at the top of your scripts, making sure to specify your Google Cloud project (especially important for the new API versions):
```python
ee.Initialize(project='your-google-cloud-project-id')
```
