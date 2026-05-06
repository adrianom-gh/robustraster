# Use Miniconda3 as the base image for Anaconda/Conda support
FROM continuumio/miniconda3

# Set working directory
WORKDIR /app

# Install system dependencies if required
RUN apt-get update && apt-get install -y build-essential git && rm -rf /var/lib/apt/lists/*

# Install Conda dependencies with specific versions from meta.yaml
# We use Python 3.10 to be compatible with typical geo libraries and setup.py requirements
RUN conda install -n base -c conda-forge -y \
    python=3.10 \
    gdal=3.10.2 \
    geopandas=1.0.1 \
    pandas=2.2.2 \
    pyarrow=21.0.0 \
    fastparquet=2024.11.0 \
    numpy=2.0.1 \
    rpy2=3.6.4 \
    # Ensure pip and setuptools are present
    pip \
    setuptools \
    && conda clean -afy

# Copy requirements file (for pip dependencies)
COPY requirements.txt .

# Install pip dependencies
# We use --no-deps for packages that might conflict with conda versions if possible, 
# but usually pip is smart enough. Using --no-cache-dir to keep image small.
RUN pip install --no-cache-dir -r requirements.txt

# Copy project files
COPY . .

# Install local package
RUN pip install .

# Set entrypoint
CMD ["python"]
