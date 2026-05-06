import re
import csv
import os
#import docker
from functools import reduce
import operator
import psutil
import math

def create_metrics_report():
    # Open the CSV file in write mode to overwrite any previous failed run's files
    with open('metrics_report.csv', 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)

        # Write the header
        writer.writerow(["Chunk Size", "C", "TC(s)", "RC(GiB)", "wMax", "wRAM", "Tpixel(s/pixel)", "Tparallel(pixel/worker)"])

def convert_to_seconds(time_str):
    # Dictionary to store conversion factors
    conversion_factors = {
        'us': 1e-6,  # microseconds to seconds
        'ms': 1e-3,  # milliseconds to seconds
        's': 1,      # seconds to seconds
        'min': 60,   # minutes to seconds
        'h': 3600,   # hours to seconds
    }
    
    # Split the string into value and unit
    time_str = time_str.strip()
    value_str = ''.join([c for c in time_str if c.isdigit() or c == '.'])
    unit = ''.join([c for c in time_str if c.isalpha()])
    
    # Convert the value to float
    value = float(value_str)
    
    # Check if the unit is valid and perform conversion
    if unit in conversion_factors:
        return value * conversion_factors[unit]
    else:
        raise ValueError(f"Unrecognized time unit: {unit}")
    
def convert_to_gigabytes(ram_str):
    # Dictionary to store conversion factors to bytes
    conversion_factors = {
        'B': 1,                 # Bytes to bytes
        'KiB': 1024,            # Kibibytes to bytes
        'MiB': 1024**2,         # Mebibytes to bytes
        'GiB': 1024**3,         # Gibibytes to bytes
        'TiB': 1024**4,         # Tebibytes to bytes
        'KB': 1000,             # Kilobytes (decimal) to bytes
        'MB': 1000**2,          # Megabytes (decimal) to bytes
        'GB': 1000**3,          # Gigabytes (decimal) to bytes
        'TB': 1000**4           # Terabytes (decimal) to bytes
    }
    
    # Split the string into value and unit
    ram_str = ram_str.strip()
    value_str = ''.join([c for c in ram_str if c.isdigit() or c == '.'])
    unit = ''.join([c for c in ram_str if c.isalpha() or c in ['iB', 'B']])

    # Convert the value to float
    value = float(value_str)

    # Check if the unit is valid and perform conversion to bytes
    if unit in conversion_factors:
        value_in_bytes = value * conversion_factors[unit]
        # Convert bytes to gigabytes
        return value_in_bytes / (1024**3)  # Divide by 1024^3 to get GiB
    else:
        raise ValueError(f"Unrecognized RAM unit: {unit}")  
      
def get_wall_time_and_memory(fallback_compute_time_seconds=None):
    # Open the HTML file
    with open('dask_report.html', 'r', encoding='utf-8') as file:
        # Read the content of the file
        content = file.read()

    # Step 1: Create a regex pattern to search for "compute time", capturing the number and unit
    # We include the space between the number and unit, and assume the unit is followed by another space or non-alphabet character
    compute_time_pattern = r'compute\s*time:\s*(\d+\.\d+|\d+)\s+([a-zA-Z]+)\s'

    # Step 2: Use re.search to find the first occurrence of "compute time"
    match = re.search(compute_time_pattern, content)

    # Step 3: If a match is found, extract the value and the unit
    if not match:
        if fallback_compute_time_seconds is not None:
            compute_time_seconds = fallback_compute_time_seconds
        else:
            raise ValueError("Compute time not found in the Dask report. IDK why it sometimes appears.")
    else:
        compute_time_value = match.group(1)  # The numeric value (e.g., "13.30")
        compute_time_unit = match.group(2)  # The unit (e.g., "s")
        compute_time_string = compute_time_value + " " + compute_time_unit
        compute_time_seconds = convert_to_seconds(compute_time_string)

    # Let's revise the regex pattern to capture the data more flexibly
    memory_pattern_final = r'"memory",\["min: [^"]+",\s*"max: ([0-9.]+) ([a-zA-Z]+)",\s*"mean: [^"]+"\]'

    # Search again for the memory max value in the html snippet
    match_final = re.search(memory_pattern_final, content)

    if match_final:
        max_memory_value = match_final.group(1)
        max_memory_unit = match_final.group(2)
        max_memory_string = max_memory_value + " " + max_memory_unit
        max_memory_gb = convert_to_gigabytes(max_memory_string)
    else:
        # Provide a small fallback memory if not found in the report
        max_memory_gb = 0.01

    return compute_time_seconds, max_memory_gb

"""
def get_dask_workers_count():
    client = docker.from_env()  # Initialize Docker client
    containers = client.containers.list()  # Get list of running containers
    dask_worker_containers = [container for container in containers if 'dask-worker' in container.name]
    return len(dask_worker_containers)"
"""

def get_available_system_memory():
    # Get total available RAM in bytes
    total_ram = psutil.virtual_memory().total

    # Convert from bytes to gigabytes (GB)
    total_ram_gb = total_ram / (1024 ** 3)

    return total_ram_gb

def get_compute_time_per_pixel(ds, compute_time_seconds, max_memory_gb):
    # Assuming xarray_obj is your chunked xarray dataset
    derived_chunk_size = {dim: chunks[0] for dim, chunks in ds.chunks.items()} if getattr(ds, "chunks", None) else {}
    pixels_per_chunk = reduce(operator.mul, derived_chunk_size.values(), 1)

    max_workers = os.cpu_count()
    
    # Get the max workers (RAM limited)
    available_system_memory = get_available_system_memory()
    ram_safety_threshold = 0.5
    max_workers_ram_limited = min(math.floor(available_system_memory * ram_safety_threshold / max_memory_gb), max_workers)

    pixel_wall_time = compute_time_seconds / pixels_per_chunk
    parallel_pixel_wall_time = pixel_wall_time / max_workers_ram_limited


    # Prepare the data for the new row, combining value and unit in one cell
    row = [f"{derived_chunk_size}", f"{pixels_per_chunk}", f"{compute_time_seconds}", f"{max_memory_gb}",
           f"{max_workers}", f"{max_workers_ram_limited}", f"{pixel_wall_time}", f"{parallel_pixel_wall_time}"]

    return row

def write_performance_metrics_to_file(ds, fallback_compute_time_seconds=None):
    compute_time_seconds, max_memory_gb = get_wall_time_and_memory(fallback_compute_time_seconds)

    row = get_compute_time_per_pixel(ds, compute_time_seconds, max_memory_gb)

    # Open the CSV file in append mode and write the header and new row
    with open('metrics_report.csv', 'a', newline='') as csvfile:
        writer = csv.writer(csvfile)
        
        # Write the row of results
        writer.writerow(row)
        
    os.remove('dask_report.html')

def clean_up_files():
    if os.path.exists("metrics_report.csv"):
        os.remove('metrics_report.csv')
    if os.path.exists('dask_report.html'):
        os.remove('dask_report.html')