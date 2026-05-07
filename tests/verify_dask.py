import xarray as xr
import dask.array as da
import numpy as np
from dask.distributed import Client, performance_report
import pandas as pd
import time

def my_func(ds):
    time.sleep(1)
    return ds

def run():
    client = Client()
    # numpy backed dataset
    ds = xr.Dataset({"A": (("x", "y"), np.random.rand(10, 10))})
    
    # dask backed template
    template = xr.Dataset({"A": (("x", "y"), da.empty((10, 10), chunks=(10, 10)))})
    
    test = xr.map_blocks(my_func, ds, template=template)
    
    with performance_report(filename="test_report.html"):
        test.compute()
        
    # read test_report.html
    with open("test_report.html", "r") as f:
        print("compute time" in f.read())

if __name__ == "__main__":
    run()
