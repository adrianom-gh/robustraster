import pandas as pd
import numpy as np
import datetime as dt

def create_time_tag(time_val):
    if isinstance(time_val, np.datetime64):
        time_val = pd.to_datetime(time_val).to_pydatetime()
    if isinstance(time_val, dt.date) and not isinstance(time_val, dt.datetime):
        time_val = dt.datetime(time_val.year, time_val.month, time_val.day)
    return str(time_val).replace(":", "").replace("-", "").replace(" ", "T").split(".")[0]