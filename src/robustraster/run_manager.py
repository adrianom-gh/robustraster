from .dataset_manager import RasterDataset, EarthEngineDataset, DegenerateTileError
from .dask_cluster_manager import DaskClusterManager
from .dask_docker_cluster_manager import DDClusterManager  # NEW: Docker-based cluster manager
from .raster_export_manager import RasterExportProcessor
from .vector_export_manager import VectorExportProcessor
from .udf_manager import UserFunctionHandler
from .dask_plugins import EEPlugin, patch_ee_methods
from .hooks import preview_dataset_hook
from .ee_grid_tiles import ee_covering_grid_tiles, clip_tiles_to_aoi
from .ee_grid_tiles import iter_tiles_from_fc
from .vrt_export_manager import export_vrt
from distributed import Semaphore
import ee
from typing import Any, Callable, Optional, Union
from dask import config
from pathlib import Path
import json
import time
import os

def DatasetAdapterFactory(source, dataset, dataset_config=None, *, chunks=None):
    """
    Smart dispatcher:
    - if source == "ee" → EarthEngineDataset
    - if source is a file path (string or list) → RasterDataset
    """
    if source == "ee":
        return EarthEngineDataset(image_collection=dataset, dataset_config=dataset_config)
    elif source == "local":
        if chunks is not None:
            dataset_config = dict(dataset_config)
            dataset_config.setdefault("chunks", chunks)
        return RasterDataset(file_path=dataset, dataset_config=dataset_config)
    else:
        raise ValueError("Source must be 'ee' or a file path (str or list of str).")

def report_dir(out_dir):
    d = Path(out_dir) / "reports"
    d.mkdir(parents=True, exist_ok=True)
    return d

def write_success(tile_id, out_dir):
    marker = report_dir(out_dir) / f"tile_{tile_id}.success.json"
    marker.write_text(json.dumps({
        "tile_id": tile_id,
        "status": "success",
        "timestamp": time.time()
    }))
    return marker

def write_failure(tile_id, out_dir, exc):
    marker = report_dir(out_dir) / f"tile_{tile_id}.failure.json"
    marker.write_text(json.dumps({
        "tile_id": tile_id,
        "status": "failure",
        "error": repr(exc),
        "timestamp": time.time()
    }))
    return marker

def run(
    dataset: str | list[str] | ee.imagecollection.ImageCollection,
    preview_dataset: bool = False,
    max_pixels_per_tile: int = 1_000_000,
    dataset_config: dict[str, Any] = None,
    user_function_config: dict[str, Any] = None,
    function_tuning_config: Optional[dict[str, Any]] = None,
    export_config: dict[str, Any] = None,
    dask_mode: str = "full",
    dask_config: dict[str, Any] = None,
    hooks: Optional[dict[str, Callable[..., Any]]] = None,
    # --- NEW ---

    docker_image: Optional[str] = None,
    dask_docker_kwargs: Optional[dict[str, Any]] = None,
):
    """
    Main interface to run a user-defined function across geospatial raster data.
    """
    if isinstance(dataset, ee.imagecollection.ImageCollection):
        source = "ee"
    elif isinstance(dataset, (str, list)):
        source = "local"
    else:
        raise TypeError("dataset must be an ee.ImageCollection, a string path, or a list of string paths.")

    dataset_config = dataset_config or {}
    function_tuning_config = function_tuning_config or {}
    tune_function = function_tuning_config.pop("tune_function", False)
    export_config = export_config or {}
    dask_config = dask_config or {}
    dask_docker_kwargs = dask_docker_kwargs or {}
    hooks = hooks or {}
    user_function_config = user_function_config or {}

    # user_function_config parameters extracted
    user_function = user_function_config.get("user_function")
    user_function_args = user_function_config.get("user_function_args", ())
    user_function_kwargs = user_function_config.get("user_function_kwargs", {})
    is_r_function = user_function_config.get("is_r_function", False)
    r_function_code = user_function_config.get("r_function_code", "")
    r_function_name = user_function_config.get("r_function_name", "")

    if is_r_function:
        if not docker_image:
            raise ValueError("Running R code requires a docker_image to be provided")
        if not r_function_code or not r_function_name:
            raise ValueError("r_function_code and r_function_name must be provided in user_function_config if is_r_function is True")

        def r_wrapper(df, *args, **kwargs):
            import_failed = False
            import_err = None
            import warnings
            import logging
            
            # Suppress rpy2 logs and warnings before importing on host machine
            rpy2_logger = logging.getLogger('rpy2')
            old_rpy2_level = rpy2_logger.level
            rpy2_logger.setLevel(logging.CRITICAL)
            
            root_logger = logging.getLogger()
            old_root_level = root_logger.level
            root_logger.setLevel(logging.CRITICAL)

            try:
                with warnings.catch_warnings():
                    warnings.simplefilter('ignore')
                    import rpy2.robjects as ro
                    from rpy2.robjects import pandas2ri
                    from rpy2.robjects.conversion import localconverter
                    from rpy2.robjects import default_converter
            except Exception as e:
                import_failed = True
                import_err = e
            finally:
                rpy2_logger.setLevel(old_rpy2_level)
                root_logger.setLevel(old_root_level)

            if not import_failed:
                import pandas as pd
                # Execute the dataframe conversion and native R operations using the modern context manager
                with localconverter(default_converter + pandas2ri.converter):
                    # Load the code into the R environment
                    ro.r(r_function_code)
                    r_func = ro.globalenv[r_function_name]

                    # Execute it with the pandas dataframe passed to R
                    r_df = ro.conversion.py2rpy(df)
                    result_r = r_func(r_df)
                    result_py = ro.conversion.rpy2py(result_r)
                
                # R POSIXct types often force a UTC timezone when coming back to Python.
                # robustraster relies on naive datetimes to map back to xarray coordinates.
                if 'time' in result_py.columns and pd.api.types.is_datetime64_any_dtype(result_py['time']):
                    if result_py['time'].dt.tz is not None:
                        result_py['time'] = result_py['time'].dt.tz_localize(None)

                return result_py
            else:
                ie = import_err
                # Handle rpy2 not installed natively (like docker run without manual R packages)
                import subprocess
                import tempfile
                import os
                import pandas as pd
                
                with tempfile.TemporaryDirectory() as temp_dir:
                    in_csv = os.path.join(temp_dir, "in.csv")
                    out_csv = os.path.join(temp_dir, "out.csv")
                    r_script_path = os.path.join(temp_dir, "script.R")
                    
                    df.to_csv(in_csv, index=False)
                    use_docker_fallback = False
                    import shutil
                    if shutil.which("Rscript") is None and docker_image:
                        use_docker_fallback = True
                        
                    if use_docker_fallback:
                        in_csv_r = "/temp_workspace/in.csv"
                        out_csv_r = "/temp_workspace/out.csv"
                    else:
                        in_csv_r = in_csv.replace('\\\\', '/')
                        out_csv_r = out_csv.replace('\\\\', '/')
                        
                    r_script_content = f"""
{r_function_code}

in_data <- read.csv("{in_csv_r}")
out_data <- {r_function_name}(in_data)
write.csv(out_data, "{out_csv_r}", row.names=FALSE)
"""
                    with open(r_script_path, 'w') as f:
                        f.write(r_script_content)
                    
                    cmd = []
                    if use_docker_fallback:
                        cmd = ["docker", "run", "--rm", "-v", f"{temp_dir}:/temp_workspace", docker_image, "Rscript", "/temp_workspace/script.R"]
                    else:
                        cmd = ["Rscript", r_script_path]
                    
                    # Ensure execution works seamlessly via command line or ephemeral docker
                    try:
                        subprocess.run(cmd, check=True, capture_output=True, text=True)
                    except subprocess.CalledProcessError as e:
                        print(f"Rscript failed with error (stderr):\\n{e.stderr}")
                        print(f"Rscript stdout:\\n{e.stdout}")
                        raise RuntimeError(f"R code execution failed! {e.stderr}")
                    except FileNotFoundError:
                        raise RuntimeError(f"Rscript is not installed or not in PATH! Error: {ie}")
                    
                    if os.path.exists(out_csv):
                        out_df = pd.read_csv(out_csv)
                        return out_df
                    else:
                        raise FileNotFoundError("Rscript did not produce the expected output CSV.")

        user_function = r_wrapper
        user_function_config["user_function"] = r_wrapper

    user_function_config.pop("is_r_function", None)
    user_function_config.pop("r_function_code", None)
    user_function_config.pop("r_function_name", None)

    chunks = function_tuning_config.get("chunks", None)

    cluster_manager = None
    client = None

    try:
         # ========== PREVIEW DATASET ===========
        if source == "ee":
             try:
                 # Ensure we have the backoff wrapper on the client side too!
                 patch_ee_methods()
             except Exception as e:
                 print(f"[robustraster] Warning: could not patch EE methods on client: {e}")

        if preview_dataset:
            adapter = DatasetAdapterFactory(source, dataset, dataset_config, chunks=chunks)
            data_source = adapter

            preview_dataset_hook(data_source.dataset, user_function, *user_function_args, **user_function_kwargs)
            return

        # ========== HOOK: before_run ==========
        if "before_run" in hooks:
            hooks["before_run"]()

        # ========== Dask Setup ==========
        config.set({
            "distributed.worker.memory.target": False,
            "distributed.worker.memory.spill": False,
            "distributed.worker.memory.pause": 0.90,
            "distributed.worker.memory.terminate": 0.98,
        })
        if docker_image:
            
            # --- Auto-mount Earth Engine Credentials ---
            # Try to find credentials on the host
            try:
                # ee.oauth.get_credentials_path() returns the path to the credentials file
                # e.g., ~/.config/earthengine/credentials
                creds_path = ee.oauth.get_credentials_path()
                if os.path.exists(creds_path):
                    print(f"[robustraster] Found EE credentials at: {creds_path}")
                    
                    # Ensure volumes dict exists in dask_config
                    dask_config = dask_config or {}
                    volumes = dask_config.get("volumes", {})
                    
                    # mount to /root/.config/earthengine/credentials
                    # Assuming the container runs as root. If not, this might need adjustment.
                    container_path = "/root/.config/earthengine/credentials"
                    
                    # The volume format for docker-py (and likely dask-docker) is:
                    # {host_path: {'bind': container_path, 'mode': 'ro'}}
                    volumes[creds_path] = {'bind': container_path, 'mode': 'ro'}
                    
                    dask_config["volumes"] = volumes
                    print(f"[robustraster] Mounting EE credentials to {container_path}")
                else:
                    print(f"[robustraster] Warning: EE credentials not found at {creds_path}. Workers may fail to authenticate.")
            except Exception as e:
                print(f"[robustraster] Warning: Failed to detect/mount EE credentials: {e}")

            # --- Auto-mount Output Directory ---
            # 1. Resolve host output path
            output_folder_raw = export_config.get("output_folder", "tiles")
            host_output_path = str(Path(output_folder_raw).resolve())
            
            # Create it on host if it doesn't exist, so Docker can mount it
            os.makedirs(host_output_path, exist_ok=True)
            
            container_output_path = "/robustraster_output"
            
            print(f"[robustraster] Mounting output: {host_output_path} -> {container_output_path}")

            # 2. Add to volumes
            dask_config = dask_config or {}
            volumes = dask_config.get("volumes", {})
            
            # Bind RW
            volumes[host_output_path] = {'bind': container_output_path, 'mode': 'rw'}
            
            # --- Auto-mount robustraster source code ---
            import robustraster
            rr_path = os.path.dirname(robustraster.__file__)
            # Mount the parent of `robustraster` (i.e. `src/`) to `/robustraster_src`
            src_dir = os.path.dirname(rr_path)
            container_src_path = "/robustraster_src"
            volumes[src_dir] = {'bind': container_src_path, 'mode': 'ro'}
            print(f"[robustraster] Mounting active package source: {src_dir} -> {container_src_path}")
            
            # --- Auto-mount Host Current Working Directory ---
            # This ensures relative paths to datasets resolve correctly inside the workers
            host_cwd = os.getcwd()
            container_cwd = "/workspace"
            volumes[host_cwd] = {'bind': container_cwd, 'mode': 'rw'}
            print(f"[robustraster] Mounting host CWD: {host_cwd} -> {container_cwd}")
            dask_config["working_dir"] = container_cwd
            
            dask_config["volumes"] = volumes
            
            # 3. Set environment variable for override
            env_vars = dask_config.get("env_vars", {})
            env_vars["ROBUSTRASTER_OVERRIDE_OUTPUT"] = container_output_path
            
            # Ensure the container uses the mounted source code
            env_vars["PYTHONPATH"] = container_src_path
            
            dask_config["env_vars"] = env_vars

            cluster_manager = DDClusterManager(docker_image=docker_image, **dask_docker_kwargs)
            cluster_manager.create_cluster(mode=dask_mode, **dask_config)
            client = cluster_manager.get_dask_client
            print(f"[robustraster] Docker Dask cluster started: {client}")
        else:
            cluster_manager = DaskClusterManager()
            cluster_manager.create_cluster(mode=dask_mode, **dask_config)
            client = cluster_manager.get_dask_client
            print(f"[robustraster] Dask cluster started: {client}")

        # ========== Earth Engine Dask auth + concurrency settings ============
        ee_semaphore = None
        # Earth Engine Dask auth
        if source == 'ee':
            ee_plugin = EEPlugin()
            client.register_plugin(ee_plugin)
            print("[robustraster] Dask workers authenticated to Earth Engine.")

            # ------------------------------------------------------------------
            # Global semaphore to limit Earth Engine concurrency across workers
            # ------------------------------------------------------------------
            #ee_max_concurrency = dask_config.get("ee_max_concurrency", 3)  # default 3
            #ee_semaphore = Semaphore(name="ee_global", max_leases=int(ee_max_concurrency))

        # ===== Check for export keyword arguments ====
        gcs = export_config.get("export_to_gcs", None)

        if gcs:
            required_gcs_keys = ["gcs_bucket"]
            missing_keys = [k for k in required_gcs_keys if k not in export_config]
            if missing_keys:
                raise ValueError(f"Missing required GCS export configuration: {', '.join(missing_keys)}")

        # ========== User Function ==========
        if callable(user_function) and tune_function:
            # No tiling necessary! As the tuning code will only grab a slice of the data at a time.
            # ONLY WORKS FOR EE I THINK!
            data_source = DatasetAdapterFactory(source, dataset, dataset_config, chunks=chunks)
            handler = UserFunctionHandler(
                **user_function_config,
                **function_tuning_config
            )

            print("[robustraster] Tuning user function...")
            handler.tune_user_function(data_source)
    
        elif callable(user_function) and not tune_function:
            handler = UserFunctionHandler(
                    **user_function_config,
                    **function_tuning_config
                )
        else:
            raise ValueError("No user function was specified or user function is not callable! Please provide a function that accepts and returns a pandas DataFrame.")

        # ========= Export ==========
        if "mode" not in export_config:
            raise ValueError("Missing required export configuration: 'mode'")

        mode = export_config["mode"]

        if mode == "raster":
            processor = RasterExportProcessor(
                user_function_handler=handler,
                ee_semaphone=ee_semaphore,
                **export_config
            )
        elif mode == "vector":
            processor = VectorExportProcessor(
                user_function_handler=handler,
                **export_config
            )
        # ========== Dataset Load + Run ==========
        if source == "ee" and max_pixels_per_tile is not None:
            import warnings
            import shutil
            import math
            import re
            
            class GraphTooLargeError(Exception):
                pass

            out_root = export_config.get("output_folder", "tiles")

            while True:
                # Required tiling inputs
                crs = dataset_config.get("crs")
                scale = dataset_config.get("scale")
                aoi = dataset_config.get("geometry")

                if crs is None or scale is None or aoi is None:
                    raise ValueError(
                        "max_pixels_per_tile requires dataset_config['geometry'], "
                        "dataset_config['crs'], and dataset_config['scale']."
                    )

                # Build server-side tile collection
                tiles_fc = ee_covering_grid_tiles(
                    aoi=aoi,
                    crs=crs,
                    scale=scale,
                    max_pixels_per_tile=int(max_pixels_per_tile),
                )

                clipped_tiles = clip_tiles_to_aoi(tiles_fc, aoi)
                print("[robustraster] AOI tiling enabled. Streaming tiles in batches...")

                total_tiles = tiles_fc.size().getInfo()
                if total_tiles == 0:
                    print("[robustraster] ⚠️ No tiles found covering the AOI. Check your geometry and scale.")

                data_source = None
                try:
                    for tile_i, tile_geom in enumerate(iter_tiles_from_fc(clipped_tiles, batch_size=200), start=1):
                        tile_dataset_config = dict(dataset_config)
                        tile_dataset_config["geometry"] = tile_geom

                        print(f"[robustraster] Processing tile {tile_i} of {total_tiles}")

                        # Resume logic: skip tiles that already succeeded
                        success_marker = report_dir(out_root) / f"tile_{tile_i}.success.json"
                        if success_marker.exists():
                            print(f"[robustraster] ✅ Tile {tile_i} already succeeded; skipping.")
                            continue
                        
                        while True:
                            try:
                                data_source = DatasetAdapterFactory(source, dataset, tile_dataset_config, chunks=chunks)
                            except DegenerateTileError as e:
                                print(f"[robustraster] ⚠️ Skipping tile {tile_i}: {e}")
                                write_failure(tile_i, out_root, e)
                                break
                            
                            try:
                                print("[robustraster] Running user function...")
                                processor._tile_id = tile_i
                                
                                # Use warnings catcher to intercept large graphs
                                with warnings.catch_warnings():
                                    warnings.filterwarnings('error', message='.*Sending large graph.*', category=UserWarning)
                                    try:
                                        processor.run_and_export_results(data_source)
                                    except UserWarning as warn_e:
                                        if "Sending large graph" in str(warn_e):
                                            raise GraphTooLargeError(str(warn_e))
                                        else:
                                            raise
                                
                                write_success(tile_i, out_root)
                            except Exception as e:
                                if isinstance(e, GraphTooLargeError):
                                    raise  # Propagate up to outer loop
                                write_failure(tile_i, out_root, e)
                                raise
                            break # Success

                except GraphTooLargeError as ge:
                    print(f"[robustraster] ⚠️ Caught Dask graph size warning:\n {ge}\n")
                    print("[robustraster] 🔄 Dynamically downscaling max_pixels_per_tile and restarting!")
                    
                    # Parse the graph size from warning message
                    msg = str(ge)
                    size_match = re.search(r"size\s+([0-9.]+)\s+([a-zA-Z]+)", msg)
                    
                    if size_match:
                        size_val = float(size_match.group(1))
                        size_unit = size_match.group(2)
                        
                        # Normalize to MiB
                        if size_unit == "GiB":
                            size_mib = size_val * 1024
                        elif size_unit == "MiB" or size_unit == "MB":
                            size_mib = size_val
                        elif size_unit == "KiB" or size_unit == "KB":
                            size_mib = size_val / 1024
                        elif size_unit == "TiB":
                            size_mib = size_val * (1024 ** 2)
                        else:
                            size_mib = size_val # fallback assumption
                            
                        # Calculate required reduction factor (targeting safe 8 MiB)
                        target_mib = 8.0
                        if size_mib > target_mib:
                            ratio = target_mib / size_mib
                            target_pixels = max_pixels_per_tile * ratio
                            
                            new_side = math.floor(math.sqrt(target_pixels))
                            new_pixels = new_side * new_side
                            
                            # Safety check, make sure it actually drops
                            if new_pixels >= max_pixels_per_tile:
                                new_pixels = int(max_pixels_per_tile * 0.5)
                                new_side = math.floor(math.sqrt(new_pixels))
                                new_pixels = new_side * new_side
                                
                            max_pixels_per_tile = int(new_pixels)
                        else:
                            # It's less than 8 MiB but Dask still complained? Force half.
                            new_pixels = int(max_pixels_per_tile * 0.5)
                            new_side = math.floor(math.sqrt(new_pixels))
                            max_pixels_per_tile = new_side * new_side
                    else:
                        # Fallback static ratio if regex parsing fails
                        new_pixels = int(max_pixels_per_tile * 0.5)
                        new_side = math.floor(math.sqrt(new_pixels))
                        max_pixels_per_tile = new_side * new_side
                        
                    print(f"[robustraster] 📉 New max_pixels_per_tile set to {max_pixels_per_tile} ({new_side}x{new_side})")
                    
                    # Wipe conflict caches
                    if os.path.exists(out_root):
                        shutil.rmtree(out_root)
                        
                    continue # Restart the outer `while True` loop
                
                # If we successfully completed all tiles without GraphTooLargeError:
                break

            # Shutdown first, then close client
            try:
                client.shutdown()
            except Exception:
                pass
            client.close()

            if export_config.get("vrt") and data_source:
                export_vrt(data_source, out_root)

        else:
            out_root = export_config.get("output_folder", "tiles")

            # existing behavior
            data_source = DatasetAdapterFactory(source, dataset, dataset_config, chunks=chunks)

            if "after_dataset_loaded" in hooks:
                hooks["after_dataset_loaded"](data_source.dataset)

            report_dir(out_root)
            print("[robustraster] Running user function...")
            processor.run_and_export_results(data_source)

            # Shutdown first, then close client
            try:
                client.shutdown()
            except Exception:
                pass
            client.close()

            if export_config.get("vrt"):
                export_vrt(data_source, out_root)

        # ========== HOOK: after_run ==========
        if "after_run" in hooks and data_source:
            hooks["after_run"](data_source.dataset)

        # ========== Upload to GEE ==========
        if export_config.get("upload_results_to_gee"):
            from .gee_upload_manager import upload_to_gee_from_gcs
            upload_to_gee_from_gcs(export_config)

    except Exception as e:
        print(f"[robustraster] Error: {type(e).__name__} during run():", str(e))
        raise

    finally:
        if client is not None:
            try:
                client.close()
            except Exception:
                pass
            try:
                client.shutdown()
            except Exception:
                pass
            print("[robustraster] Dask client closed.")
        if cluster_manager is not None:
            if hasattr(cluster_manager, "shutdown"):
                try:
                    cluster_manager.shutdown()
                except Exception:
                    pass
            print("[robustraster] Dask cluster shut down.")