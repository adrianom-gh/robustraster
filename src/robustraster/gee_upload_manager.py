import ee
from google.cloud import storage
import collections
import posixpath

def upload_to_gee_from_gcs(export_config):
    """
    Search GCS for exported TIF files, group them by time tag (e.g. year),
    and submit an Earth Engine manifest upload task for each group.
    """
    gcs_credentials = export_config.get("gcs_credentials")
    gcs_bucket = export_config.get("gcs_bucket")
    gcs_folder = export_config.get("gcs_folder", "")
    gee_asset_path = export_config.get("gee_asset_path")

    gcs_project = export_config.get("gcs_project")

    if not gee_asset_path:
        raise ValueError("Missing 'gee_asset_path' in export_config required for GEE upload.")
    
    if gcs_credentials:
        storage_client = storage.Client.from_service_account_json(gcs_credentials, project=gcs_project)
    else:
        try:
            storage_client = storage.Client(project=gcs_project)
        except EnvironmentError as e:
            raise ValueError("Could not determine Google Cloud project. Please provide 'gcs_project' in export_config or set the GOOGLE_CLOUD_PROJECT environment variable.") from e
        
    bucket = storage_client.bucket(gcs_bucket)
    if not bucket.exists():
        print(f"[robustraster] Bucket '{gcs_bucket}' does not exist. Creating it...")
        bucket = storage_client.create_bucket(gcs_bucket, project=storage_client.project)
    
    # List all TIF files in the folder
    prefix = gcs_folder + "/" if gcs_folder and not gcs_folder.endswith('/') else gcs_folder
    blobs = bucket.list_blobs(prefix=prefix)
    
    tif_uris = []
    for blob in blobs:
        if blob.name.endswith(".tif"):
            tif_uris.append(f"gs://{gcs_bucket}/{blob.name}")
    
    if not tif_uris:
        print("[robustraster] No .tif files found in the specified GCS location. Skipping GEE upload.")
        return
    
    # Group URIs by time_tag. 
    # Filenames generally end in: __<first_dim>_<time_tag>.tif
    # Example: x123_456_y123_456__time_20200101T000000.tif
    groups = collections.defaultdict(list)
    for uri in tif_uris:
        base = uri.split("/")[-1]
        if "__" in base:
            suffix = base.split("__")[-1]  # e.g. time_20200101T000000.tif
            # Remove .tif
            suffix = suffix.replace(".tif", "")
            # Split by first "_" to remove the dimension name
            parts = suffix.split("_", 1)
            time_tag = parts[1] if len(parts) > 1 else suffix
        else:
            time_tag = "default"
            
        groups[time_tag].append(uri)
        
    print(f"[robustraster] Found {len(tif_uris)} files across {len(groups)} time steps. Starting GEE ingestion...")
    
    # If there are multiple time steps or a specific time_tag, gee_asset_path is treated as a parent ImageCollection.
    if len(groups) > 1 or (len(groups) == 1 and list(groups.keys())[0] != "default"):
        parts = gee_asset_path.split('/')
        if gee_asset_path.startswith('projects/') and len(parts) >= 3:
            root_idx = 3
        elif gee_asset_path.startswith('users/') and len(parts) >= 2:
            root_idx = 2
        else:
            root_idx = 1
            
        paths_to_check = []
        current = "/".join(parts[:root_idx])
        for part in parts[root_idx:]:
            current = f"{current}/{part}"
            paths_to_check.append(current)
            
        for i, path in enumerate(paths_to_check):
            try:
                ee.data.getAsset(path)
            except Exception:
                atype = 'IMAGE_COLLECTION' if i == len(paths_to_check) - 1 else 'FOLDER'
                print(f"[robustraster] Asset '{path}' may not exist. Attempting to create it as {atype}...")
                try:
                    ee.data.createAsset({'type': atype}, path)
                    print(f"[robustraster] ✅ Successfully created {atype}: {path}")
                except Exception as create_e:
                    if i == len(paths_to_check) - 1:
                        print(f"[robustraster] ❌ Could not create {atype}: {create_e}")
                        raise RuntimeError(f"Failed to create {atype} at {path}. Earth Engine error: {create_e}") from create_e
                    else:
                        print(f"[robustraster] ⚠️ Warning: Could not create FOLDER {path}: {create_e}")
    
    for time_tag, uris in groups.items():
        # Construct the destination asset ID
        if time_tag != "default":
            # Extract date from time_tag to make a distinct asset name (e.g. YYYY_MM_DD)
            clean_tag = time_tag
            if "T" in clean_tag:
                date_part = clean_tag.split("T")[0]
                if len(date_part) == 8 and date_part.isdigit(): # YYYYMMDD
                    clean_tag = f"{date_part[:4]}_{date_part[4:6]}_{date_part[6:]}"
            asset_id = posixpath.join(gee_asset_path, clean_tag)
        else:
            asset_id = gee_asset_path
        
        # Fix asset ID to include 'projects/...' if it's missing but users usually supply full path
        
        # Build manifest
        # Earth Engine requires each spatial tile to be in its own separate ImageSource 
        # in order to mosaic them together into a single Image asset.
        sources = []
        for uri in uris:
            sources.append({"uris": [uri]})

        manifest = {
            "name": asset_id,
            "tilesets": [
                {
                    "id": "tileset_1",
                    "sources": sources
                }
            ]
        }
        
        try:
            task_id = ee.data.newTaskId()[0]
            ee.data.startIngestion(task_id, manifest)
            print(f"[robustraster] ✅ Earth Engine task {task_id} launched for asset {asset_id}. Tiles: {len(uris)}.")
        except Exception as e:
            print(f"[robustraster] ❌ Failed to launch Earth Engine task for {asset_id}: {e}")
