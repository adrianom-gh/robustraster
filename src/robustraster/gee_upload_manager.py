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

    if not gee_asset_path:
        raise ValueError("Missing 'gee_asset_path' in export_config required for GEE upload.")
    
    storage_client = storage.Client.from_service_account_json(gcs_credentials)
    bucket = storage_client.get_bucket(gcs_bucket)
    
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
    
    for time_tag, uris in groups.items():
        # Construct the destination asset ID
        if time_tag != "default":
            # Extract year from time_tag to make the asset name just the year
            year = time_tag[:4] if len(time_tag) >= 4 and time_tag[:4].isdigit() else time_tag
            asset_id = posixpath.join(gee_asset_path, year)
        else:
            asset_id = gee_asset_path
        
        # Fix asset ID to include 'projects/...' if it's missing but users usually supply full path
        
        # Build manifest
        sources = []
        # Chunk URIs to batches of 9000 to remain safely under the 10,000 threshold per source
        batch_size = 9000
        for i in range(0, len(uris), batch_size):
            chunk = uris[i:i + batch_size]
            sources.append({"uris": chunk})

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
