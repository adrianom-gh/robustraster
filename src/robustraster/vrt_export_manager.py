import glob
from osgeo import gdal
import os
from .dataset_manager import RasterDataset, EarthEngineDataset
from .format_time import create_time_tag

def export_vrt(data_source: RasterDataset | EarthEngineDataset, out_root):
        first_dim = list(data_source.dataset.dims)[0]
        for i, time_val in enumerate(data_source.dataset[first_dim].values):
            find_tif_files(first_dim, time_val, out_root)
    
def find_tif_files(first_dim, time_val, out_root):
    """Generate a VRT from all GeoTIFF files in a given directory."""
    time_tag = create_time_tag(time_val)

    vrt_basename = f"{first_dim}_{time_tag}"
    output_vrt_path = os.path.join(out_root, f"{vrt_basename}.vrt")
    tif_files = glob.glob(os.path.join(out_root, f"*{time_tag}*.tif"))
    generate_vrt(tif_files, output_vrt_path)
    
def generate_vrt(input_files: list, output_vrt_path: str):
    """Generate a VRT file from a list of GeoTIFF files."""
    if not input_files:
        print("No GeoTIFF files found to create VRT.")
        return
        
    vrt_dataset = gdal.BuildVRT(output_vrt_path, input_files)

    if vrt_dataset:
        try:
            first_dataset = gdal.Open(input_files[0])
            if first_dataset:
                for i in range(1, first_dataset.RasterCount + 1):
                    desc = first_dataset.GetRasterBand(i).GetDescription()
                    if desc:
                        vrt_dataset.GetRasterBand(i).SetDescription(desc)
                first_dataset = None
        except Exception as e:
            print(f"Could not copy band descriptions: {e}")
            
        vrt_dataset.FlushCache()  # Save changes
        vrt_dataset = None  # Close dataset
        print(f"VRT file created successfully: {output_vrt_path}")
    else:
        print("Failed to create VRT file.")