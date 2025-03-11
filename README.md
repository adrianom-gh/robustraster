# Robust-Raster

Robust-Raster is a Python software package designed to empower scientists and researchers to analyze large satellite datasets effectively. In recent years, the amount of data collected from satellites has grown dramatically. While this data can provide insights into our planet, its sheer size poses significant challenges for traditional analysis methods. Robust-Raster bridges the gap, offering a user-friendly tool to perform custom analyses on large datasets without requiring advanced computing expertise.

## Purpose
Google Earth Engine (GEE) is a powerful platform for accessing satellite data and analysis tools, but it has limitations in the types of analyses it can perform. Robust-Raster addresses these limitations by enabling users to:
- Design functions not supported by GEE.
- Access GEE data without being constrained by storage or local RAM limitations.
- Use data frames instead of more complex data structures like xarray, simplifying workflows.

Robust-Raster aims to lower the barriers to analyzing large datasets, making advanced analysis accessible to a broader audience.

## Features
- **Custom Analyses:** Allows users to design and run functions that extend beyond GEE's capabilities.
- **Efficient Data Handling:** Enables access to GEE data without being hindered by local hardware constraints.
- **User-Friendly Design:** Supports data frames for analysis, providing a simpler alternative to working with xarray objects.

## Installation
### 1. Conda
```bash
conda create -n robustraster python=3.10.16
conda activate robustraster
conda install adrianom::robustraster
```

### 2. Virtualenv + pyenv
Installing using pip is broken at the moment due to the use of GDAL in my package. GDAL is a compiled C++ library code and makes my package trickier to work on different platforms. At the moment, Anaconda is the only option and is available
only on Windows machines. I will update this as I go. Thanks!

## Usage
A comprehensive example is available in `demo.ipynb`, showcasing how to effectively use Robust-Raster. This notebook includes detailed comments to guide users through the process step by step. A Documentation PDF is available in the /docs folder. Please note that Robust-Raster is still in its early stages, and more documentation and updates will be provided over time!

## Contributing
I welcome contributions to Robust-Raster! If you have suggestions or encounter issues, please submit them via the GitHub Issues page.

## License
*To be determined.*

Note: Robust-Raster uses Python and incorporates several libraries, including xarray, xee (an extension of xarray for accessing GEE data), and Dask. Licensing will take these dependencies into account.

## Contact
For any questions or feedback, please contact us via email: [adrianom@unr.edu](mailto:adrianom@unr.edu).

## Acknowledgments
I would like to acknowledge the following projects for their contributions and inspiration:

- California Air Resources Board. *"Advanced Carbon Modeling Techniques for the Forest Health Quantification Methodology (Phase 2)."* 2024. Greenberg, J.A., E. Hanan and N. Inglis.
- CALFIRE. *"Research for a Cyberinfrastructure-Enabled Carbon and Fuels Mapping Model Prototype (Phase 2)."* 2022. Greenberg, J.A.
- CALFIRE. *"Research for a Cyberinfrastructure-Enabled Carbon and Fuels Mapping Model Prototype (Phase 1)."* 2021. Ramirez, C. and J.A. Greenberg.
- California Air Resources Board. *"Advanced Carbon Modeling Techniques for the Forest Health Quantification Methodology."* 2021. Greenberg, J.A. and E. Hanan.
