# robustraster

robustraster is a Python software package designed to empower scientists and researchers to analyze large satellite datasets effectively. In recent years, the amount of data collected from satellites has grown dramatically. While this data can provide insights into our planet, its sheer size poses significant challenges for traditional analysis methods. robustraster bridges the gap, offering a user-friendly tool to perform custom analyses on large datasets without requiring advanced computing expertise.

## Purpose
Google Earth Engine (GEE) is a powerful platform for accessing satellite data and analysis tools, but it has limitations in the types of analyses it can perform. robustraster addresses these limitations by enabling users to:
- Design functions not supported by GEE.
- Access GEE data without being constrained by storage or local RAM limitations.
- Use data frames instead of more complex data structures like xarray, simplifying workflows.

robustraster aims to lower the barriers to analyzing large datasets, making advanced analysis accessible to a broader audience.

## Features
- **Custom Analyses:** Allows users to design and run functions that extend beyond GEE's capabilities.
- **Efficient Data Handling:** Enables access to GEE data without being hindered by local hardware constraints.
- **User-Friendly Design:** Supports data frames for analysis, providing a simpler alternative to working with xarray objects.

## Documentation

Extensive documentation is available in the [`/docs`](docs/) folder:

- [Installation](docs/00_install.md)
- [Overview](docs/01_overview.md)
- [Quickstart](docs/02_quickstart.md)
- [Brief Intro to Dask](docs/03_what_is_dask.md)
- [API Reference](docs/04_api_reference.md)
- [Function Tuning](docs/05_tuning.md)

## Installation
```bash
conda create -n robustraster 
conda activate robustraster
conda install adrianom::robustraster
```

## Usage
Comprehensive examples are available in /demos, showcasing how to effectively use robustraster. This notebook includes detailed comments to guide users through the process step by step. Additional examples can be found in the /docs folder.

## Contributing
I welcome contributions to robustraster! If you have suggestions or encounter issues, please submit them via the GitHub Issues page.

## License
*To be determined.*

Note: robustraster uses Python and incorporates several libraries, including xarray, xee (an extension of xarray for accessing GEE data), and Dask. Licensing will take these dependencies into account.

## Contact
For any questions or feedback, please contact us via email: [adrianom@unr.edu](mailto:adrianom@unr.edu).

## Acknowledgments
I would like to acknowledge the following projects for their contributions and inspiration:

- California Air Resources Board. *"Advanced Carbon Modeling Techniques for the Forest Health Quantification Methodology (Phase 2)."* 2024. Greenberg, J.A., E. Hanan and N. Inglis.
- CALFIRE. *"Research for a Cyberinfrastructure-Enabled Carbon and Fuels Mapping Model Prototype (Phase 2)."* 2022. Greenberg, J.A.
- CALFIRE. *"Research for a Cyberinfrastructure-Enabled Carbon and Fuels Mapping Model Prototype (Phase 1)."* 2021. Ramirez, C. and J.A. Greenberg.
- California Air Resources Board. *"Advanced Carbon Modeling Techniques for the Forest Health Quantification Methodology."* 2021. Greenberg, J.A. and E. Hanan.
- NASA Carbon Monitoring System. *"A Phase 3 CMS that disaggregates forest biomass estimates in response to stakeholder needs: Seeing the Forest for the Trees."*
