def setup_dask_reports(output_folder, tile_id=None, slice_tag=None):
    if tile_id:
        reports_folder = output_folder / "reports"
        reports_folder.mkdir(parents=True, exist_ok=True)
        report_path = reports_folder / f"tile_{tile_id}_dask_report.html"
        return report_path
    elif slice_tag:
        reports_folder = output_folder / "reports"
        reports_folder.mkdir(parents=True, exist_ok=True)
        report_path = reports_folder / f"slice_{slice_tag}_dask_report.html"
        return report_path
    else:
        reports_folder = output_folder / "reports"
        reports_folder.mkdir(parents=True, exist_ok=True)
        report_path = reports_folder / f"dask_report.html"
        return report_path