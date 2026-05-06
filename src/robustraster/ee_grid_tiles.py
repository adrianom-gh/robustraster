import math
import ee


def ee_covering_grid_tiles(aoi, crs: str, scale: float, max_pixels_per_tile: int):
    """
    Create rectangular grid tiles covering an AOI, aligned to the given CRS.
    Tiles are sized by sqrt(max_pixels_per_tile) * scale.

    If AOI already fits inside one tile, return a FeatureCollection with a single AOI feature.

    Parameters
    ----------
    aoi : ee.Geometry | ee.Feature | ee.FeatureCollection | geojson-like
        AOI object.
    crs : str
        CRS string like "EPSG:3310".
    scale : float
        Pixel scale (meters).
    max_pixels_per_tile : int
        Maximum number of pixels per tile (controls tile side length).

    Returns
    -------
    ee.FeatureCollection
        FeatureCollection of rectangular tiles.
    """

    # Normalize AOI to ee.Geometry safely
    if isinstance(aoi, ee.featurecollection.FeatureCollection):
        aoi_geom = aoi.geometry()
    elif isinstance(aoi, ee.feature.Feature):
        aoi_geom = aoi.geometry()
    else:
        aoi_geom = ee.Geometry(aoi)

    # Compute tile size from pixel budget
    tile_side_px = int(math.floor(math.sqrt(int(max_pixels_per_tile))))
    if tile_side_px <= 0:
        raise ValueError("max_pixels_per_tile must be > 0")

    tile_side_m = float(tile_side_px) * float(scale)
    if tile_side_m <= 0:
        raise ValueError("scale must be > 0")

    proj = ee.Projection(crs)

    # Compute AOI size in projection units (meters-ish)
    bounds = aoi_geom.bounds(ee.ErrorMargin(1), proj)
    coords = ee.List(bounds.coordinates().get(0))

    ll = ee.List(coords.get(0))  # lower-left
    ur = ee.List(coords.get(2))  # upper-right

    width = ee.Number(ur.get(0)).subtract(ll.get(0)).abs()
    height = ee.Number(ur.get(1)).subtract(ll.get(1)).abs()

    # If AOI already fits in one tile, return single AOI feature
    fits_in_one_tile = width.lte(tile_side_m).And(height.lte(tile_side_m))

    grid_tiles = aoi_geom.coveringGrid(proj.atScale(tile_side_m)).filter(
        ee.Filter.intersects(leftField=".geo", rightValue=aoi_geom)
    )

    single_tile_fc = ee.FeatureCollection([ee.Feature(aoi_geom)])

    tiles_fc = ee.FeatureCollection(
        ee.Algorithms.If(fits_in_one_tile, single_tile_fc, grid_tiles)
    )

    return tiles_fc


def clip_tiles_to_aoi(tiles_fc, aoi_geom):
    """
    Clip each tile geometry to AOI using intersection, and remove empty results.

    Parameters
    ----------
    tiles_fc : ee.FeatureCollection
        Rectangular tiles FeatureCollection.
    aoi_geom : ee.Geometry
        AOI geometry.

    Returns
    -------
    ee.FeatureCollection
        FeatureCollection where each tile's geometry is tile ∩ AOI.
    """

    def _clip_one_tile(f):
        f = ee.Feature(f)
        clipped = f.geometry().intersection(aoi_geom, ee.ErrorMargin(1))
        return ee.Feature(clipped).copyProperties(f).set(
            "clip_area", clipped.area(ee.ErrorMargin(1))
        )

    clipped_fc = ee.FeatureCollection(tiles_fc.map(_clip_one_tile)).filter(
        ee.Filter.gt("clip_area", 0)
    )

    return clipped_fc


def iter_tiles_from_fc(tiles_fc, batch_size=200):
    """
    Generator that yields ee.Geometry tiles from a server-side FeatureCollection.
    Tiles are fetched from Earth Engine in batches (default 200) so we don't
    pull the entire FeatureCollection client-side at once.
    """
    total = tiles_fc.size().getInfo()  # one safe getInfo() for tile count

    for offset in range(0, total, batch_size):
        batch_list = tiles_fc.toList(batch_size, offset)
        batch_len = min(batch_size, total - offset)

        for i in range(batch_len):
            geom = ee.Feature(batch_list.get(i)).geometry()
            yield geom