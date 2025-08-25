import dask
import dask.dataframe as dd
import geopandas as gpd
import json
import numpy as np
import os
import xarray as xr
import rasterio
import rasterio.features
import rioxarray
import time
from rasterio.windows import Window
from rasterio.windows import transform as window_transform
from dask.distributed import as_completed

from dask.distributed import Client, progress, as_completed
from rasterio.windows import Window
from shapely.geometry import box


# -----------------------------
# CONFIGURATION
# -----------------------------
census_fp = os.getenv("CENSUS_FILE")
lulc_fp = os.getenv("LULC_FILE")
output_dir = os.getenv("OUTPUT_DIR")
tile_size = int(os.getenv("TILE_SIZE"))
pop_field = os.getenv("POP_FIELD")

# Check required variables
required_vars = [census_fp, lulc_fp, output_dir, pop_field]
if any(var is None for var in required_vars):
    print("ERROR: Missing required environment variables")
    exit(1)

# Weights for LULC classes (example NLCD)
weights = {
    0: 0.0,
    11: 0.0,
    21: 1.0,
    22: 1.0,
    23: 1.0,
    24: 1.0,
    31: 0.0,
    41: 0.0,
    42: 0.0,
    43: 0.0,
    52: 0.3,
    71: 0.2,
    81: 0.5,
    82: 0.5,
    90: 0.0,
    95: 0.0,
    250: 0.0
}

os.makedirs(output_dir, exist_ok=True)

# -----------------------------
# TILING FUNCTION
# -----------------------------
def generate_tiles(width, height, tile_size):
    for row_off in range(0, height, tile_size):
        for col_off in range(0, width, tile_size):
            yield Window(col_off, row_off,
                         min(tile_size, width - col_off),
                         min(tile_size, height - row_off))

# -----------------------------
# DASYMETRIC PROCESS FOR A TILE
# -----------------------------
# def process_tile(window, lulc_fp, gdf, weights, pop_field):
#     print(f"Processing tile at row {window.row_off}, col {window.col_off}")
#     # with rasterio.open(lulc_fp) as src:
#     #     lulc_tile = src.read(1, window=window)
#     #     transform = src.window_transform(window)
#     #     crs = src.crs

#     lulc_tile = rioxarray.open_rasterio(lulc_fp)

#     # Get bounding box of tile
#     bbox = box(*rasterio.windows.bounds(window, transform))
#     tile_blocks = gdf[gdf.intersects(bbox)].copy()
#     if tile_blocks.empty:
#         print(f"  Tile {window.row_off},{window.col_off}: No census blocks found")
#         return np.zeros(lulc_tile.shape, dtype=np.float32), transform, window

#     print(f"  Tile {window.row_off},{window.col_off}: Processing {len(tile_blocks)} census blocks")

#     pop_raster = np.zeros_like(lulc_tile, dtype=np.float32)
#     nodata = np.nan

#     for idx, (_, row) in enumerate(tile_blocks.iterrows()):
#         if idx % 100 == 0 and idx > 0:
#             print(f"    Processed {idx}/{len(tile_blocks)} blocks in tile {window.row_off},{window.col_off}")

#         geom = row.geometry
#         pop = row[pop_field]

#         # Mask LULC by polygon
#         mask = rasterio.features.geometry_mask([geom], lulc_tile.shape, transform, invert=True)
#         lc_masked = np.where(mask, lulc_tile, np.nan)

#         # Assign weights
#         weight_raster = np.zeros_like(lc_masked, dtype=float)
#         for lc_class, w in weights.items():
#             weight_raster[lc_masked == lc_class] = w

#         # Normalize and allocate
#         total_weight = np.nansum(weight_raster)
#         if total_weight > 0:
#             pop_raster += (weight_raster / total_weight) * pop

#     return pop_raster, transform, window



_RSD = None
def _get_rsd(path):
    global _RSD
    if _RSD is None:
        # chunks=None => returns NumPy on slice; no nested dask
        _RSD = rioxarray.open_rasterio(path, masked=True, chunks=None, cache=False)
    return _RSD

def process_tile(window: Window, lulc_fp, gdf, weights, pop_field):
    print(f"Processing tile at row {window.row_off}, col {window.col_off}")

    rsd = _get_rsd(lulc_fp)  # one open per worker process
    # slice by pixel indices (dims are ('band','y','x') or ('y','x') depending on source)
    y0, y1 = int(window.row_off), int(window.row_off + window.height)
    x0, x1 = int(window.col_off), int(window.col_off + window.width)
    # squeeze band if present and .values => NumPy array in-memory
    lulc_tile = rsd.isel(y=slice(y0, y1), x=slice(x0, x1)).squeeze().values

    # window-specific transform using the full-raster transform
    full_transform = rsd.rio.transform()
    transform = window_transform(window, full_transform)
    # crs = rsd.rio.crs  # if you need it

    # spatial filter with bbox for polygons
    from shapely.geometry import box
    bbox = box(*rasterio.windows.bounds(window, transform))
    tile_blocks = gdf[gdf.intersects(bbox)].copy()
    if tile_blocks.empty:
        return np.zeros(lulc_tile.shape, dtype=np.float32), transform, window

    pop_raster = np.zeros_like(lulc_tile, dtype=np.float32)
    for _, row in tile_blocks.iterrows():
        geom = row.geometry
        pop  = row[pop_field]
        mask = rasterio.features.geometry_mask([geom], lulc_tile.shape, transform, invert=True)
        lc_masked = np.where(mask, lulc_tile, np.nan)

        weight_raster = np.zeros_like(lc_masked, dtype=float)
        for lc_class, w in weights.items():
            weight_raster[lc_masked == lc_class] = w

        total_weight = np.nansum(weight_raster)
        if total_weight > 0:
            pop_raster += (weight_raster / total_weight) * pop

    return pop_raster, transform, window
# -----------------------------
# MAIN EXECUTION WITH DASK
# -----------------------------
if __name__ == "__main__":
    print("=== DASYMETRIC POPULATION ALLOCATION ===")
    print(f"Census file: {census_fp}")
    print(f"LULC file: {lulc_fp}")
    print(f"Output directory: {output_dir}")
    print(f"Tile size: {tile_size}")
    print(f"Population field: {pop_field}")

    print("\n1. Loading census data...")
    start_time = time.time()
    #gdf = gpd.read_file(census_fp)
    gdf = dd.read_parquet(census_fp)
    print(f"   Loaded {len(gdf)} census blocks in {time.time() - start_time:.2f} seconds")
    print(f"   Available columns: {list(gdf.columns)}")

    # Check if population field exists
    if pop_field not in gdf.columns:
        print(f"   ERROR: Population field '{pop_field}' not found!")
        print(f"   Available columns: {list(gdf.columns)}")
        exit(1)

    print(
        f"   Population stats: min={gdf[pop_field].min()}, max={gdf[pop_field].max()}, mean={gdf[pop_field].mean():.2f}")

    print("\n2. Loading LULC raster...")
    start_time = time.time()
    # with rasterio.open(lulc_fp) as src:
    #     width, height = src.width, src.height
    #     profile = src.profile
    #     print(f"   Raster dimensions: {width} x {height}")
    #     print(f"   CRS: {src.crs}")
    #     print(f"   Loaded in {time.time() - start_time:.2f} seconds")

    rsd = rioxarray.open_rasterio(lulc_fp)
    print(rsd)
  
    height, width = rsd.rio.shape
    transform = rsd.rio.transform()
    crs = rsd.rio.crs
    #profile = rsd.profile

    print("\n3. Connecting to Dask scheduler...")
    client = Client(os.getenv("DASK_SCHEDULER", None))
    print(f"   Dask Client: {client}")
    print(f"   Workers: {len(client.scheduler_info()['workers'])}")

    print("\n4. Generating tiles...")
    tiles = list(generate_tiles(width, height, tile_size))
    print(f"   Generated {len(tiles)} tiles")

    print("\n5. Creating Dask tasks...")
    tasks = []
    for i, window in enumerate(tiles):
        task = dask.delayed(process_tile)(window, lulc_fp, gdf, weights, pop_field)
        tasks.append(task)
    print(f"   Created {len(tasks)} tasks")

    print("\n6. Executing tasks with Dask...")
    start_time = time.time()

    # Submit all tasks and get futures
    futures = client.compute(tasks)

    # Monitor progress
    print("   Monitoring progress...")
    completed = 0
    for future in as_completed(futures):
        completed += 1
        print(f"   Progress: {completed}/{len(futures)} tiles completed ({100 * completed / len(futures):.1f}%)")

    # Get results
    results = [future.result() for future in futures]
    print(f"   All tasks completed in {time.time() - start_time:.2f} seconds")

    print("\n7. Combining results...")
    start_time = time.time()

    # Initialize full raster
    pop_raster = np.zeros((height, width), dtype=np.float32)
    # with rasterio.open(lulc_fp) as src:
    #     transform = src.transform
    #     crs = src.crs

    for i, (pop_tile, tile_transform, window) in enumerate(results):
        if i % 10 == 0:
            print(f"   Combining tile {i + 1}/{len(results)}")
        row_off = int(window.row_off)
        col_off = int(window.col_off)
        rows, cols = pop_tile.shape
        pop_raster[row_off:row_off + rows, col_off:col_off + cols] = pop_tile

    print(f"   Combined results in {time.time() - start_time:.2f} seconds")
    print(f"   Population raster stats: min={pop_raster.min()}, max={pop_raster.max()}, mean={pop_raster.mean():.2f}")

    print("\n8. Saving output...")
    start_time = time.time()

    # Save as COG
    # profile.update(dtype='float32', compress='lzw', nodata=0)
    # output_file = os.path.join(output_dir, "population_30m.tif")
    # with rasterio.open(output_file, 'w', **profile) as dst:
    #     dst.write(pop_raster, 1)

    da = xr.DataArray(pop_raster, dims=("y","x"))
    da = da.rio.write_transform(transform).rio.write_crs(crs).rio.write_nodata(0.0)
    output_file = os.path.join(output_dir, "population_30m.tif")
    da.rio.to_raster(
    output_file,
    dtype="float32",
    compress="lzw",
    tiled=True,
    blockxsize=512,
    blockysize=512,
    BIGTIFF="IF_SAFER",
)
    print(f"   Saved population raster: {output_file}")

    # Generate STAC metadata
    stac_metadata = {
        "type": "Feature",
        "stac_version": "1.0.0",
        "id": "dasymetric_population",
        "properties": {
            "title": "Dasymetric Population Allocation",
            "description": "Population distributed from census polygons to 30m grid using LULC weights",
            "license": "CC-BY-4.0",
            "processing_time": time.time() - start_time,
            "tile_count": len(tiles),
            "census_blocks": len(gdf)
        },
        "assets": {
            "population_raster": {
                "href": output_file,
                "type": "image/tiff; application=geotiff",
                "roles": ["data"]
            }
        }
    }
    with open(os.path.join(output_dir, "stac_metadata.json"), "w") as f:
        json.dump(stac_metadata, f, indent=2)
    print(f"   STAC metadata created in {time.time() - start_time:.2f} seconds")

    print("\n=== PROCESSING COMPLETE ===")
    print(f"Output files:")
    print(f"  - {output_file}")
    print(f"  - {os.path.join(output_dir, 'stac_metadata.json')}")
















