import dask
import geopandas as gpd
import json
import numpy as np
import os
import rasterio
import rasterio.features
import time
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
def process_tile(window, lulc_fp, gdf, weights, pop_field):
    print(f"Processing tile at row {window.row_off}, col {window.col_off}")
    with rasterio.open(lulc_fp) as src:
        lulc_tile = src.read(1, window=window)
        transform = src.window_transform(window)
        crs = src.crs

    # Get bounding box of tile
    bbox = box(*rasterio.windows.bounds(window, transform))
    tile_blocks = gdf[gdf.intersects(bbox)].copy()
    if tile_blocks.empty:
        print(f"  Tile {window.row_off},{window.col_off}: No census blocks found")
        return np.zeros(lulc_tile.shape, dtype=np.float32), transform, window

    print(f"  Tile {window.row_off},{window.col_off}: Processing {len(tile_blocks)} census blocks")

    pop_raster = np.zeros_like(lulc_tile, dtype=np.float32)
    nodata = np.nan

    for idx, (_, row) in enumerate(tile_blocks.iterrows()):
        if idx % 100 == 0 and idx > 0:
            print(f"    Processed {idx}/{len(tile_blocks)} blocks in tile {window.row_off},{window.col_off}")

        geom = row.geometry
        pop = row[pop_field]

        # Mask LULC by polygon
        mask = rasterio.features.geometry_mask([geom], lulc_tile.shape, transform, invert=True)
        lc_masked = np.where(mask, lulc_tile, np.nan)

        # Assign weights
        weight_raster = np.zeros_like(lc_masked, dtype=float)
        for lc_class, w in weights.items():
            weight_raster[lc_masked == lc_class] = w

        # Normalize and allocate
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
    gdf = gpd.read_file(census_fp)
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
    with rasterio.open(lulc_fp) as src:
        width, height = src.width, src.height
        profile = src.profile
        print(f"   Raster dimensions: {width} x {height}")
        print(f"   CRS: {src.crs}")
        print(f"   Loaded in {time.time() - start_time:.2f} seconds")

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
    #results = [future.result() for future in futures]
    
    print(f"   All tasks completed in {time.time() - start_time:.2f} seconds")

    print("\n7. Combining results...")
    start_time = time.time()

    # Initialize full raster
    pop_raster = np.zeros((height, width), dtype=np.float32)
    with rasterio.open(lulc_fp) as src:
        transform = src.transform
        crs = src.crs

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
    profile.update(dtype='float32', compress='lzw', nodata=0)
    output_file = os.path.join(output_dir, "population_30m.tif")
    with rasterio.open(output_file, 'w', **profile) as dst:
        dst.write(pop_raster, 1)
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

# import os
# import time
# import json
# import numpy as np
# import geopandas as gpd
# import rasterio
# import rasterio.features
# from shapely.geometry import box
# from rasterio.windows import Window
# from dask.distributed import Client, as_completed

# try:
#     import psutil
# except Exception:
#     psutil = None

# # -----------------------------
# # CONFIGURATION
# # -----------------------------
# census_fp = os.getenv("CENSUS_FILE")
# lulc_fp   = os.getenv("LULC_FILE")
# output_dir = os.getenv("OUTPUT_DIR")
# tile_size  = int(os.getenv("TILE_SIZE"))
# pop_field  = os.getenv("POP_FIELD")

# LOG_EVERY = int(os.getenv("LOG_EVERY", "1"))           # log every N completed tiles
# LOG_TILE_STATS = os.getenv("LOG_TILE_STATS", "0") == "1"

# def log(msg):
#     ts = time.strftime("%Y-%m-%d %H:%M:%S")
#     print(f"[{ts}] {msg}", flush=True)

# def mem_note():
#     if not psutil:
#         return ""
#     rss = psutil.Process(os.getpid()).memory_info().rss / (1024**3)
#     return f" | RSS={rss:.2f} GiB"

# # Check required variables
# required_vars = [census_fp, lulc_fp, output_dir, pop_field]
# if any(v is None for v in required_vars):
#     log("ERROR: Missing required environment variables")
#     raise SystemExit(1)

# # LULC weights (example NLCD)
# weights = {
#     0: 0.0, 11: 0.0, 21: 1.0, 22: 1.0, 23: 1.0, 24: 1.0,
#     31: 0.0, 41: 0.0, 42: 0.0, 43: 0.0, 52: 0.3, 71: 0.2,
#     81: 0.5, 82: 0.5, 90: 0.0, 95: 0.0, 250: 0.0
# }

# os.makedirs(output_dir, exist_ok=True)

# # -----------------------------
# # TILING
# # -----------------------------
# def generate_tiles(width, height, ts):
#     for row_off in range(0, height, ts):
#         for col_off in range(0, width, ts):
#             yield Window(
#                 col_off, row_off,
#                 min(ts, width - col_off),
#                 min(ts, height - row_off)
#             )

# # -----------------------------
# # TILE PROCESS
# # -----------------------------
# def process_tile(window, lulc_fp, gdf, weights, pop_field):
#     # Runs on a worker; its prints go to the worker pod logs
#     with rasterio.open(lulc_fp) as src:
#         lulc_tile = src.read(1, window=window)
#         transform = src.window_transform(window)

#     bbox = box(*rasterio.windows.bounds(window, transform))
#     tile_blocks = gdf[gdf.intersects(bbox)].copy()
#     if tile_blocks.empty:
#         pop_raster = np.zeros(lulc_tile.shape, dtype=np.float32)
#         return pop_raster, transform, window

#     pop_raster = np.zeros_like(lulc_tile, dtype=np.float32)
#     for _, row in tile_blocks.iterrows():
#         geom = row.geometry
#         pop = float(row[pop_field])

#         mask = rasterio.features.geometry_mask([geom], lulc_tile.shape, transform, invert=True)
#         lc_masked = np.where(mask, lulc_tile, np.nan)

#         weight_raster = np.zeros_like(lc_masked, dtype=np.float32)
#         for lc_class, w in weights.items():
#             weight_raster[lc_masked == lc_class] = w

#         total_weight = np.nansum(weight_raster)
#         if total_weight > 0:
#             pop_raster += (weight_raster / total_weight) * pop

#     return pop_raster, transform, window

# # -----------------------------
# # MAIN
# # -----------------------------
# if __name__ == "__main__":
#     log("=== DASYMETRIC POPULATION ALLOCATION ===")
#     log(f"Census: {census_fp}")
#     log(f"LULC:   {lulc_fp}")
#     log(f"Out:    {output_dir}")
#     log(f"Tile:   {tile_size}")
#     log(f"Field:  {pop_field}")

#     # 1) Load census
#     t0 = time.time()
#     gdf = gpd.read_file(census_fp)
#     load_secs = time.time() - t0
#     log(f"Loaded {len(gdf)} census blocks in {load_secs:.2f}s{mem_note()}")
#     if pop_field not in gdf.columns:
#         log(f"ERROR: Population field '{pop_field}' not found. Columns: {list(gdf.columns)}")
#         raise SystemExit(1)

#     log(f"Population stats: min={gdf[pop_field].min()}, max={gdf[pop_field].max()}, "
#         f"mean={gdf[pop_field].mean():.2f}")

#     # 2) Raster meta
#     t1 = time.time()
#     with rasterio.open(lulc_fp) as src:
#         width, height = src.width, src.height
#         profile = src.profile
#         ras_crs = src.crs
#     log(f"Raster: {width}x{height}, CRS={ras_crs}, loaded in {time.time()-t1:.2f}s{mem_note()}")

#     # Ensure CRS alignment
#     if gdf.crs != ras_crs:
#         log(f"Reprojecting census from {gdf.crs} -> {ras_crs}…")
#         gdf = gdf.to_crs(ras_crs)
#         log("Reprojection done.")

#     # 3) Connect to Dask
#     dask_addr = os.environ["DASK_SCHEDULER"]
#     client = Client(dask_addr)
#     try:
#         min_workers = int(os.getenv("MIN_WORKERS", "1"))
#         client.wait_for_workers(min_workers)
#     except Exception:
#         pass
#     workers_ct = len(client.scheduler_info().get('workers', {}))
#     log(f"Dask connected: {client} | Workers={workers_ct}{mem_note()}")

#     # 3a) Scatter gdf (broadcast)
#     log("Scattering GeoDataFrame to workers…")
#     gdf_b = client.scatter(gdf, broadcast=True)
#     log("Scatter complete.")

#     # 4) Tiles
#     tiles = list(generate_tiles(width, height, tile_size))
#     total_tiles = len(tiles)
#     log(f"Generated {total_tiles} tiles.")

#     # 5) Build delayed tasks
#     import dask
#     tasks = [dask.delayed(process_tile)(w, lulc_fp, gdf_b, weights, pop_field) for w in tiles]
#     log(f"Created {len(tasks)} delayed tasks.")

#     # 6) Execute & FULL progress logs
#     t_exec = time.time()
#     futures = client.compute(tasks)
#     log("Submitted tasks; monitoring progress…")

#     # Keep ordered results and track failures
#     results = [None] * total_tiles
#     failures = []
#     idx_by_future = {f: i for i, f in enumerate(futures)}
#     started = time.time()

#     for fut in as_completed(futures):
#         i = idx_by_future[fut]
#         w = tiles[i]
#         try:
#             res = fut.result()
#             results[i] = res
#             if (i + 1) % LOG_EVERY == 0 or (i + 1) == total_tiles:
#                 log(f"[DONE {i+1}/{total_tiles}] tile at (row={int(w.row_off)}, col={int(w.col_off)}) "
#                     f"size=({int(w.height)}x{int(w.width)}) "
#                     f"elapsed={time.time()-started:.1f}s{mem_note()}")
#         except Exception as e:
#             failures.append((i, repr(e)))
#             log(f"[FAIL {i+1}/{total_tiles}] tile at (row={int(w.row_off)}, col={int(w.col_off)}) "
#                 f"ERROR: {e}")

#     log(f"Tile execution finished in {time.time()-t_exec:.1f}s "
#         f"(ok={total_tiles-len(failures)}, fail={len(failures)})")

#     if failures:
#         log("Failed tiles:")
#         for i, err in failures:
#             w = tiles[i]
#             log(f"  - idx={i} window(row={int(w.row_off)}, col={int(w.col_off)}, "
#                 f"h={int(w.height)}, w={int(w.width)}): {err}")
#         # You can choose to exit here if failures occurred
#         # raise SystemExit(2)

#     # Sanity: ensure all results are present
#     ok_count = sum(1 for r in results if r is not None)
#     if ok_count != total_tiles:
#         log(f"WARNING: only {ok_count}/{total_tiles} tiles completed successfully.")

#     # 7) Combine (with per-tile logs)
#     t_comb = time.time()
#     pop_raster = np.zeros((height, width), dtype=np.float32)
#     with rasterio.open(lulc_fp) as src:
#         transform = src.transform
#         crs = src.crs

#     covered_pixels = 0
#     for i, res in enumerate(results, 1):
#         if res is None:
#             continue
#         pop_tile, tile_transform, window = res
#         rows, cols = pop_tile.shape
#         row_off = int(window.row_off)
#         col_off = int(window.col_off)

#         if LOG_TILE_STATS:
#             tmin = float(np.nanmin(pop_tile)) if pop_tile.size else 0.0
#             tmax = float(np.nanmax(pop_tile)) if pop_tile.size else 0.0
#             tmean = float(np.nanmean(pop_tile)) if pop_tile.size else 0.0
#             stat_msg = f" stats(min={tmin:.3f}, max={tmax:.3f}, mean={tmean:.3f})"
#         else:
#             stat_msg = ""

#         log(f"Combining tile {i}/{total_tiles} at (row={row_off}, col={col_off}) "
#             f"size=({rows}x{cols}){stat_msg}{mem_note()}")

#         pop_raster[row_off:row_off + rows, col_off:col_off + cols] = pop_tile
#         covered_pixels += rows * cols

#     log(f"Combined {ok_count} tiles in {time.time()-t_comb:.2f}s; "
#         f"pixels covered={covered_pixels:,}/{height*width:,}{mem_note()}")
#     log(f"Population raster stats: min={pop_raster.min():.6f}, "
#         f"max={pop_raster.max():.6f}, mean={pop_raster.mean():.6f}")

#     # 8) Save output
#     t_save = time.time()
#     profile.update(dtype='float32', compress='lzw', nodata=0.0)
#     output_file = os.path.join(output_dir, "population_30m.tif")
#     with rasterio.open(output_file, 'w', **profile) as dst:
#         dst.write(pop_raster, 1)
#     log(f"Saved population raster: {output_file} in {time.time()-t_save:.2f}s")

#     # 9) STAC metadata
#     stac_metadata = {
#         "type": "Feature",
#         "stac_version": "1.0.0",
#         "id": "dasymetric_population",
#         "properties": {
#             "title": "Dasymetric Population Allocation",
#             "description": "Population distributed from census polygons to grid using LULC weights",
#             "license": "CC-BY-4.0",
#             "processing_time": time.time() - t0,
#             "tile_count": len(tiles),
#             "census_blocks": len(gdf),
#         },
#         "assets": {
#             "population_raster": {
#                 "href": output_file,
#                 "type": "image/tiff; application=geotiff",
#                 "roles": ["data"]
#             }
#         }
#     }
#     with open(os.path.join(output_dir, "stac_metadata.json"), "w") as f:
#         json.dump(stac_metadata, f, indent=2)
#     log("Wrote STAC sidecar: stac_metadata.json")

#     log("=== PROCESSING COMPLETE ===")















