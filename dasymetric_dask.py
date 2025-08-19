# import dask
# import geopandas as gpd
# import json
# import numpy as np
# import os
# import rasterio
# import rasterio.features
# import time

# from dask.distributed import Client, progress, as_completed
# from rasterio.windows import Window
# from shapely.geometry import box


# # -----------------------------
# # CONFIGURATION
# # -----------------------------
# census_fp = os.getenv("CENSUS_FILE")
# lulc_fp = os.getenv("LULC_FILE")
# output_dir = os.getenv("OUTPUT_DIR")
# tile_size = int(os.getenv("TILE_SIZE"))
# pop_field = os.getenv("POP_FIELD")

# # Check required variables
# required_vars = [census_fp, lulc_fp, output_dir, pop_field]
# if any(var is None for var in required_vars):
#     print("ERROR: Missing required environment variables")
#     exit(1)

# # Weights for LULC classes (example NLCD)
# weights = {
#     0: 0.0,
#     11: 0.0,
#     21: 1.0,
#     22: 1.0,
#     23: 1.0,
#     24: 1.0,
#     31: 0.0,
#     41: 0.0,
#     42: 0.0,
#     43: 0.0,
#     52: 0.3,
#     71: 0.2,
#     81: 0.5,
#     82: 0.5,
#     90: 0.0,
#     95: 0.0,
#     250: 0.0
# }

# os.makedirs(output_dir, exist_ok=True)

# # -----------------------------
# # TILING FUNCTION
# # -----------------------------
# def generate_tiles(width, height, tile_size):
#     for row_off in range(0, height, tile_size):
#         for col_off in range(0, width, tile_size):
#             yield Window(col_off, row_off,
#                          min(tile_size, width - col_off),
#                          min(tile_size, height - row_off))

# # -----------------------------
# # DASYMETRIC PROCESS FOR A TILE
# # -----------------------------
# def process_tile(window, lulc_fp, gdf, weights, pop_field):
#     print(f"Processing tile at row {window.row_off}, col {window.col_off}")
#     with rasterio.open(lulc_fp) as src:
#         lulc_tile = src.read(1, window=window)
#         transform = src.window_transform(window)
#         crs = src.crs

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

# # -----------------------------
# # MAIN EXECUTION WITH DASK
# # -----------------------------
# if __name__ == "__main__":
#     print("=== DASYMETRIC POPULATION ALLOCATION ===")
#     print(f"Census file: {census_fp}")
#     print(f"LULC file: {lulc_fp}")
#     print(f"Output directory: {output_dir}")
#     print(f"Tile size: {tile_size}")
#     print(f"Population field: {pop_field}")

#     print("\n1. Loading census data...")
#     start_time = time.time()
#     gdf = gpd.read_file(census_fp)
#     print(f"   Loaded {len(gdf)} census blocks in {time.time() - start_time:.2f} seconds")
#     print(f"   Available columns: {list(gdf.columns)}")

#     # Check if population field exists
#     if pop_field not in gdf.columns:
#         print(f"   ERROR: Population field '{pop_field}' not found!")
#         print(f"   Available columns: {list(gdf.columns)}")
#         exit(1)

#     print(
#         f"   Population stats: min={gdf[pop_field].min()}, max={gdf[pop_field].max()}, mean={gdf[pop_field].mean():.2f}")

#     print("\n2. Loading LULC raster...")
#     start_time = time.time()
#     with rasterio.open(lulc_fp) as src:
#         width, height = src.width, src.height
#         profile = src.profile
#         print(f"   Raster dimensions: {width} x {height}")
#         print(f"   CRS: {src.crs}")
#         print(f"   Loaded in {time.time() - start_time:.2f} seconds")

#     print("\n3. Connecting to Dask scheduler...")
#     client = Client(os.getenv("DASK_SCHEDULER", None))
#     print(f"   Dask Client: {client}")
#     print(f"   Workers: {len(client.scheduler_info()['workers'])}")

#     print("\n4. Generating tiles...")
#     tiles = list(generate_tiles(width, height, tile_size))
#     print(f"   Generated {len(tiles)} tiles")

#     print("\n5. Creating Dask tasks...")
#     tasks = []
#     for i, window in enumerate(tiles):
#         task = dask.delayed(process_tile)(window, lulc_fp, gdf, weights, pop_field)
#         tasks.append(task)
#     print(f"   Created {len(tasks)} tasks")

#     print("\n6. Executing tasks with Dask...")
#     start_time = time.time()

#     # Submit all tasks and get futures
#     futures = client.compute(tasks)

#     # Monitor progress
#     print("   Monitoring progress...")
#     completed = 0
#     for future in as_completed(futures):
#         completed += 1
#         print(f"   Progress: {completed}/{len(futures)} tiles completed ({100 * completed / len(futures):.1f}%)")

#     # Get results
#     results = [future.result() for future in futures]
#     print(f"   All tasks completed in {time.time() - start_time:.2f} seconds")

#     print("\n7. Combining results...")
#     start_time = time.time()

#     # Initialize full raster
#     pop_raster = np.zeros((height, width), dtype=np.float32)
#     with rasterio.open(lulc_fp) as src:
#         transform = src.transform
#         crs = src.crs

#     for i, (pop_tile, tile_transform, window) in enumerate(results):
#         if i % 10 == 0:
#             print(f"   Combining tile {i + 1}/{len(results)}")
#         row_off = int(window.row_off)
#         col_off = int(window.col_off)
#         rows, cols = pop_tile.shape
#         pop_raster[row_off:row_off + rows, col_off:col_off + cols] = pop_tile

#     print(f"   Combined results in {time.time() - start_time:.2f} seconds")
#     print(f"   Population raster stats: min={pop_raster.min()}, max={pop_raster.max()}, mean={pop_raster.mean():.2f}")

#     print("\n8. Saving output...")
#     start_time = time.time()

#     # Save as COG
#     profile.update(dtype='float32', compress='lzw', nodata=0)
#     output_file = os.path.join(output_dir, "population_30m.tif")
#     with rasterio.open(output_file, 'w', **profile) as dst:
#         dst.write(pop_raster, 1)
#     print(f"   Saved population raster: {output_file}")

#     # Generate STAC metadata
#     stac_metadata = {
#         "type": "Feature",
#         "stac_version": "1.0.0",
#         "id": "dasymetric_population",
#         "properties": {
#             "title": "Dasymetric Population Allocation",
#             "description": "Population distributed from census polygons to 30m grid using LULC weights",
#             "license": "CC-BY-4.0",
#             "processing_time": time.time() - start_time,
#             "tile_count": len(tiles),
#             "census_blocks": len(gdf)
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
#     print(f"   STAC metadata created in {time.time() - start_time:.2f} seconds")

#     print("\n=== PROCESSING COMPLETE ===")
#     print(f"Output files:")
#     print(f"  - {output_file}")
#     print(f"  - {os.path.join(output_dir, 'stac_metadata.json')}")




import dask
import geopandas as gpd
import json
import numpy as np
import os
import rasterio
import rasterio.features
import time

from dask.distributed import Client, as_completed
from rasterio.windows import Window
from shapely.geometry import box

# -----------------------------
# CONFIGURATION
# -----------------------------
census_fp  = os.getenv("CENSUS_FILE")
lulc_fp    = os.getenv("LULC_FILE")
output_dir = os.getenv("OUTPUT_DIR")
tile_size  = int(os.getenv("TILE_SIZE", "1024"))
pop_field  = os.getenv("POP_FIELD")

required = [("CENSUS_FILE", census_fp), ("LULC_FILE", lulc_fp),
            ("OUTPUT_DIR", output_dir), ("POP_FIELD", pop_field)]
missing = [k for k,v in required if not v]
if missing:
    print(f"ERROR: Missing required env vars: {', '.join(missing)}")
    raise SystemExit(1)

os.makedirs(output_dir, exist_ok=True)
tile_out_dir = os.path.join(output_dir, "tiles")
os.makedirs(tile_out_dir, exist_ok=True)

# Example NLCD weights (tune as you like)
weights = {
    0: 0.0, 11: 0.0, 21: 1.0, 22: 1.0, 23: 1.0, 24: 1.0,
    31: 0.0, 41: 0.0, 42: 0.0, 43: 0.0, 52: 0.3, 71: 0.2,
    81: 0.5, 82: 0.5, 90: 0.0, 95: 0.0, 250: 0.0,
}

# -----------------------------
# HELPERS
# -----------------------------
def generate_tiles(width, height, ts):
    for row_off in range(0, height, ts):
        for col_off in range(0, width, ts):
            yield Window(col_off, row_off,
                         min(ts, width - col_off),
                         min(ts, height - row_off))

def process_tile(row_off, col_off, rows, cols, lulc_fp, weights, pop_field,
                 idxs, gdf, tile_dir):
    """
    Worker-side function:
    - Reads LULC window
    - Subsets polygons to the tile bbox using provided indices
    - Allocates population per weights
    - Writes a per-tile GeoTIFF
    - Returns small metadata only
    """
    w = Window(col_off, row_off, cols, rows)
    with rasterio.open(lulc_fp) as src:
        lulc_tile = src.read(1, window=w)
        transform = src.window_transform(w)
        crs = src.crs

    # Subset polygons for this tile using precomputed indices
    if idxs is None or len(idxs) == 0:
        # No polygons intersect this window; just write zeros
        pop_tile = np.zeros(lulc_tile.shape, dtype=np.float32)
    else:
        tile_blocks = gdf.iloc[idxs]
        if tile_blocks.empty:
            pop_tile = np.zeros(lulc_tile.shape, dtype=np.float32)
        else:
            pop_tile = np.zeros_like(lulc_tile, dtype=np.float32)
            # For each polygon, mask and distribute by weights
            # (Straightforward loop; optimize further if needed)
            for _, row in tile_blocks.iterrows():
                geom = row.geometry
                pop  = float(row[pop_field])

                mask = rasterio.features.geometry_mask([geom], lulc_tile.shape, transform, invert=True)
                lc_masked = np.where(mask, lulc_tile, np.nan)

                weight_r = np.zeros_like(lc_masked, dtype=np.float32)
                # assign weights (vectorized by class)
                for lc_class, wgt in weights.items():
                    # fill only where class matches; leaves NaN elsewhere
                    weight_r[lc_masked == lc_class] = wgt

                total = np.nansum(weight_r)
                if total > 0:
                    pop_tile += (weight_r / total) * pop

    # Write this tile to disk (small, local write on the node)
    tile_path = os.path.join(tile_dir, f"tile_r{row_off}_c{col_off}.tif")
    profile = {
        "driver": "GTiff",
        "height": rows,
        "width": cols,
        "count": 1,
        "dtype": "float32",
        "crs": crs,
        "transform": transform,
        "compress": "lzw",
        "tiled": True,
        "blockxsize": min(cols, 512),
        "blockysize": min(rows, 512),
        "nodata": 0.0,
    }
    with rasterio.open(tile_path, "w", **profile) as dst:
        dst.write(pop_tile, 1)

    return {
        "row_off": int(row_off),
        "col_off": int(col_off),
        "rows": int(rows),
        "cols": int(cols),
        "path": tile_path,
    }

# -----------------------------
# MAIN
# -----------------------------
if __name__ == "__main__":
    print("=== DASYMETRIC POPULATION ALLOCATION ===")
    print(f"Census: {census_fp}")
    print(f"LULC:   {lulc_fp}")
    print(f"Out:    {output_dir}")
    print(f"Tile:   {tile_size}")
    print(f"Field:  {pop_field}")

    # 1) Load census polygons (client-side) and ensure CRS matches raster
    t0 = time.time()
    gdf = gpd.read_file(census_fp)
    with rasterio.open(lulc_fp) as src0:
        raster_crs = src0.crs
        width, height = src0.width, src0.height
        full_transform = src0.transform
        full_profile = src0.profile
    if gdf.crs != raster_crs:
        gdf = gdf.to_crs(raster_crs)
    if pop_field not in gdf.columns:
        raise SystemExit(f"ERROR: '{pop_field}' not in census columns: {list(gdf.columns)}")
    print(f"Loaded {len(gdf)} blocks in {time.time()-t0:.2f}s; CRS aligned: {gdf.crs == raster_crs}")

    # 2) Dask client
    client = Client(os.getenv("DASK_SCHEDULER", None))
    print(f"Dask: {client}; workers: {len(client.scheduler_info()['workers'])}")

    # 3) Build tiles and pre-index polygons → per-tile lists (tiny to ship)
    print("Indexing polygons per tile…")
    tiles = list(generate_tiles(width, height, tile_size))
    sindex = gdf.sindex  # rtree/pygeos spatial index
    tile_indices = []
    for w in tiles:
        bbox = box(*rasterio.windows.bounds(w, full_transform))
        # query returns integer indices of intersecting rows
        idxs = list(sindex.query(bbox, predicate="intersects"))
        tile_indices.append(idxs)

    # 4) Scatter the GeoDataFrame ONCE to all workers
    gdf_b = client.scatter(gdf, broadcast=True)

    # 5) Submit worker tasks (each writes its own tile to disk)
    print(f"Submitting {len(tiles)} tile tasks…")
    futures = []
    for w, idxs in zip(tiles, tile_indices):
        f = client.submit(
            process_tile,
            int(w.row_off), int(w.col_off), int(w.height), int(w.width),
            lulc_fp, weights, pop_field,
            idxs, gdf_b, tile_out_dir,
            pure=False  # tiles overlap in logic; safer to disable caching
        )
        futures.append(f)

    # 6) Stream progress & collect small metadata (no big gathers)
    metas = []
    done = 0
    total = len(futures)
    print("Monitoring progress…")
    for fut in as_completed(futures):
        metas.append(fut.result())  # tiny dict
        done += 1
        if done % 10 == 0 or done == total:
            print(f"Progress: {done}/{total} tiles ({100*done/total:.1f}%)")

    # 7) Final mosaic write (sequential; low memory on client)
    print("Writing final mosaic…")
    out_profile = full_profile.copy()
    out_profile.update(
        dtype="float32",
        count=1,
        compress="lzw",
        nodata=0.0,
        tiled=True,
        blockxsize=min(tile_size, 512),
        blockysize=min(tile_size, 512),
    )
    out_path = os.path.join(output_dir, "population_30m.tif")
    t_merge = time.time()
    with rasterio.open(out_path, "w", **out_profile) as dst:
        for i, m in enumerate(metas, 1):
            if i % 50 == 0 or i == len(metas):
                print(f"  Merging tile {i}/{len(metas)}")
            w = Window(m["col_off"], m["row_off"], m["cols"], m["rows"])
            with rasterio.open(m["path"]) as src_tile:
                arr = src_tile.read(1)
            dst.write(arr, 1, window=w)

    print(f"Mosaic written to {out_path} in {time.time()-t_merge:.1f}s")

    # 8) STAC sidecar
    stac = {
        "type": "Feature",
        "stac_version": "1.0.0",
        "id": "dasymetric_population",
        "properties": {
            "title": "Dasymetric Population Allocation",
            "description": "Population distributed from census polygons to 30m grid using LULC weights",
            "license": "CC-BY-4.0",
            "tile_count": len(tiles),
            "census_blocks": len(gdf),
        },
        "assets": {
            "population_raster": {
                "href": out_path,
                "type": "image/tiff; application=geotiff",
                "roles": ["data"],
            }
        },
    }
    with open(os.path.join(output_dir, "stac_metadata.json"), "w") as f:
        json.dump(stac, f, indent=2)

    print("=== PROCESSING COMPLETE ===")
    print(f"Output:\n  - {out_path}\n  - {os.path.join(output_dir, 'stac_metadata.json')}")


