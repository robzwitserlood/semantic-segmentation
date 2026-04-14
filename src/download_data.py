import io
import pathlib
import requests
import time
import wget
import zipfile
import geopandas as gpd
from itertools import product
from concurrent.futures import ThreadPoolExecutor

import numpy as np
import rasterio
from rasterio.transform import from_origin
from PIL import Image
from tqdm import tqdm

from owslib.wfs import WebFeatureService

from config import config_download_data

# Set configuration parameters as global
PARAMS = config_download_data.copy()


def store_data_from_wfs(wfs_url, layer_name, path):
        """Store gml data.
        """
        # Initiate Web Feature Service instance
        wfs = WebFeatureService(wfs_url)
        # Get the desired layer
        response = wfs.getfeature(typename=[layer_name])
        # Write to file
        with open(path, 'wb') as f:
            f.write(response.read())


def update_globals_based_on_region_name():
        """Get geometry of the spatial scope.
        """
        path = pathlib.Path(PARAMS['process_bestuurlijkegebieden']['storage_dir'],
                            PARAMS['process_bestuurlijkegebieden']['file_name'])
        # Read data
        gdf = gpd.read_file(path, driver='GML')
        # Get geometry of spatial scope
        geometry = gdf.loc[gdf['naam'] == PARAMS['region_name']]['geometry'].values[0]
        # Create envelope of geometry
        bbox = geometry.buffer(10).envelope
        # Update global variable based on bbox
        PARAMS['process_bgt']['api_params']['geofilter'] = bbox.wkt
        PARAMS['process_luchtfotos']['bbox'] = bbox.bounds


def get_download_request_id():
    """Do post request and get download ID.

    :param params: parameters that define API call
    :type params: dict
    ...
    :return: ID representing the requested download
    :rtype: str
    """
    # Post request
    post_response = requests.post(url=PARAMS['process_bgt']['post_url'],
                                  json=PARAMS['process_bgt']['api_params'])
    # Get ID
    download_id = post_response.json()['downloadRequestId']

    return download_id


def get_download_link(download_id):
    """Get the download link once data is ready for download.

    :param download_id: ID representing the requested download
    :type download_id: str
    ...
    :return: link to download link bgt data
    :rtype: str
    """
    # Fill in download ID
    get_url = PARAMS['process_bgt']['get_url']
    get_url = get_url.replace('RequestId', download_id)
    # Wait until ready for download
    while True:
        # Get update on status
        get_response = requests.get(get_url)
        print(f"Status: {get_response.json()['status']}, progress: {get_response.json()['progress']}%", end='\r')
        # Check if completed
        if get_response.json()['status'] == 'COMPLETED':
            break
        else:
            time.sleep(3)
            continue
    # Get download link
    download_link = get_response.json()['_links']['download']['href']

    return download_link


def download_extract_and_store(download_link, path):
    """Download, extract and store bgt data.

    :param download_link: link to download bgt data
    :type download_link: str
    """
    # Fill in download link
    download_url = PARAMS['process_bgt']['download_url']
    download_url = download_url.replace('/downloadLink', download_link)
    # Download
    wget.download(download_url, str(path))
    # Extract
    with zipfile.ZipFile(path, "r") as archive:
        archive.extractall(path.parents[0])


def process_bestuurlijkegebieden(path):
    """
    """
    store_data_from_wfs(wfs_url=PARAMS['process_bestuurlijkegebieden']['wfs_url'],
                        layer_name=PARAMS['process_bestuurlijkegebieden']['layer_name'],
                        path=path)


def process_wijkenbuurten(path):
    """
    """
    store_data_from_wfs(wfs_url=PARAMS['process_wijkenbuurten']['wfs_url'],
                        layer_name=PARAMS['process_wijkenbuurten']['layer_name'],
                        path=path)


def process_bgt(path):
    """
    """
    download_id = get_download_request_id()
    download_link = get_download_link(download_id)
    download_extract_and_store(download_link, path)

   
def _fetch_wmts_tile(args):
    """Download one WMTS JPEG tile and return (row_idx, col_idx, rgb_array or None)."""
    url, ri, ci = args
    try:
        response = requests.get(url, timeout=30)
        if response.status_code == 200:
            img = np.array(Image.open(io.BytesIO(response.content)).convert('RGB'))
            return ri, ci, img
    except Exception:
        pass
    return ri, ci, None


def process_luchtfotos(path):
    """Download 2022 aerial imagery from PDOK WMTS and store as georeferenced GeoTIFF tiles.

    For each 1 km × 1 km cell in the region bbox, WMTS tiles at the configured
    zoom level are downloaded, stitched into a mosaic, and written as a
    GeoTIFF with EPSG:28992 CRS and affine transform.  The output filename
    convention (2022_X_Y_RGB_hrl.tif) and storage directory match the
    original layout expected by etl.py.
    """
    # WMTS grid constants for EPSG:28992
    ORIGIN_X, ORIGIN_Y = -285401.92, 903401.92
    ZOOM = PARAMS['process_luchtfotos']['wmts_zoom']
    URL_TEMPLATE = PARAMS['process_luchtfotos']['wmts_url_template']
    LAYER = PARAMS['process_luchtfotos']['wmts_layer']
    # OGC standard: pixel_size (m) = scale_denominator × 0.00028
    # Scale denominators halve with each zoom level; SD(0) = 12 288 000
    PIXEL_SIZE = 12_288_000 / (2 ** ZOOM) * 0.00028   # metres per pixel
    TILE_M = 256 * PIXEL_SIZE                           # tile edge in metres

    # Snap region bbox to 1 km grid boundaries
    xmin, ymin, xmax, ymax = PARAMS['process_luchtfotos']['bbox']
    xmin = int(np.floor(xmin / 1_000) * 1_000)
    ymin = int(np.floor(ymin / 1_000) * 1_000)
    xmax = int(np.ceil(xmax  / 1_000) * 1_000)
    ymax = int(np.ceil(ymax  / 1_000) * 1_000)

    # Sentinel file: presence signals that the download step has run,
    # so execute_if_allowed skips re-execution on subsequent pipeline runs.
    with open(path, 'wb') as fp:
        fp.write(b'wmts download sentinel')

    km_tiles = list(product(range(xmin, xmax, 1_000), range(ymin, ymax, 1_000)))
    for x, y in tqdm(km_tiles):
        # WMTS tile indices that fully cover [x, x+1000] × [y, y+1000]
        col_min = int(np.floor((x           - ORIGIN_X) / TILE_M))
        col_max = int(np.floor((x + 1_000   - ORIGIN_X) / TILE_M))
        row_min = int(np.floor((ORIGIN_Y - (y + 1_000)) / TILE_M))
        row_max = int(np.floor((ORIGIN_Y -  y          ) / TILE_M))

        n_tile_cols = col_max - col_min + 1
        n_tile_rows = row_max - row_min + 1

        # Affine transform anchored at top-left corner of the stitched mosaic
        x_left = ORIGIN_X + col_min * TILE_M
        y_top  = ORIGIN_Y - row_min * TILE_M
        transform = from_origin(x_left, y_top, PIXEL_SIZE, PIXEL_SIZE)

        mosaic = np.zeros((3, n_tile_rows * 256, n_tile_cols * 256), dtype=np.uint8)

        # Build one download task per WMTS tile
        tasks = [
            (URL_TEMPLATE.format(layer=LAYER, TileMatrix=ZOOM,
                                 TileCol=col_min + ci, TileRow=row_min + ri),
             ri, ci)
            for ri in range(n_tile_rows)
            for ci in range(n_tile_cols)
        ]

        with ThreadPoolExecutor(max_workers=16) as executor:
            for ri, ci, img in executor.map(_fetch_wmts_tile, tasks):
                if img is not None:
                    mosaic[:, ri*256:(ri+1)*256, ci*256:(ci+1)*256] = img.transpose(2, 0, 1)

        output_path = pathlib.Path(
            path.parent,
            path.name.replace('X', str(x)).replace('Y', str(y))
        )
        with rasterio.open(
            output_path, 'w',
            driver='GTiff',
            height=mosaic.shape[1],
            width=mosaic.shape[2],
            count=3,
            dtype='uint8',
            crs='EPSG:28992',
            transform=transform,
            compress='lzw',
        ) as dst:
            dst.write(mosaic)


def execute_if_allowed(func, allow_overwrite):
    """"
    """
    path = pathlib.Path(PARAMS[func.__name__]['storage_dir'],
                        PARAMS[func.__name__]['file_name'])
    path.parents[0].mkdir(parents=True, exist_ok=True)

    if path.exists() and not allow_overwrite:
        print(f'Did not run {func.__name__}: file already exists and overwriting not allowed')
    else:
        func(path)
        print(f'Succesfully completed {func.__name__}')


def main():
    execute_if_allowed(process_bestuurlijkegebieden, allow_overwrite=False)
    update_globals_based_on_region_name()
    execute_if_allowed(process_wijkenbuurten, allow_overwrite=False)
    execute_if_allowed(process_bgt, allow_overwrite=False)
    execute_if_allowed(process_luchtfotos, allow_overwrite=False)

if __name__ == "__main__":
    main()