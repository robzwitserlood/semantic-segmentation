import json
import pathlib
import requests
import time
import wget
import zipfile
import geopandas as gpd
from itertools import product
import numpy as np
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

   
def process_luchtfotos(path):
    """
    """
    # unpack tuple with bounds
    xmin, ymin, xmax, ymax = PARAMS['process_luchtfotos']['bbox']
    # floor min-bounds and ceil max-bounds to thousands  
    xmin = int(np.floor(xmin / 1_000) * 1_000)
    ymin = int(np.floor(ymin / 1_000) * 1_000)
    xmax = int(np.ceil(xmax / 1_000) * 1_000)
    ymax = int(np.ceil(ymax / 1_000) * 1_000)
    # create inclusive range
    xrange = range(xmin, xmax + 1_000, 1_000)
    yrange = range(ymin, ymax + 1_000, 1_000)
    # Create dummy file to avoid re-downloading if already done
    with open(path, 'wb') as fp:
        fp.write(bytes("dummy file to avoid re-downloading if already done", 'utf-8'))
    # loop through all combinations
    for x, y in tqdm(product(xrange, yrange)):
        # update url and get response
        url = PARAMS['process_luchtfotos']['url_format'].replace('X', str(x)).replace('Y', str(y))
        response = requests.get(url)
        # ugly code alert: workaround since url-format is not consistent 
        if response.status_code != 200:
            url = url.replace('/04/','/02/')
            response = requests.get(url)
        # update path and write to file
        updated_path = pathlib.Path(path.parent, path.name.replace('X', str(x)).replace('Y', str(y))) 
        # write to file if response is ok
        if response.status_code == 200:
            with open(updated_path, 'wb') as fp:
                fp.write(response.content)
        else:
            print(f'Download of {updated_path.name} failed, status code: {response.status_code}')


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