import json
import time
import wget
import zipfile
import pathlib
import requests

import pandas as pd
import geopandas as gpd

with open('config.json', 'r') as fp:
    config = json.load(fp)


def data_already_stored():
    """Check if bgt data is already stored.

    :return: indicator value for stored or not stored
    :rtype: bool
    """

    # Set storage path
    out_dir = pathlib.Path.cwd() / config['storagedirs']['bgt'] / config['bgt']['labeledgeometriesfilename']
    # Perform check
    if out_dir.exists():
        return True
    else:
        return False


def ready_to_start(overwrite):
    """Perform all checks if etl is ready to start.

    :param overwrite: indicator if stored bgt data should be overwritten
    :type overwrite: bool
    ...
    :return: indicator value
    :rtype: bool
    """

    if data_already_stored() and not overwrite:
        return False
    else:
        return True


def get_params(geo_filter):
    """Add user input for geofilter to parameters in config file.

    :param geo_filter: geographical region to be included in API call
    :type geo_filter: str (well-known text)
    ...
    :return: all parameters for api call
    :rtype: dict
    """

    params = config['bgt']['api_params']
    params['geofilter'] = geo_filter

    return params


def get_download_request_id(params):
    """Do post request and get download ID.

    :param params: parameters that define API call
    :type params: dict
    ...
    :return: ID representing the requested download
    :rtype: str
    """

    # Post request
    post_response = requests.post(config['bgt']['post_url'], json=params)
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
    get_url = config['bgt']['get_url']
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


def download_extract_and_store(download_link):
    """Download, extract and store bgt data.

    :param download_link: link to download bgt data
    :type download_link: str
    """

    # Set storage dir and path
    out_dir = pathlib.Path.cwd() / config['storagedirs']['bgt']
    # Create directory if non-existing
    out_dir.mkdir(parents=True, exist_ok=True)
    zip_path = pathlib.Path(out_dir, config['bgt']['zipfilename'])
    # Fill in download link
    download_url = config['bgt']['download_url']
    download_url = download_url.replace('/downloadLink', download_link)
    # Download
    wget.download(download_url, str(zip_path))
    # Extract
    with zipfile.ZipFile(zip_path, "r") as archive:
        archive.extractall(out_dir)


def concat_data_from_feature_type(feature_type, gdf_in):
    """Concatenate data of a feature type to GeoDataframe.

    :param feature_type: name of feature type
    :type feature_type: str
    :param gdf_in: initial GeoDataframe
    :type gdf_in: geopandas.geodataframe.GeoDataFrame
    ...
    :return: concatenated GeoDataframe
    :rtype: geopandas.geodataframe.GeoDataFrame
    """

    # Read GML file
    gml_filename = 'bgt_' + feature_type + '.gml'
    gml_path = pathlib.Path.cwd() / config['storagedirs']['bgt'] / gml_filename
    gdf = gpd.read_file(gml_path, engine='pyogrio')
    # Remove duplicates based on 'identificatie.lokaalID'
    gdf.drop_duplicates(subset='identificatie.lokaalID', keep="last", inplace=True)
    # Add featuretype as column
    gdf['featuretype'] = feature_type
    # Add classification label
    if feature_type in config['bgt']['monolabel_featuretypes']:
        gdf['label'] = config['bgt']['monolabel_featuretypes'][feature_type]
    else:
        gdf['label'] = gdf['bgt-fysiekVoorkomen'].apply(lambda x: config['bgt']['bgt-fysiekVoorkomen'][x])
    # Select columns
    gdf = gdf[config['bgt']['columns']]
    # Concatenate to concat_gdf
    concat_gdf = gpd.GeoDataFrame(pd.concat([gdf_in, gdf], ignore_index=True))

    return concat_gdf


def drop_unused_rows(gdf):
    """Drop rows that will not be used for labeling.

    :param gdf: initial GeoDataframe
    :type gdf: geopandas.geodataframe.GeoDataFrame
    ...
    :return: reduced GeoDataframe
    :rtype: geopandas.geodataframe.GeoDataFrame
    """

    # Delete geometry types without surface area
    mask1 = gdf.geometry.type.str.contains('Polygon')
    gdf = gdf[mask1]
    # Delete rows labeled as unknown (onbekend)
    mask2 = gdf.label != 'onbekend'
    gdf = gdf[mask2]

    return gdf


def transform(crs):
    """Transform bgt data.

    :param crs: coordinate reference frame of the geometries
    :type crs: pyproj.crs.crs.CRS
    """

    # Initialize GeoDataFrame
    gdf = gpd.GeoDataFrame(columns=config['bgt']['columns'], crs=crs)
    # Occupy GeoDataFrame with all data
    for feature_type in config['bgt']['api_params']['featuretypes']:
        # Occupy GeoDataFrame with data from one feature type
        gdf = concat_data_from_feature_type(feature_type, gdf)
    # Drop rows that will not be used for labelling
    gdf = drop_unused_rows(gdf)
    # Add column with surface area
    gdf['area'] = gdf.geometry.area
    # Write to file
    out_dir = pathlib.Path.cwd() / config['storagedirs']['bgt'] / config['bgt']['labeledgeometriesfilename']
    gdf.to_parquet(out_dir)


def load():
    """Load transformed data from file to GeoDataFrame.

    :return: transformed data
    :rtype: geopandas.geodataframe.GeoDataFrame
    """
    file_dir = pathlib.Path.cwd() / config['storagedirs']['bgt'] / config['bgt']['labeledgeometriesfilename']
    gdf = gpd.read_parquet(file_dir)

    return gdf


def process(geo_filter, crs, overwrite=False):
    """Perform etl processing steps.

    :param geo_filter: geographical region to be included in API call
    :type geo_filter: str (well-known text)
    :param crs: coordinate reference frame of the geometries
    :type crs: pyproj.crs.crs.CRS
    :param overwrite: indicator if stored bgt data should be overwritten
    :type overwrite: bool
    ...
    :return: transformed data
    :rtype: geopandas.geodataframe.GeoDataFrame
    """
    if ready_to_start(overwrite):
        params = get_params(geo_filter)
        download_id = get_download_request_id(params)
        download_link = get_download_link(download_id)
        download_extract_and_store(download_link)
        transform(crs)
    gdf = load()

    return gdf
