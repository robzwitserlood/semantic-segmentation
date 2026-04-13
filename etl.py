import time
import h5py
import pathlib
import psycopg
import argparse

import numpy as np
import pandas as pd
import shapely as shl
import rasterio as rs
import geopandas as gpd

from tqdm import tqdm
from math import log10

from functools import wraps
from rasterio.mask import mask
from shapely.geometry import box
from skimage.filters import sobel
from skimage.color import rgb2gray
from scipy.signal import convolve2d
from rasterio.merge import merge
from rasterio.io import MemoryFile
from rasterio.features import rasterize
from skimage.measure import shannon_entropy
from skimage.metrics import mean_squared_error

from config import config_etl_aerial, config_etl_satellite
import utils as ut


PARAMS = dict()


def set_config_params():
    """Set global configuration parameters
    based in command line argument
    """
    # Initialize command line argument parser
    parser = argparse.ArgumentParser()
    # Define argument type
    parser.add_argument('imagery', type=str)
    # Get parsed arguments
    args = parser.parse_args()
    # Select the corresponding configuration parameters
    global PARAMS
    if args.imagery == 'satellite':
        PARAMS.update(config_etl_satellite)
    elif args.imagery == 'aerial':
        PARAMS.update(config_etl_aerial)


def timeit(func):
    @wraps(func)
    def timeit_wrapper(*args, **kwargs):
        start_time = time.perf_counter()
        print(f'Started {func.__name__}')
        result = func(*args, **kwargs)
        end_time = time.perf_counter()
        total_time = end_time - start_time
        print(f'\tCompleted {func.__name__} in {total_time:.4f} seconds')
        return result
    return timeit_wrapper


def timeit_with_args(func):
    @wraps(func)
    def timeit_wrapper(*args, **kwargs):
        start_time = time.perf_counter()
        print(f'Started {func.__name__}{args}')
        result = func(*args, **kwargs)
        end_time = time.perf_counter()
        total_time = end_time - start_time
        print(f'\tCompleted {func.__name__}{args} in {total_time:.4f} seconds')
        return result
    return timeit_wrapper


@timeit
def create_postgis_database():
    """Creates and connects a postgis database and returns
    the connection and cursor to the database

    :return: connection and cursor to the database
    :rtype: psycopg.extensions.cursor, psycopg.extensions.connection
    """
    # connect to postgres
    conn = psycopg.connect(f"""
    user={PARAMS['postgres']['user']}
    password={PARAMS['postgres']['password']}
    """, autocommit=True)
    cur = conn.cursor()
    # create database with UTF8 encoding
    cur.execute(f"""
    DROP DATABASE IF EXISTS {PARAMS['postgres']['db_name']}
    """)
    cur.execute(f"""
    CREATE DATABASE {PARAMS['postgres']['db_name']}
    WITH ENCODING 'utf8'
    """)
    # close connection to database
    conn.close()
    # connect to database
    conn, cur = ut.get_connection_and_cursor(PARAMS['postgres']['db_name'])
    cur.execute("CREATE EXTENSION postgis")

    return cur, conn


@timeit
def drop_and_create_tables(cur, conn):
    """Drops and creates all tables using the queries in
    'create_table_queries' list

    :param cur: cursor to the database
    :type cur: psycopg.extensions.cursor
    :param conn: connection to the database
    :type conn: psycopg.extensions.connection
    """
    for query in PARAMS['postgres']['create_table_queries']:
        cur.execute(query)
        conn.commit()


def get_data_dimImageFiles(src):
    """Get data for dimImageFiles table from source file

    :param src: source file
    :type src: rasterio.io.DatasetReader
    ...
    :return: data for dimImageFiles table
    :rtype: tuple
    """
    data = (
        src.name.split('/')[-1],
        src.bounds.left,
        src.bounds.bottom,
        src.bounds.right,
        src.bounds.top,
        box(*src.bounds).wkt
    )
    return data


@timeit
def insert_data_dimImageFiles(cur, conn):
    """Insert data in dimImageFiles

    :param cur: cursor to the database
    :type cur: psycopg.extensions.cursor
    :param conn: connection to the database
    :type conn: psycopg.extensions.connection
    """
    # Get directory and name format of image files
    dir_imagery = pathlib.Path(PARAMS['postgres']['dir_imagery'])
    file_format = PARAMS['postgres']['file_format_imagery']
    # Initiate list for data
    data = list()
    # Loop through files and append data
    for path in dir_imagery.glob(file_format):
        with rs.open(path, 'r') as src:
            data.append(get_data_dimImageFiles(src))
    # Insert data in database
    cur.executemany(PARAMS['postgres']['insert_query_dimImageFiles'], data)
    conn.commit()


def get_geometry_to_be_tiled():
    """Get geometry that should be tiled, i.e. the geometry that should
    be competely covered by the image tiles in the dataset

    :return: geometry to be tiled
    :rtype: shapely.geometry
    """
    # Load data in geodataframe to select the desired geometry
    path = pathlib.Path(PARAMS['postgres']['dir_geom_to_be_tiled'],
                        PARAMS['postgres']['file_geom_to_be_tiled'])
    gdf = gpd.read_file(path)
    # Extract the desired geometry from the geodataframe
    geom = gdf[gdf.naam == PARAMS['postgres']['region_name']].\
        geometry.values[0]

    return geom


def tile_geometry(geom, cur):
    """Tile the input geometry, i.e. create an array of tiles
    that completely covers the geometry

    :param geom: geometry to be tiled
    :type geom: shapely.geometry
    :param cur: cursor to the database
    :type cur: psycopg.extensions.cursor
    ...
    :return: array of tiles that cover the geometry
    :rtype: numpy.array
    :return: affine of the array of tiles
    :rtype: rasterio affine.Affine
    """
    # Extract coordinates top-left point to be covered by tiles
    topleft = (geom.bounds[0], geom.bounds[3])
    # Select the image file that contains the top-left point
    cur.execute(PARAMS['postgres']['select_image_file_by_point'], topleft)
    response = cur.fetchone()
    # Read the image file that contains the top-left point
    path = pathlib.Path(PARAMS['postgres']['dir_imagery'], response[0])
    with rs.open(path) as image_file:
        # Get the coordinates of the pixel that contains the top-left point
        _, out_transform = mask(dataset=image_file,
                                shapes=[geom],
                                all_touched=True,
                                crop=True)
        # Set the desired tile dimensions (width and height in pixels)
        transform = out_transform * image_file.transform.\
            scale(PARAMS['postgres']['tile_width_in_pixels'],
                  PARAMS['postgres']['tile_height_in_pixels'])
    # Extract coordinates bottom-right point to be covered by tiles
    bottomright = (geom.bounds[2], geom.bounds[1])
    # Get a rectangular grid of tiles that covers the envelope of the geometry
    out_shape = rs.transform.rowcol(transform=transform,
                                    xs=bottomright[0],
                                    ys=bottomright[1])
    # Mark the tiles in the grid that touch the geometry itself
    tile_grid = rasterize([geom],
                          out_shape=out_shape,
                          transform=transform,
                          all_touched=True)

    return tile_grid, transform


def extract_tile_features(tile_grid, transform):
    """Extract features of the tiles that should be in the dataset

    :param tile_grid: array of tiles that cover the geometry
    :type tile_grid: numpy.array
    :param transform: affine of the array of tiles
    :type transform: rasterio affine.Affine
    ...
    :return: features of the tiles
    :rtype: list
    """
    # Select only tiles that touch the geometry
    tile_row_col = np.argwhere(tile_grid > 0)
    # Extract row and column indices, transform to python int
    rows = [int(row) for row in tile_row_col[:, 0]]
    cols = [int(col) for col in tile_row_col[:, 1]]
    # Extract bounds
    bound_left, bound_top = rs.transform.xy(transform=transform,
                                            rows=rows,
                                            cols=cols,
                                            offset='ul')
    bound_right, bound_bottom = rs.transform.xy(transform=transform,
                                                rows=rows,
                                                cols=cols,
                                                offset='lr')
    # Extract wkt representation of bounding box of tile
    bounds = zip(bound_left, bound_bottom, bound_right, bound_top)
    bbox_geom = [box(*bound).wkt for bound in bounds]
    # Create tuple of all features
    features = [rows,
                cols,
                bound_left,
                bound_top,
                bound_right,
                bound_bottom,
                bbox_geom]
    # Transpose to facilitate inserting the data in the database
    features = list(zip(*features))

    return features


@timeit
def insert_data_dimTiles(cur, conn):
    """Insert data in dimTiles

    :param cur: cursor to the database
    :type cur: psycopg.extensions.cursor
    :param conn: connection to the database
    :type conn: psycopg.extensions.connection
    """
    # Get geometry that should be tiled
    geom = get_geometry_to_be_tiled()
    # Tile the geometry
    tile_grid, transform = tile_geometry(geom, cur)
    # Extract features of all tiles that should be in the dataset
    features = extract_tile_features(tile_grid, transform)
    # Write features to database
    cur.executemany(PARAMS['postgres']['insert_query_dimTiles'], features)
    conn.commit()


@timeit
def insert_data_factTilesFiles(cur, conn):
    """Insert data in factTilesFiles

    :param cur: cursor to the database
    :type cur: psycopg.extensions.cursor
    :param conn: connection to the database
    :type conn: psycopg.extensions.connection
    """
    cur.execute(PARAMS['postgres']['insert_query_factTilesFiles'])
    conn.commit()


def drop_unused_rows(gdf):
    """Drop rows that will not be used for labeling

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
    mask2 = gdf.label != 2
    gdf = gdf[mask2]

    return gdf


@timeit_with_args
def extract_ground_truth_data(layer):
    """Read bgt input file, transform based on configuration settings
    and return ground truth data

    :param layer: name of layer represented by input file
    :type layer: str
    """
    # Read as geodataframe for transformation purposes
    path = pathlib.Path(PARAMS['postgres']['bgt_storage_dir'],
                        f'bgt_{layer}.gml')
    gdf = gpd.read_file(path, engine='pyogrio')
    # Remove duplicates based on 'identificatie.lokaalID'
    gdf.drop_duplicates(subset='identificatie.lokaalID',
                        keep="last",
                        inplace=True)
    # Add layer name as column
    gdf['layer'] = layer
    # Add wkt representation of geometry to write to postgis
    gdf['geom_wkt'] = gdf['geometry'].apply(lambda geom: geom.wkt)
    # Add ground truth label based on configuration settings
    if layer in PARAMS['postgres']['layer_to_label']:
        gdf['label'] = PARAMS['postgres']['layer_to_label'][layer]
    else:
        gdf['label'] = gdf['bgt-fysiekVoorkomen'].\
            apply(lambda x: PARAMS['postgres']['fysiekVoorkomen_to_label'][x])
    # Drop rows that will not be used for ground truth labelling
    gdf = drop_unused_rows(gdf)
    # Select columns
    gdf = gdf[[
        'identificatie.lokaalID',
        'layer',
        'label',
        'geom_wkt'
    ]]
    # Convert to list of tuples for postgis
    ground_truth_data = gdf.to_records(index=False).tolist()

    return ground_truth_data


@timeit
def insert_data_dimGroundTruth(cur, conn):
    """Insert data in dimGroundTruth

    :param cur: cursor to the database
    :type cur: psycopg.extensions.cursor
    :param conn: connection to the database
    :type conn: psycopg.extensions.connection
    """
    for layer in PARAMS['postgres']['bgt_used_layers']:
        ground_truth_data = extract_ground_truth_data(layer)
        cur.executemany(PARAMS['postgres']['insert_query_dimGroundTruth'],
                        ground_truth_data)
        conn.commit()


@timeit
def insert_data_factTilesGroundTruth(cur, conn):
    """Insert data in factTilesGroundTruth

    :param cur: cursor to the database
    :type cur: psycopg.extensions.cursor
    :param conn: connection to the database
    :type conn: psycopg.extensions.connection
    """
    # Temporarily drop constraints to speed up insert statement
    cur.execute(PARAMS['postgres']['drop_constraints_factTilesGroundTruth'])
    conn.commit()
    # Partition data by row to avoid out of memory
    cur.execute(PARAMS['postgres']['select_distinct_row_dimTiles'])
    # Insert data per partition
    for row in tqdm(cur.fetchall()):
        cur.execute(PARAMS['postgres']['insert_query_factTilesGroundTruth'],
                    row)
        conn.commit()
    # Reinstate constraints
    cur.execute(PARAMS['postgres']['reinst_constraints_factTilesGroundTruth'])
    conn.commit()


def insert_data_in_tables(cur, conn):
    """Insert data in all tables

    :param cur: cursor to the database
    :type cur: psycopg.extensions.cursor
    :param conn: connection to the database
    :type conn: psycopg.extensions.connection
    """
    insert_data_dimImageFiles(cur, conn)
    insert_data_dimTiles(cur, conn)
    insert_data_factTilesFiles(cur, conn)
    insert_data_dimGroundTruth(cur, conn)
    insert_data_factTilesGroundTruth(cur, conn)


def get_raster_parts(engine, tile_id, tile):
    """Get input (rgb) data for one tile

    :param engine: engine for the database
    :type engine: sqlalchemy.engine.Engine
    :param tile_id: tile ID
    :type tile_id: int
    :param tile: tile geometry
    :type tile: shapely.geometry
    ...
    :return: parts of the raster, one part for each file
    :rtype: list
    :return: Affine transformation object of top-left raster
    :rtype: affine.Affine
    """
    # Get all filenames in correct order (top to bottom, left to right)
    df = pd.read_sql(f"""
                     SELECT dIF.file_name, dIF.bound_left, dIF.bound_bottom
                     FROM factTilesFiles fTF
                     JOIN dimImageFiles dIF ON dIF.file_id = fTF.file_id
                     WHERE fTF.tile_id = {tile_id}
                     ORDER BY bound_bottom DESC, bound_left ASC
                     """, engine)
    # Initialize list of raster parts
    raster_parts = list()
    # Initialize affile of top left corner
    transform_topleft = None
    # Set storage path of files
    path = pathlib.Path(PARAMS['postgres']['dir_imagery'])
    # Read all pixels values in tile
    for i, file_name in enumerate(df.file_name):
        with rs.open(path / file_name, 'r') as src:
            raster_part, transform_part = mask(src, [tile], crop=True)
            if i == 0:
                transform_topleft = transform_part
            raster_part = ut.correct_raster_error_if_exists(raster_part)
            raster_parts.append(raster_part)

    return raster_parts, transform_topleft


def concat_raster_parts(raster_parts):
    """Concatenate all raster parts to one rgb image
    of 256 x 256 pixels

    :param raster_parts: parts of the raster, one part for each file
    :type raster_parts: list
    ...
    :return: rgb data for tile
    :rtype: numpy.array
    """
    # Initialize with correct shape
    raster = np.zeros((3, 256, 256), dtype=np.uint8)
    # Select the correct scenario
    if len(raster_parts) == 1:
        # Tile is completely covered by 1 image file
        raster[:, :, :] = raster_parts[0]
    elif len(raster_parts) == 4:
        # Tile is split over 4 image files
        col_offset = raster_parts[0].shape[1]
        row_offset = raster_parts[0].shape[2]
        raster[:, :col_offset, :row_offset] = raster_parts[0]
        raster[:, :col_offset, row_offset:] = raster_parts[1]
        raster[:, col_offset:, :row_offset] = raster_parts[2]
        raster[:, col_offset:, row_offset:] = raster_parts[3]
    elif raster_parts[0].shape[1] < 256:
        # Tile is split over 2 image files: bottom and top
        col_offset = raster_parts[0].shape[1]
        raster[:, :col_offset, :] = raster_parts[0]
        raster[:, col_offset:, :] = raster_parts[1]
    else:
        # Tile is split over 2 image files: left and right
        row_offset = raster_parts[0].shape[2]
        raster[:, :, :row_offset] = raster_parts[0]
        raster[:, :, row_offset:] = raster_parts[1]

    return raster


def get_rgb_data(engine, tile_id):
    """Get input (rgb) data for one tile, potentially from
    multiple files

    :param engine: engine for the database
    :type engine: sqlalchemy.engine.Engine
    :param tile_id: tile ID
    :type tile_id: int
    ...
    :return: rgb data for tile
    :rtype: numpy.array
    :return: Affine transformation object of rgb data
    :rtype: affine.Affine
    """
    tile = ut.get_tile_geometry(engine, tile_id)
    raster_parts, transform = get_raster_parts(engine, tile_id, tile)
    raster = concat_raster_parts(raster_parts)

    return raster, transform


def remove_overlap(gs):
    """Return surface area per ground truth label.

    :param gs: labelled geometries
    :type gs: geopandas.GeoSeries
    ...
    :return: labelled geometries without overlap
    :rtype: geopandas.GeoSeries
    """
    # Create copy
    gs_copy = gs.copy()
    # Remove overlap
    gs.loc[0] = gs_copy.loc[0].difference(gs_copy.loc[1])
    gs.loc[1] = gs_copy.loc[1].difference(gs_copy.loc[0])

    return gs


def get_ground_truth_data(engine, tile_id, transform):
    """Get ground truth (imperviousness)
    data for one tile

    :param engine: engine for the database
    :type engine: sqlalchemy.engine.Engine
    :param tile_id: tile ID
    :type tile_id: int
    :param transform: Affine transformation object of rgb data
    :type transform: affine.Affine
    ...
    :return: ground truth data for tile
    :rtype: numpy.array
    """
    # Get ground truth label ids
    label_ids = list(PARAMS['postgres']['label_id_to_name'].keys())
    # Get elements to derive ground truth from
    elements = gpd.read_postgis(
        f"""
        SELECT fTGT.intersect_id, dGT.label, dGT.geom
        FROM factTilesGroundTruth fTGT
        JOIN dimGroundTruth dGT ON fTGT.element_id = dGT.element_id
        WHERE fTGT.tile_id = {tile_id}
        """,
        engine,
        geom_col='geom',
        index_col='intersect_id')
    # Dissolve to one geometry object per ground truth label
    elements = elements.dissolve(by='label')['geom']\
        .reindex(label_ids[:2],
                 fill_value=shl.geometry.Polygon())
    # Remove overlap to enable unique labelling
    elements = remove_overlap(elements)
    # Rasterize geometries to extract ground truth value per pixel
    geom_label_pairs = list(zip(elements.values, elements.index))
    geom_label_pairs = [i for i in geom_label_pairs if not i[0].is_empty]
    height = PARAMS['postgres']['tile_height_in_pixels']
    width = PARAMS['postgres']['tile_width_in_pixels']
    if len(geom_label_pairs) > 0:
        gt = rs.features.rasterize(geom_label_pairs,
                                   out_shape=(height, width),
                                   transform=transform,
                                   fill=label_ids[2],
                                   dtype=np.uint8)
    else:
        gt = np.full(shape=(height, width),
                     fill_value=label_ids[2],
                     dtype=np.uint8)

    return gt


def get_dataset_properties(tile_count):
    """Return properties of datasets to created in hdf5

    :param tile_count: number of tiles in the dataset
    :type tile_count: int
    ...
    :return: properties of datasets to created
    :rtype: dict
    """
    # Initialize properties based on configuration parameters
    dset_dict = PARAMS['hdf5']['dset_dict']
    # Update tile count in each dataset
    for dset_name in dset_dict.keys():
        # tile_count + 1 since tile ids are 1 based (i.e. tile_id=0 is missing)
        dset_dict[dset_name]['shape'][0] = tile_count + 1

    return dset_dict


@timeit
def write_tiles_to_disk(engine):
    """Get input (rgb) and ground truth (imperviousness)
    data per tile and write to file

    :param engine: engine for the database
    :type engine: sqlalchemy.engine.Engine
    """
    # Get tile count to set dimentions of datasets
    tile_count = ut.get_tile_count(engine)
    # Get properties of datasets to be created
    dset_dict = get_dataset_properties(tile_count)
    # Create file, truncate if exists
    f = h5py.File(PARAMS['hdf5']['path'], 'w')
    # Create datasets
    dset_rgb = f.create_dataset('rgb', **dset_dict['rgb'])
    dset_gt = f.create_dataset('gt', **dset_dict['gt'])
    _ = f.create_dataset('pred', **dset_dict['pred'])
    # List all tile_ids for tqdm purposes (note: one-based counting)
    tile_ids = [i for i in range(1, tile_count + 1)]
    for tile_id in tqdm(tile_ids):
        rgb, transform = get_rgb_data(engine, tile_id)
        gt = get_ground_truth_data(engine, tile_id, transform)
        dset_rgb[tile_id, :, :, :] = rgb
        dset_gt[tile_id, :, :, :] = gt
    f.close()


def get_tile_characteristics(tile_id, f):
    """Return various characteristics of tile

    :param tile_id: tile ID
    :type tile_id: int
    :param f: file object
    :type f: h5py.File
    ...
    :return: characteristics of tile
    :rtype: tuple
    """
    # Get rgb and ground truth values
    rgb = f['rgb'][tile_id,]
    gt = f['gt'][tile_id,]
    # Get ground truth label count
    count_0 = np.count_nonzero(gt == 0)
    count_1 = np.count_nonzero(gt == 1)
    count_2 = np.count_nonzero(gt == 2)
    # Rearrange axis for transformation to grayscale
    rgb = np.rollaxis(rgb, 0, 3)
    # Transform to grayscale (to enable feature extraction steps)
    gray = rgb2gray(rgb)
    gray_mean = np.ones(gray.shape) * np.mean(gray)
    # Feature extraction
    entropy = shannon_entropy(gray)
    gray_mse = mean_squared_error(gray, gray_mean)
    if gray_mse == 0.0:
        psnr = 0.0
    else:
        psnr = 10 * log10(1 / gray_mse)
    conv_hor = convolve2d(gray, np.array([[-1, 1], [0, 0]]), mode='valid')
    conv_ver = convolve2d(gray, np.array([[-1, 0], [1, 0]]), mode='valid')
    avg_diff = np.mean(np.abs(conv_hor) + np.abs(conv_ver))
    avg_edge = np.mean(sobel(gray))
    avg_grad = np.mean(np.sqrt((conv_hor ** 2 + conv_ver ** 2) / 2))
    i_max = np.max(gray)
    i_min = np.min(gray)
    if i_max + i_min == 0.0:
        contrast = 0.0
    else:
        contrast = (i_max - i_min) / (i_max + i_min)

    return (count_0, count_1, count_2, entropy, psnr, avg_diff, avg_edge,
            avg_grad, contrast, tile_id)


@timeit
def load_tile_characteristics(cur, conn):
    """Get tile characteristics and load to database for
    exploratory data analysis purposes

    :param cur: cursor to the database
    :type cur: psycopg.extensions.cursor
    :param conn: connection to the database
    :type conn: psycopg.extensions.connection
    """
    # Get file object for tiles
    f = h5py.File(PARAMS['hdf5']['path'], 'r')
    # Get all tile IDs
    tile_ids = [i for i in range(1, f['rgb'].shape[0])]
    # Iterate batches of tiles IDs
    for batch in tqdm(ut.batch(tile_ids, batch_size=100)):
        # Get characteristics
        chcs = [get_tile_characteristics(tile_id, f) for tile_id in batch]
        # Load characteristics to database
        cur.executemany(PARAMS['postgres']['insert_characteristics_dimTiles'],
                        chcs)
        conn.commit()
    f.close()


@timeit
def write_downsampled_overwiew_to_disk(engine):
    """Write an overview of the rgb data to a single file for
    exploratory data analysis purposes (downlsample to decrease
    load time and memory required)

    :param engine: engine for the database
    :type engine: sqlalchemy.engine.Engine
    """
    # Get all filenames that overlap with the tiles
    df = pd.read_sql(
        """
        SELECT DISTINCT fTF.file_id, dIF.file_name
        FROM factTilesFiles fTF
        JOIN dimImageFiles dIF ON dIF.file_id = fTF.file_id
        """, engine)
    # Get path of image files
    path = pathlib.Path(PARAMS['postgres']['dir_imagery'])
    # Initiate list of datasets to be merged
    datasets = list()
    # Iterate over all files
    for file_name in df.file_name:
        # Read image file as dataset object
        dataset = rs.open(path / file_name)
        # Downsample
        data, transform = ut.downsample_rgb_data(dataset,
                                                 PARAMS['downsample_for_plot'])
        # Update heigt, width and resolution
        output_meta = dataset.meta.copy()
        output_meta.update({'driver': 'GTiff',
                            'height': data.shape[1],
                            'width': data.shape[2],
                            'transform': transform})
        # Store data as memory file to enable merge
        dataset = MemoryFile().open(**output_meta)
        dataset.write(data)
        # Append dataset to list
        datasets.append(dataset)
    # Perform the merge of all datasets (i.e. one for each image file)
    mosaic, output = merge(datasets)
    # Update heigt, width and resolution
    output_meta = datasets[0].meta.copy()
    output_meta.update({'driver': 'GTiff',
                        'height': mosaic.shape[1],
                        'width': mosaic.shape[2],
                        'transform': output})
    # Write to one single file
    output_path = pathlib.Path(PARAMS['path_downsampled_overview'])
    with rs.open(output_path, 'w', **output_meta) as m:
        m.write(mosaic)


@timeit
def load_data_split(cur, conn):
    """Randomly load a data split label to tabel, which indicates
    whether the tile belows to the train, validation or test set
    should be exclused from any of these sets

    :param cur: cursor to the database
    :type cur: psycopg.extensions.cursor
    :param conn: connection to the database
    :type conn: psycopg.extensions.connection
    """
    # Randomly load a data split label to tabel
    cur.execute(PARAMS['postgres']['split_tiles'])
    conn.commit()
    # Exclused tiles that should not be used at all
    if PARAMS['postgres']['exclude_tiles']:
        cur.execute(PARAMS['postgres']['exclude_tiles'])
        conn.commit()


def main():
    """
    """
    set_config_params()
    cur, conn = create_postgis_database()
    drop_and_create_tables(cur, conn)
    insert_data_in_tables(cur, conn)
    conn.close()
    conn, cur = ut.get_connection_and_cursor(PARAMS['postgres']['db_name'])
    engine = ut.get_engine(PARAMS['postgres']['db_name'])
    write_tiles_to_disk(engine)
    load_tile_characteristics(cur, conn)
    load_data_split(cur, conn)
    write_downsampled_overwiew_to_disk(engine)
    

if __name__ == "__main__":
    main()
