import psycopg
import sqlalchemy

import numpy as np
import pandas as pd
import geopandas as gpd

from rasterio.enums import Resampling

from config import config_utils

# Set configuration parameters as global
PARAMS = config_utils.copy()


def get_connection_and_cursor(db_name):
    """Connects a postgis database and returns
    the connection and cursor to the database

    :param db_name: name of database
    :type db_name: str
    ...
    :return: connection and cursor to the database
    :rtype: psycopg.extensions.cursor, psycopg.extensions.connection
    """
    # connect to database
    conn = psycopg.connect(f"""
    host={PARAMS['postgres']['host']}
    dbname={db_name}
    user={PARAMS['postgres']['user']}
    password={PARAMS['postgres']['password']}
    """)
    cur = conn.cursor()

    return conn, cur


def get_engine(db_name):
    """Connects a postgis database and returns
    an engine engine for the database

    :param db_name: name of database
    :type db_name: str
    ...
    :return: engine for the database
    :rtype: sqlalchemy.engine.Engine
    """
    # create url
    url_object = sqlalchemy.engine.URL.create(
        drivername='postgresql+psycopg2',
        username=PARAMS['postgres']['user'],
        password=PARAMS['postgres']['password'],
        host=PARAMS['postgres']['host'],
        database=db_name
        )
    # Get engine object
    engine = sqlalchemy.create_engine(url_object)

    return engine


def get_tile_geometry(engine, tile_id):
    """Returns geometry object that corresponds to
    the given tile ID

    :param engine: engine for the database
    :type engine: sqlalchemy.engine.Engine
    :param tile_id: tile ID
    :type tile_id: int
    ...
    :return: tile geometry
    :rtype: shapely.geometry
    """
    tile = gpd.read_postgis(
        f"""
        SELECT tile_id, bbox_geom FROM dimTiles
        WHERE tile_id = {tile_id}
        """,
        engine,
        geom_col='bbox_geom',
        index_col='tile_id').iloc[0, 0]

    return tile


def correct_raster_error_if_exists(raster):
    """Remove all-zero rows or columns on the outside
    of the raster if dimensions are not correct

    :param raster: 3D array representing an rgb raster
    :type raster: numpy.array
    ...
    :return: 3D array representing an rgb raster
    :rtype: numpy.array
    """
    # Only drop if dimension is not 256
    if not raster.shape[2] == 256:
        # Get all-zero row/col
        all_zero = np.all(raster == 0, axis=(0, 1))
        # Only drop if all-zero row/col is on the outside
        if all_zero[0] or all_zero[-1]:
            # Set rows/cols that are not on the outside to False
            # to make sure they are never dropped
            all_zero[1:-1] = False
            raster = np.delete(raster, all_zero, axis=2)
    # Only drop if dimension is greater than 256
    if not raster.shape[1] == 256:
        # Get all-zero row/col
        all_zero = np.all(raster == 0, axis=(0, 2))
        # Only drop if all-zero row/col is on the outside
        if all_zero[0] or all_zero[-1]:
            # Set rows/cols that are not on the outside to False
            # to make sure they are never dropped
            all_zero[1:-1] = False
            raster = np.delete(raster, all_zero, axis=1)

    return raster


def get_tile_count(engine):
    """Return number of tiles in the dimTiles table

    :param engine: engine for the database
    :type engine: sqlalchemy.engine.Engine
    ...
    :return: number of tiles
    :rtype: int
    """
    tile_count = pd.read_sql(
        """
        SELECT COUNT(tile_id) FROM dimTiles
        """,
        engine).iloc[0, 0]

    return tile_count


def batch(to_batch, batch_size=1):
    """Batch a list given the batch size

    :param to_batch: list to be bathed
    :type to_batch: list
    :param batch_size: batch size
    :type batch_size: int
    ...
    :return: list of batches
    :rtype: list
    """
    length = len(to_batch)
    batch_ids = range(0, length, batch_size)
    batched_list = [to_batch[batch_id:min(batch_id + batch_size, length)] for
                    batch_id in batch_ids]

    return batched_list


def downsample_rgb_data(dataset, downscale_factor):
    """Downsample a dataset with rgb data with a
    downscaling factor

    :param dataset: object with rgb data
    :type dataset: rasterio.DatasetReader
    :param downscale_factor: downscaling factor
    :type downscale_factor: int
    ...
    :return: downsampled dataset
    :rtype: numpy.array
    :return: Affine transformation object of downsampled dataset
    :rtype: affine.Affine
    """
    # resample data to target shape
    data = dataset.read(
        out_shape=(
            dataset.count,
            int(dataset.height / downscale_factor),
            int(dataset.width / downscale_factor)
        ),
        resampling=Resampling.bilinear
    )
    # scale image transform
    transform = dataset.transform * dataset.transform.scale(
        (dataset.width / data.shape[-1]),
        (dataset.height / data.shape[-2])
    )

    return data, transform
