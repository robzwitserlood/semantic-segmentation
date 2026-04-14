import json
import pathlib

import numpy as np
import xarray as xr
import geopandas as gpd
import xml.etree.ElementTree as et

from pyproj import CRS
from itertools import product
from shapely.geometry import box, mapping
from owslib.wfs import WebFeatureService


def build_aerial_mosaic(storage_dir, file_format):
    """Build an in-memory mosaic DataArray from aerial GeoTIFF tiles.

    :param storage_dir: directory containing the tiff files
    :type storage_dir: str
    :param file_format: glob pattern matching the tiff files
    :type file_format: str
    :return: merged DataArray covering all tiles
    :rtype: xarray.DataArray
    """
    import rioxarray as rxr
    from rioxarray.merge import merge_arrays

    dir_path = pathlib.Path.cwd() / storage_dir
    paths = sorted(dir_path.glob(file_format))
    if not paths:
        raise FileNotFoundError(f"No files matching {file_format} in {dir_path}")
    return merge_arrays([rxr.open_rasterio(p) for p in paths])


class NlRegionToGeom:
    """Class to load geometries of regions in the Netherlands
    via a web feature service.

    :param storage_dir: directory of gml file
    :type storage_dir: str
    :param file_name: name of gml file
    :type file_name: str
    :param wfs_url: url of web feature service
    :type wfs_url: str
    :param layer_name: name of layer that holds regions
    :type layer_name: str
    """

    def __init__(self, storage_dir, file_name, wfs_url, layer_name):
        """Constructor method
        """
        self.path = pathlib.Path.cwd() / storage_dir / file_name
        self.wfs_url = wfs_url
        self.layer_name = layer_name
        if not self.data_already_stored():
            self.store_data()
        self.crs = self.get_crs()

    def data_already_stored(self):
        """Check if required gml data is already stored.

        :return: indicator value for stored or not stored
        :rtype: bool
        """
        # Perform check
        if self.path.exists():
            return True
        else:
            return False

    def store_data(self):
        """Store gml data.
        """
        # Initiate Web Feature Service instance
        wfs = WebFeatureService(self.wfs_url)
        # Get the desired layer
        response = wfs.getfeature(typename=[self.layer_name])
        # Make directory
        self.path.parent.mkdir(parents=True, exist_ok=True)
        # Write to file
        with open(self.path, 'wb') as f:
            f.write(response.read())

    def get_crs(self):
        """Get coordinate reference system (CSR) of the geometries.

        :return: coordinate reference system object
        :rtype: pyproj.crs.crs.CRS
        """
        # Parse GML to get crs (with et since gpd fails to do so)
        for child in et.parse(self.path).getroot().iter():
            if child.get('srsName'):
                crs_string = child.get('srsName')
                break
        # Get geometry of spatial scope
        crs = CRS.from_string(crs_string)

        return crs

    def from_region_name(self, region_name):
        """Get geometry of the spatial scope.

        :return: geometry object
        :rtype: shapely.geometry.polygon.Polygon
        """
        # Read data
        gdf = gpd.read_file(self.path, driver='GML')
        # Get geometry of spatial scope
        geometry = gdf.loc[gdf['naam'] == region_name]['geometry'].values[0]

        return geometry


class TiffBasedTiledBbox:
    """Class to create tiled bbox based on the grid in a tiff file.

    :param geometry: geometry to be bboxed and tiled
    :type geometry: shapely.geometry.polygon.Polygon
    :param crs: coordinate reference system object
    :type crs: pyproj.crs.crs.CRS
    :param storage_dir: directory of tiff file on which bbox is based
    :type storage_dir: str
    :param file_name: name of tiff file on which bbox is based
    :type file_name: str
    :param image_size: image (aka tile) size
    :type image_size: int
    """

    def __init__(self, geometry, crs, storage_dir, file_name, image_size, data_array=None):
        """Constructor method
        """
        self.geometry = geometry
        self.crs = crs
        self.data_array = data_array if data_array is not None else xr.open_dataarray(pathlib.Path.cwd() / storage_dir / file_name)
        self.image_size = image_size
        self.bbox = self.get_bbox()
        self.row_col_list = self.get_row_col_list()
    
    def get_bbox(self):
        """Get bottom-left and top-right coordinates of tiled bbox.

        :return: bottom-left and top-right coordinates
        :rtype: list
        """
        # Clip raster with bounds of geometry
        xdsc = self.data_array.rio.clip_box(*self.geometry.bounds)
        # Calc required elongation (in pixels) to fit integer number of tiles
        elong_x = self.image_size - (len(xdsc.x) % self.image_size)
        elong_y = self.image_size - (len(xdsc.y) % self.image_size)
        elong_left = 0
        elong_right = elong_x
        elong_top = int(np.ceil(elong_y / 2))
        elong_bottom = int(np.floor(elong_y / 2))
        # Calc pixel size in meters (assume square pixels)
        pixel_size = xdsc.x.values[1] - xdsc.x.values[0]
        # Calc bounds of bbox
        x_min = xdsc.x.values[0] - elong_left * pixel_size
        y_min = xdsc.y.values[-1] - elong_bottom * pixel_size
        x_max = xdsc.x.values[-1] + elong_right * pixel_size
        y_max = xdsc.y.values[0] + elong_top * pixel_size
        
        return [x_min, y_min, x_max, y_max]

    def get_row_col_list(self):
        """Get rows range (y) and column range (x) of tile matrix set.

        :return: List of row-column combinations of tiles
        :rtype: list
        """
        # Clip raster with bounds of geometry
        xdsc = self.data_array.rio.clip_box(*self.bbox)
        # Calc range of rows and colums (bottom-left is 0, 0)
        row_range = range(0, len(xdsc.y) // self.image_size)
        col_range = range(0, len(xdsc.x) // self.image_size)
        # Create list of (column, row) tuples
        row_col_list = list(product(row_range, col_range))

        return row_col_list

    def id_to_bbox(self, tile_id):
        """Return tile object that corresponds to tile id

        :return: bottom-left and top-right coordinates
        :rtype: list
        """
        # Get number of tiles to the right and top (bottom-left is 0, 0)
        to_the_top, to_the_right = self.row_col_list[tile_id]
        # Calc pixel and tile size in meters (assume square pixels and tiles)
        pixel_size = self.data_array.x.values[1] - self.data_array.x.values[0]
        tile_size = pixel_size * self.image_size
        # Calculate tile bbox
        xmin = self.bbox[0] + to_the_right * tile_size
        xmax = xmin + pixel_size * (self.image_size - 1)
        ymin = self.bbox[1] + to_the_top * tile_size
        ymax = ymin + pixel_size * (self.image_size - 1)

        return [xmin, ymin, xmax, ymax]

    def explore(self):
        """Visualize tiled bbox on top of base map.

        :return: interactive map
        :rtype: folium.folium.Map
        """
        return gpd.GeoDataFrame(data={'label': ['tiled bbox', 'envelope', 'region'],
                                      'geometry': [box(*self.bbox).boundary,
                                                   self.geometry.envelope.boundary,
                                                   self.geometry.boundary]}, crs=self.crs).explore(tiles='CartoDB positron', column='label')

    def iter_bbox(self):
        """Iterate over all tiles as tiles and yield the bbox of the tile.

        :return: bottom-left and top-right coordinates of all tiles
        :rtype: generator function
        """
        for i in range(len(self.row_col_list)):
            yield self.id_to_bbox(i)

    def get_wkt_bbox(self):
        """Return wkt representation of bbox.

        :return: wkt representation of bbox
        :rtype: str
        """
        return box(*self.bbox).wkt

    def get_wkt_bbox_with_buffer(self):
        """Return wkt representation of bbox.

        :return: wkt representation of bbox
        :rtype: str
        """
        return box(*self.bbox).buffer(1).wkt

    def write_geojson_of_bbox(self, file_dir, file_name):
        """Write bbox to GeoJSON file
        """
        path = pathlib.Path.cwd() / file_dir / file_name
        # Open file
        with open(path, 'r') as fp:
            geojson_format = json.load(fp)
        geojson_bbox = mapping(box(*self.bbox))
        # Replace coordinates in format
        geojson_format['features'][0]['geometry']['coordinates'] = [[[i[0], i[1]] for i in geojson_bbox['coordinates'][0]]]
        # Write to file
        with open(path, 'w') as fp:
            json.dump(geojson_format, fp)
