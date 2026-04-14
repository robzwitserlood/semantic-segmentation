import json
import pathlib
import matplotlib
matplotlib.rcParams.update({'font.size': 16})
import multiprocessing

import numpy as np
import xarray as xr
import rioxarray as rxr
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt

from tqdm import tqdm
from math import log10
from shapely import geometry
from rasterio.plot import plotting_extent
from skimage.filters import sobel
from skimage.color import rgb2gray
from scipy.signal import convolve2d
from joblib import Parallel, delayed
from geocube.api.core import make_geocube
from skimage.measure import shannon_entropy
from skimage.metrics import mean_squared_error


def rasterize_accordingly(gdfc, tile_ds, col):
    """Rasterize geometries according to a given raster.
        
    :param gdfc: labelled geometries to be rasterized
    :type gdfc: geopandas.GeoDataFrame
    :param tile_ds: raster according to which should be rasterized 
    :type tile_ds: xarray.DataArray
    :param col: name of column in gdfc to be used as measurement
    :type col: str
    ...
    :return: Rasterized geometries
    :rtype: xarray.DataArray
    """
    # Rasterize GeoDataFrame with ground truth data
    try:
        out_grid = make_geocube(
            vector_data=gdfc,
            like=tile_ds,
            measurements=[col],
            fill = -1
        )
        out_grid = out_grid[col]
    except:
        out_grid = xr.zeros_like(tile_ds.red)
    
    return out_grid

class DataSetCreator:
    """Class to create data set of tiles.

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

    def __init__(self, tbbox, gdf, in_storage_dir, in_file_name, out_storage_dir, out_file_name, featuretypes):
        """Constructor method
        """
        self.tbbox = tbbox
        self.gdf = gdf
        self.data_array = rxr.open_rasterio(pathlib.Path.cwd() / in_storage_dir / in_file_name)
        self.resolution = self.get_resolution()
        self.area_of_tile = self.get_area_of_tile()
        self.out_storage_dir = pathlib.Path.cwd() / out_storage_dir
        self.out_file_name = out_file_name
        self.featuretypes = featuretypes
        
    def get_resolution(self):
        """Get resolution of tiff file
        """
        size_meters = self.data_array.x.values[1] - self.data_array.x.values[0]
        
        return size_meters, size_meters
    
    def get_area_of_tile(self):
        """Get resolution of tiff file
        """
        # Get bounds of example tile
        xmin, ymin, xmax, ymax = self.tbbox.id_to_bbox(0)
        # Calc area (width x height)
        area_of_tile = (xmax - xmin) * (ymax - ymin)
        
        return area_of_tile
    
    def remove_overlap(self, gdf_tile):
        """Return surface area per ground truth label.
        
        :param gdf_tile: labelled geometries
        :type gdf_tile: geopandas.GeoDataFrame
        ...
        :return: labelled geometries without overlap
        :rtype: geopandas.GeoDataFrame
        """
        # Groupby label to find overlap
        geoms_per_label = gdf_tile.dissolve(by='label')[['geometry']]
        geoms_per_label = geoms_per_label.reindex(['impervious', 'pervious'], fill_value=geometry.Polygon())
        geoms_per_label_copy = geoms_per_label.copy()
        # Remove overlap
        geoms_per_label.loc['impervious', 'geometry'] = geoms_per_label_copy.loc['impervious', 'geometry'].difference(
            geoms_per_label_copy.loc['pervious', 'geometry'])
        geoms_per_label.loc['pervious', 'geometry'] = geoms_per_label_copy.loc['pervious', 'geometry'].difference(
            geoms_per_label_copy.loc['impervious', 'geometry'])
        
        return geoms_per_label.reset_index()
    
    def get_area_per_featuretype(self, gdf_tile):
        """Return surface area per feature type.
        
        :param gdf_tile: labelled geometries
        :type gdf_tile: pandas.DataFrame
        ...
        :return: surface area per feature type
        :rtype: pandas.DataFrame
        """
        # Groupby featuretype
        try: 
            area_per_featuretype = gdf_tile.dissolve(by='featuretype')[['geometry']].area
        except:
            area_per_featuretype = pd.DataFrame(data=[0]*len(self.featuretypes), columns=['area'], index=self.featuretypes).area
        # Fill missing features with value 0
        area_per_featuretype = area_per_featuretype.reindex(self.featuretypes, fill_value=0)
        
        return area_per_featuretype
    
    def create_tile_dataset(self, gdfc, xdsc):
        """Create dataset with sattelite and ground truth data of one tile.
        
        :param gdfc: labelled geometries
        :type gdfc: pandas.DataFrame
        :param xdsc: satellite data
        :type xdsc: xarray.DataArray
        ...
        :return: sattelite and ground truth data of one tile
        :rtype: xarray.DataSet
        """
        # Create dataset with satelitte band as start point
        tile_ds = xdsc.to_dataset(dim='band').rename({1: 'red', 2: 'green', 3: 'blue'})
        # Add columns to GeoDataFrame for rasterization purposes
        gdfc['impervious'] = gdfc['label'].apply(lambda x: 1 if x == 'impervious' else 0)
        gdfc['pervious'] = gdfc['label'].apply(lambda x: 1 if x == 'pervious' else 0)
        # Rasterize GeoDataFrame with ground truth data
        out_grid_vv = rasterize_accordingly(gdfc, tile_ds, 'impervious')
        out_grid_vo = rasterize_accordingly(gdfc, tile_ds, 'pervious')
        # Remove overlap between the two labels (impervious, pervious)
        tile_ds['impervious'] = xr.where((out_grid_vv > 0.5) & (out_grid_vo < 0.5), 1, 0)
        tile_ds['pervious'] = xr.where((out_grid_vo > 0.5) & (out_grid_vv < 0.5), 1, 0)
        # Explicitly label unknown pixels as unknown (unknown)
        tile_ds['unknown'] = xr.where(tile_ds.impervious + tile_ds.pervious == 0, 1, 0)
        
        return tile_ds
    
    def get_input_features(self, ds_tile):
        """Return various features of the rgb input tile
        
        :param ds_tile: sattelite and ground truth data of one tile
        :type ds_tile: xarray.DataSet
        ...
        :return: mean value of red, green and blue
        :rtype: pandas.DataFrame
        """
        # Extract numpy array with rgb values like (height, width, band)
        color = ds_tile[['red', 'green', 'blue']].to_array().to_numpy()
        color = np.rollaxis(color, 0, 3)
        # Transform to grayscale (required for some of the feature extraction steps)
        gray = rgb2gray(color)
        gray_mean = np.ones(gray.shape) * np.mean(gray)
        # Feature extraction
        entropy = shannon_entropy(gray)
        psnr = 10 * log10(1 / mean_squared_error(gray, gray_mean))
        conv_hor = convolve2d(gray, np.array([[-1, 1], [0, 0]]), mode='valid')
        conv_ver = convolve2d(gray, np.array([[-1, 0], [1, 0]]), mode='valid')
        avg_diff = np.mean(np.abs(conv_hor) + np.abs(conv_ver)) 
        avg_edge = np.mean(sobel(gray))
        avg_grad = np.mean(np.sqrt((conv_hor ** 2 + conv_ver ** 2) / 2))
        i_max = np.max(gray)
        i_min = np.min(gray)
        contrast = (i_max - i_min) / (i_max + i_min)
        # Write to series
        df = pd.Series(data=[entropy, psnr, avg_diff, avg_edge, avg_grad, contrast],
                       index=['entropy', 'psnr', 'avg_diff', 'avg_edge', 'avg_grad', 'contrast'])
        
        return df
    
    def get_pixel_count_per_label(self, ds_tile):
        """Return pixel count per ground truth label in tile
        
        :param ds_tile: sattelite and ground truth data of one tile
        :type ds_tile: xarray.DataSet
        ...
        :return: pixel count per label
        :rtype: pandas.DataFrame
        """
        df = ds_tile[['impervious', 'pervious', 'unknown']].to_dataframe().reset_index()
        df = df[['impervious', 'pervious', 'unknown']].apply(np.sum, axis=0)
                                  
        return df                 

    def clip_geometries(self, tile_bbox):
        """Return geodataframes, one without and one with buffer
        
        :param tile_bbox: bounding box of tile
        :type tile_bbox: list
        ...
        :return: aggregated data for exploratory data analysis
        :rtype: geopandas.GeoDataFrame
        """
        # Calc bounds of tile with buffer
        tile_bbox_buffer = geometry.box(*tile_bbox).buffer(1).bounds
        # Clip geometries
        gdf_tile = self.gdf[['featuretype', 'label', 'geometry']].clip(tile_bbox)
        gdf_tile_buffer = self.gdf[['featuretype', 'label', 'geometry']].clip(tile_bbox_buffer)
        
        return gdf_tile, gdf_tile_buffer
    
    def ready_to_start(self, overwrite):
        """Perform all checks if dataset creator is ready to start.

        :param overwrite: indicator if stored bgt data should be overwritten
        :type overwrite: bool
        ...
        :return: indicator value
        :rtype: bool
        """
        path = self.out_storage_dir / self.out_file_name
        if path.exists() and not overwrite:
            return False
        else:
            return True
     
    def create(self, overwrite=False):
        """Create dataset
        
        :param overwrite: indicator if existing dataset is overwritten
        :type overwrite: bool
        ...
        :return: aggregated data for exploratory data analysis
        :rtype: pandas.DataFrame
        """
        if self.ready_to_start(overwrite):
            # Initiate list for aggregated data
            agg_data = list()
            # Loop through all tiles
            for tile_id, tile_bbox in tqdm(enumerate(self.tbbox.iter_bbox()), total=len(self.tbbox.row_col_list)):
                # Extract ground truth data from Geodataframe
                gdf_tile, gdf_tile_buffer = self.clip_geometries(tile_bbox)
                # Extract satellite data from tiff
                xdsc = self.data_array.rio.clip_box(*tile_bbox)
                # Create xarray dataset for one tile
                ds_tile = self.create_tile_dataset(gdf_tile_buffer, xdsc)
                # Get data for exploratory data analysis
                pixel_count_per_label = self.get_pixel_count_per_label(ds_tile)
                area_per_featuretype = self.get_area_per_featuretype(gdf_tile)
                input_features = self.get_input_features(ds_tile)
                # Concat and add to eda data
                bgt_agg_data = pd.concat([pixel_count_per_label, area_per_featuretype, input_features])
                agg_data.append(list(bgt_agg_data.values))
                # Write xarray dataset to file
                file_name = f'tile_{str(tile_id).zfill(5)}.nc'
                tile_path = self.out_storage_dir / file_name
                ds_tile.attrs.clear()
                ds_tile.astype(int, casting='safe').to_netcdf(tile_path,
                                                                encoding={'red': {'dtype': 'int16'},
                                                                        'green': {'dtype': 'int16'},
                                                                        'blue': {'dtype': 'int16'},
                                                                        'impervious': {'dtype': 'int8'},
                                                                        'pervious': {'dtype': 'int8'},
                                                                        'unknown': {'dtype': 'int8'}})
                ds_tile.close()
            # Store aggregated data in DataFrame
            df = pd.DataFrame(agg_data, columns=bgt_agg_data.index) 
            # Write DataFrame to csv
            path = self.out_storage_dir / self.out_file_name
            df.to_parquet(path)

            return df

    def preview_tile(self, tile_id):
        """Plot rgb image of tile and labelling of pixels
        
        :param tile_id: id of tile
        :type tile_id: int
        """
        file_name = f'tile_{str(tile_id).zfill(5)}.nc'
        tile_path = self.out_storage_dir / file_name
        tile_ds = xr.open_dataset(tile_path)
        tile_ds.close()

        fig, ax = plt.subplots(2, 2, figsize=(15, 15))

        rgb = np.moveaxis(tile_ds[['red', 'green', 'blue']].to_array().values, 0, -1)
        ax[0, 0].imshow(rgb)
        ax[0, 1].imshow(rgb)
        ax[1, 0].imshow(rgb)
        ax[1, 1].imshow(rgb)

        ax[0, 1].imshow(tile_ds['unknown'].values, cmap='Blues', alpha=0.5)
        ax[1, 0].imshow(tile_ds['pervious'].values, cmap='Blues', alpha=0.5)
        ax[1, 1].imshow(tile_ds['impervious'].values, cmap='Blues', alpha=0.5)

        ax[0, 0].set_title('satelliet_beeld')
        ax[0, 1].set_title('unknown')
        ax[1, 0].set_title('pervious')
        ax[1, 1].set_title('impervious')

        #ax[0, 0].set_figsize(6, 6)

        plt.show()


class ExploratoryDataAnalysis:
    """Visualize data for exporatory data analysis.
    
    :param storage_dir: directory of metadata of tiles
    :type storage_dir: str
    :param file_name: name of file with metadata of tiles
    :type file_name: str
    :param tbbox: tiled bbox of spatial scope
    :type tbbox: ttbox
    """
    
    def __init__(self, storage_dir, in_file_name, tbbox, sat_image, out_file_name):
        """Constructor method
        """
        self.gdf = self.load_dataframe(storage_dir, in_file_name, tbbox)
        self.ds = sat_image.rio.clip_box(*tbbox.bbox).to_dataset(dim='band').rename({1: 'red', 2: 'green', 3: 'blue'})
        self.plotting_extent = plotting_extent(self.ds['red'], self.ds.rio.transform())
        # Set a default mask to all is False
        self.mask = self.gdf.index < -1
        self.out_path = pathlib.Path.cwd() / storage_dir / out_file_name
        
    def load_dataframe(self, storage_dir, file_name, tbbox):
        """
        """
        # Load aggregated tile data from disk
        df = pd.read_parquet(pathlib.Path.cwd() / storage_dir / file_name)
        # Add row and column values for tiles for analysis purposes
        df[['row', 'column']] = tbbox.row_col_list
        # Create geometry for each tile for visualization purposes
        tile_geoms = [geometry.box(*tile_bbox) for tile_bbox in tbbox.iter_bbox()]
        # Create Geodataframe to facilitate analysis
        gdf = gpd.GeoDataFrame(data=df, geometry=tile_geoms, crs=tbbox.crs)

        return gdf

    def set_mask(self, contrast_thld=None, entropy_thld=None):
        """
        """
        # Mask upper left corner in which not input is available
        submask1 = self.gdf.contrast.isnull()
        # Create buffer to also mask tiles with partial input
        y0 = self.gdf[submask1].row.min() - 5
        x0 = self.gdf[submask1].column.max() + 2
        submask1 = self.gdf.row > y0 + (self.gdf.row.max() - y0) / x0 * self.gdf.column
        # Mask first column in which input is partially available
        submask2 = self.gdf.column == 0
        # Mask clouds by threshold on contrast 
        if contrast_thld:
            submask3 = self.gdf.contrast.fillna(0) < contrast_thld
        else:
            # Set all to False in no value provided
            submask3 = self.gdf.index < -1
        # Mask clouds by threshold on contrast 
        if entropy_thld:
            submask4 = self.gdf.entropy.fillna(0) < entropy_thld
        else:
            # Set all to False in no value provided
            submask4 = self.gdf.index < -1
        # Combine masks
        self.mask = submask1 | submask2 | submask3 | submask4

    def satellite_and_tile_aggregate_side_by_side(self, column_name, write_to_file=False, dpi_write=200):
        """
        """
        fig, ax = plt.subplots(1, 3, figsize=(15, 8), gridspec_kw={'width_ratios': [30, 30, 1]})
        ax[0].axis('off')
        ax[1].axis('off')

        ax[0].imshow(np.moveaxis(self.ds[['red', 'green', 'blue']].to_array().values, 0, -1))
        self.gdf.plot(column=column_name, ax=ax[1], legend=True, cax=ax[2])

        ax[1].tick_params(left=False, labelleft=False, bottom=False, labelbottom=False)
        ax[1].margins(0)

        ax[0].set_title('Satelliet beeld')
        ax[1].set_title(column_name + ' (tile aggregate)')

        # Manually adjust position of axis for colorbar since geopandas messes up
        bbox = ax[2].get_position()
        intervaly = ax[1].get_position().intervaly
        bbox.update_from_data_y(intervaly)
        ax[2].set_position(bbox)

        if write_to_file:
            path = pathlib.Path.cwd() / 'visuals' / f'sat_{column_name}_side_by_side.jpg'
            plt.savefig(path, pad_inches=0.1, bbox_inches='tight', dpi=dpi_write)
        else:
            plt.show()
        
    def masked_satellite_image(self, write_to_file=False, dpi_write=200):
        """
        """
        fig, ax = plt.subplots(1, 1, figsize=(15, 15))
        ax.axis('off')

        ax.imshow(np.moveaxis(self.ds[['red', 'green', 'blue']].to_array().values, 0, -1), extent=self.plotting_extent)
        self.gdf[self.mask].plot(ax=ax)

        ax.tick_params(left=False, labelleft=False, bottom=False, labelbottom=False)
        ax.margins(0)

        if write_to_file:
            path = pathlib.Path.cwd() / 'visuals' / 'masked_satellite_image.jpg'
            plt.savefig(path, pad_inches=0.1, bbox_inches='tight', dpi=dpi_write)
        else:
            plt.show()

    def four_mask_options(self, column_name='contrast', thld_values=[0.975, 0.980, 0.985, 0.990],
                          write_to_file=False, dpi_write=200):
        """
        """
        fig, axs = plt.subplots(4, 1, figsize=(10, 40))
        axs = axs.reshape(-1)
        [ax.axis('off') for ax in axs]

        masks = [self.gdf[column_name].fillna(0) < thld for thld in thld_values]

        for i, ax in enumerate(axs):
            ax.imshow(np.moveaxis(self.ds[['red', 'green', 'blue']].to_array().values, 0, -1), extent=self.plotting_extent)
            self.gdf[masks[i]].plot(ax=ax)

        [ax.tick_params(left=False, labelleft=False, bottom=False, labelbottom=False) for ax in axs]
        [ax.margins(0) for ax in axs]
        [ax.set_title(column_name + ' < ' + str(thld_values[i])) for i, ax in enumerate(axs)]

        if write_to_file:
            path = pathlib.Path.cwd() / 'visuals' / f'four_mask_options_{column_name}.jpg'
            plt.savefig(path, pad_inches=0.1, bbox_inches='tight', dpi=dpi_write)
        else:
            plt.show()
    
    def average_label_share(self, unmasked_only=True, write_to_file=False, dpi_write=100):
        """
        """
        fig, ax = plt.subplots(1, 1, figsize=(8, 8))
        if unmasked_only:
            df_pie = self.gdf.loc[~self.mask, ['impervious', 'pervious', 'unknown']].mean(axis=0).sort_values()
        else:
            df_pie = self.gdf[['impervious', 'pervious', 'unknown']].mean(axis=0).sort_values()
        df_pie.plot.pie(ax=ax,
                        autopct='%.1f%%',
                        startangle=90,
                        colors=['tab:blue','tab:gray','tab:green'])
        plt.ylabel("")
        ax.set_title('average label share [% of pixels]')
        if write_to_file:
            path = pathlib.Path.cwd() / 'visuals' / f'average_label_share_unmasked_only_{unmasked_only}.jpg'
            plt.savefig(path, pad_inches=0.1, bbox_inches='tight', dpi=dpi_write)
        else:
            plt.show()
        
    def pixel_count_bar(self, unmasked_only=True, write_to_file=False, dpi_write=100):
        """
        """
        if unmasked_only:
            df_bar = self.gdf.loc[~self.mask, ['impervious', 'pervious', 'unknown']].apply(lambda x: x / 1000)
        else:
            df_bar = self.gdf[['impervious', 'pervious', 'unknown']].apply(lambda x: x / 1000)
        
        df_bar1 = df_bar.sort_values(['impervious'])
        df_bar2 = df_bar.sort_values(['pervious'])
        df_bar3 = df_bar.sort_values(['unknown'])
        
        fig, ax = plt.subplots(1, 3, figsize=(15, 8))
        df_bar1.plot.bar(ax=ax[0],
                        stacked=True,
                        width=1,
                        color=['tab:gray','tab:green','tab:blue'])
        df_bar2.plot.bar(ax=ax[1],
                        stacked=True,
                        width=1,
                        color=['tab:gray','tab:green','tab:blue'])
        df_bar3.plot.bar(ax=ax[2],
                        stacked=True,
                        width=1,
                        color=['tab:gray','tab:green','tab:blue'])

        ax[0].set_title('sort by \n "impervious"')
        ax[1].set_title('sort by \n "pervious"')
        ax[2].set_title('sort by \n "unknown"')

        ax[0].tick_params(axis='x',
                          which='both',
                          bottom=False,
                          top=False,
                          labelbottom=False)
        ax[1].tick_params(axis='both',
                          which='both',
                          bottom=False,
                          top=False,
                          labelbottom=False,
                          left=False,
                          labelleft=False) 
        ax[2].tick_params(axis='both', 
                          which='both',
                          bottom=False,
                          top=False,
                          labelbottom=False,
                          left=False,
                          labelleft=False)

        ax[0].set_ylabel("pixel count [x1.000]")
        ax[0].set_xlabel('tiles [-]')
        ax[1].set_xlabel('tiles [-]')
        ax[2].set_xlabel('tiles [-]')

        ax[0].legend(fontsize=12)
        ax[1].legend(fontsize=12)
        ax[2].legend(fontsize=12)

        if write_to_file:
            path = pathlib.Path.cwd() / 'visuals' / f'pixel_count_bar_unmasked_only_{unmasked_only}.jpg'
            plt.savefig(path, pad_inches=0.1, bbox_inches='tight', dpi=dpi_write)
        else:
            plt.show()

    def entropy_histogram(self, unmasked_only=True, write_to_file=False, dpi_write=100):
        """
        """
        fig, ax = plt.subplots(1, 1, figsize=(8, 8))
        if unmasked_only:
            df_hist = self.gdf.loc[~self.mask, ['entropy']]
        else:
            df_hist = self.gdf['entropy']
            
        df_hist.entropy.plot.hist(bins=100, ax=ax)
        plt.xlabel("Value")
        ax.set_title('Entropy (tile aggregate)')
        if write_to_file:
            path = pathlib.Path.cwd() / 'visuals' / f'entropy_histogram_unmasked_only_{unmasked_only}.jpg'
            plt.savefig(path, pad_inches=0.1, bbox_inches='tight', dpi=dpi_write)
        else:
            plt.show()

    def export_unmasked_tiles(self):
        """
        """
        unmasked = ~self.mask.to_frame('used') 
        unmasked.to_parquet(self.out_path)    

class DataSetSplitter:
    """Class to split data set in train, validation and test set.

    :param fractions: approximate fractions of train, val and test set compared to total 
    :type fractions: list
    :param stratification_columns: names of columns to use for statification 
    :type stratification_columns: list
    :param bins: Number of bins for discretization of stratification columns 
    :type bins: int
    :param storage_dir: directory of files
    :type storage_dir: str
    :param agg_tile_data_file_name: name of files with aggregated tile data
    :type agg_tile_data_file_name: str
    :param usable_tiles_file_name: name of files with tiles to be used
    :type usable_tiles_file_name: str
    :param data_split_file_name: name of files with data split
    :type data_split_file_name: str
    """

    def __init__(self, fractions, stratification_columns, bins,
                 storage_dir, agg_tile_data_file_name, usable_tiles_file_name, data_split_file_name):
        """Constructor method
        """
        self.fractions = fractions
        self.strat_col_names = stratification_columns
        self.bins = bins
        self.quant_col_names = [col_name + '_quant' for col_name in self.strat_col_names]
        self.distributions = {quant_col_name: pd.DataFrame() for quant_col_name in self.quant_col_names}
        self.agg_tile_data_path = pathlib.Path.cwd() / storage_dir / agg_tile_data_file_name
        self.usable_tiles_path = pathlib.Path.cwd() / storage_dir / usable_tiles_file_name
        self.data_split_path = pathlib.Path.cwd() / storage_dir / data_split_file_name
        self.to_split = self.initiate_dataframe()
        self.seed = 0
        
    def initiate_dataframe(self):
        """
        """
        # Load dataframe with aggregated tile data for stratification purposes
        agg_tile_data_path = pd.read_parquet(self.agg_tile_data_path)
        # Load series with usable tiles for selection purposes
        usable_tiles = pd.read_parquet(self.usable_tiles_path).used
        # Select required rows and columns for dataframe
        df = agg_tile_data_path.loc[usable_tiles, self.strat_col_names].copy()
        # Create a quantile column for each stratification column
        for quant_col_name, strat_col_name in zip(self.quant_col_names, self.strat_col_names):
            df[quant_col_name] = pd.qcut(df[strat_col_name],
                                         q=self.bins,
                                         labels=False,
                                         duplicates='drop').astype('category')
            
        return df
    
    def set_seed(self, seed):
        """
        """
        # Set seed of Numpy
        np.random.seed(seed)
        # Set seed of DataSetSplitter instance
        self.seed = seed
    
    def split(self):
        """
        """
        # Randomly allocate each tile to train, validation or test set
        self.to_split['split'] = np.random.choice(['train','val','test'],
                                                  len(self.to_split),
                                                  p=self.fractions)
        
    def get_distribution_over_sets(self):
        """
        """
        # Get distribution for each column name
        for quant_col_name in self.quant_col_names:
            df = self.to_split.groupby(['split', quant_col_name]).size().unstack()
            df = df.reindex(['train','val','test'])
            self.distributions[quant_col_name] = df
            
    def get_skewness_of_distribution(self):
        """
        """
        # Initialize skessness value at zero
        skewness_value = 0
        # Add skewness of each distribution
        for distribution in self.distributions.values():
            skewness_value += np.std(distribution, axis=1).sum()
            
        return skewness_value
    
    def find_best_split(self, seed_values, write_to_file=False):
        """
        """
        # Initialize series to store results
        skewness = pd.Series(index=seed_values)
        # Iterate through seed values and get skewness
        for seed in seed_values:
            self.set_seed(seed)
            self.split()
            self.get_distribution_over_sets()
            skewness[seed] = self.get_skewness_of_distribution()
        
        # Set seed to best value
        self.set_seed(skewness.idxmin())
        # Split data with current seed
        self.split()
        # Get distributions with current seed
        self.get_distribution_over_sets()
        
        # Plot results
        matplotlib.rcParams.update({'font.size': 16})
        fig, ax = plt.subplots(1, 1, figsize=(10, 6))
        skewness.plot(ax=ax, label='all options considered')
        ax.plot(skewness.idxmin(), skewness.min(), 'ro', label='selected option')
        ax.set_title('Data splitting options')
        ax.set_xlabel('Random seed')
        ax.set_ylabel('Skewness metric')
        ax.legend(loc='upper right')
        # Write plot to file or show inline
        if write_to_file:
            path = pathlib.Path.cwd() / 'visuals' / 'data_splitting_options.jpg'
            plt.savefig(path, pad_inches=0.1, bbox_inches='tight')
        else:
            plt.show()
            
    def plot_distribution(self, write_to_file=False):
        """
        """
        matplotlib.rcParams.update({'font.size': 16})
        fig, axs = plt.subplots(2, 2, figsize=(20, 10))
        axs = axs.reshape(-1)
        for i, key in enumerate(self.distributions.keys()):
            self.distributions[key].plot.bar(ax=axs[i])
            axs[i].set_title(' '.join(key.split('_')[:-1]))
            axs[i].tick_params(axis='x', labelrotation=0)
            axs[i].set_xlabel('')
            axs[i].set_ylim(0, 2000)
            axs[i].grid(axis='y')
            axs[i].legend(title='Bin index', ncol=1, loc='upper right', labelspacing=.2)

        axs[0].set_ylabel('tile count')
        axs[2].set_ylabel('tile count')
        axs[0].tick_params(labelbottom=False)
        axs[1].tick_params(labelleft=False, labelbottom=False)
        axs[3].tick_params(labelleft=False)
        
        if write_to_file:
            path = pathlib.Path.cwd() / 'visuals' / 'data_splitting_distribution.jpg'
            plt.savefig(path, pad_inches=0.1, bbox_inches='tight')
        else:
            plt.show()
            
    def print_split_table(self):
        """
        """
        # Count number of tiles per set
        df = self.to_split.groupby('split').size().reindex(['train','val','test']).to_frame('tile count')
        # Add total
        total_count = df['tile count'].sum()
        df = pd.concat([df, pd.DataFrame(data=total_count, index=['total'], columns=['tile count'])])
        # Add column for percentage share
        df['share of total'] = df['tile count'].apply(lambda x: round(x / total_count, 2))
        # Print result
        print(df)
    
    def write_data_split_to_file(self):
        """
        """
        self.to_split[['split']].to_parquet(self.data_split_path)