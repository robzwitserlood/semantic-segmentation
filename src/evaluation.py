import json
import pathlib
import warnings
import matplotlib

import numpy as np
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt

from tqdm import tqdm
from shapely import geometry

import xarray as xr
import earthpy.plot as ep
from matplotlib.patches import Patch
from owslib.wfs import WebFeatureService


matplotlib.rcParams.update({'font.size': 16})


def tile_in_neighbourhood(tile_geom, nbh_list):
    """Return name of neighbourhood the tile is in
    
    :param tile_geom: geometry of tile
    :type tile_geom: shapely.geometry
    :param nbh_list: names and geometries of neighbourhoods
    :type nbh_list: list
    """
    # Initialize name of neighbourhood at none
    tile_nbh = None
    # Loop through list to find correct neighboorhood
    for name, geom in nbh_list:
        if geom.contains(tile_geom.centroid):
            # Skip first 8 characters of name (are uninformative)
            tile_nbh = name[8:]
            break
        else:
            continue
    
    return tile_nbh

class PerformanceEvaluation:
    """Evaluate performance of model by analysing predictions
    """

    def __init__(self, dataset_dir, datasplit_filename, agg_tile_filename,
                 pred_dir, agg_pred_filename,
                 log_dir, log_files,
                 nbh_path, nbh_url,
                 tbbox, geom):
        """Constructor method

        :param dataset_dir: directory containing tile .nc files and parquet metadata
        :param datasplit_filename: parquet file name for data split
        :param agg_tile_filename: parquet file name for aggregated tile characteristics
        :param pred_dir: directory containing prediction .nc files
        :param agg_pred_filename: parquet file name for aggregated predictions
        :param log_dir: directory containing training log JSON files
        :param log_files: list of training log file names
        :param nbh_path: path to neighbourhood GML file
        :param nbh_url: WFS URL to download neighbourhood GML if not cached
        :param tbbox: tiled bounding box object
        :param geom: region geometry
        """
        cwd = pathlib.Path().cwd()
        datasplit_path = cwd / dataset_dir / datasplit_filename
        agg_pred_path = cwd / pred_dir / agg_pred_filename
        agg_tile_path = cwd / dataset_dir / agg_tile_filename
        log_paths = [cwd / log_dir / f for f in log_files]
        # Set attributes
        self.tile_input_dir = cwd / dataset_dir
        self.tile_output_dir = cwd / pred_dir
        self.datasplit = pd.read_parquet(datasplit_path)
        self.agg_pred = pd.read_parquet(agg_pred_path)
        self.agg_tile = pd.read_parquet(agg_tile_path)
        self.log_dict = self.get_log_dict(log_paths)
        self.nbh_gdf = self.get_nbh_gdf(pathlib.Path(nbh_path), nbh_url)
        self.tbbox = tbbox
        self.geom = geom
                
    def get_log_dict(self, log_paths):
        """Create one dictionary from three log files
        """
        # Initiate dictionary
        log_dict = dict()
        # Loop through paths
        for log_path in log_paths:
            # Get name from fixed length filename
            phase_name = log_path.name[15:22]
            with open(log_path, 'r') as fp:
                log_dict[phase_name] = json.load(fp)
                
        return log_dict
    
    def get_nbh_gdf(self, nbh_path, url):
        """Get geodataframe with geometries of neighbourhoods
        """
        # Write file if not exists
        if not nbh_path.exists():
            wfs = WebFeatureService(url)
            response = wfs.getfeature(typename=['cbs_wijken_2021'])
            with open(nbh_path, 'wb') as f:
                f.write(response.read())
        # Open file as geodataframe
        gdf = gpd.read_file(nbh_path, driver='GML')
        # Select municipality Utrecht only
        gdf = gdf[gdf.gemeentenaam == 'Utrecht']
        # Select name and geometry column only
        gdf = gdf[['wijknaam', 'geometry']]

        return gdf
    
    def plot_training_process(self, write_to_file=False, dpi_write=200):
        """
        """
        # Initiate a bunch of list for metrics
        phase = list()
        lr = list()
        train_loss = list()
        val_loss = list()
        train_f1 = list()
        val_f1 = list()
        # Extract metrics from log dictionary
        for key1, val1 in self.log_dict.items():
            for key2, val2 in val1.items():
                phase.append(key1)
                lr.append(val2['lr'])
                train_loss.append(val2['train']['loss'])
                val_loss.append(val2['val']['loss'])
                train_f1.append(val2['train']['f1'])
                val_f1.append(val2['val']['f1'])
        
        fig, ax = plt.subplots(3, 1, figsize=(15, 8), gridspec_kw={'height_ratios': [.5, 4, 4]})

        ax[1].plot(lr, label='learning rate')
        ax[2].plot(train_f1, 'g--', label='train F1')
        ax[2].plot(val_f1, 'g', label='validation F1')
        ax[2].plot(train_loss, 'r--', label='train loss')
        ax[2].plot(val_loss, 'r', label='validation loss')
        
        ax[0].axis('off')
        ax[2].set_xlabel('Epoch [-]')
        ax[1].set_yscale('log')
        phase_color_map = {'phase_1': '0.8', 'phase_2': '0.7', 'phase_3': '0.6'}
        for phase_name, color in phase_color_map.items():
            phase_idxs = [i for i, item in enumerate(phase) if item == phase_name]
            xmin = max(min(phase_idxs) - 1, 0)
            xmax = max(phase_idxs)
            ax[0].axvspan(xmin, xmax, facecolor=color ,alpha=0.3)
            ax[0].text((xmin - 1 + xmax) / 2, .3, 'Phase ' + phase_name[-1], ma='center')
            ax[1].axvspan(xmin, xmax, facecolor=color ,alpha=0.3)
            ax[2].axvspan(xmin, xmax, facecolor=color ,alpha=0.3)
        
        ax[0].set_xmargin(0)
        ax[1].set_xmargin(0)
        ax[2].set_xmargin(0)
        
        ax[1].legend()
        ax[2].legend()

        if write_to_file:
            path = pathlib.Path.cwd() / 'visuals' / 'training_process.jpg'
            plt.savefig(path, pad_inches=0.1, bbox_inches='tight', dpi=dpi_write)
        else:
            plt.show()
    
    def join_split_and_agg_predictions(self):
        """Join dataframes with split and aggregated predictions
        on tile id (which is the index for both dataframes)
        """
        df = pd.concat([self.datasplit, self.agg_pred], axis=1)
        
        return df
    
    def join_split_and_agg_tile_and_agg_predictions(self):
        """Join dataframes with split, aggregated file characteristics and 
        aggregated predictions on tile id (which is the index for all dataframes)
        """
        df = pd.concat([self.datasplit, self.agg_tile, self.agg_pred], axis=1, join='inner')
        
        return df
    
    def join_all(self):
        """Join all (geo)dataframes on tile id (which is the index for all dataframes)
        """
        # Get joined dataframes 
        df = self.join_split_and_agg_tile_and_agg_predictions()
        # Add geomtery and return as geodataframe
        gdf = self.create_geodataframe(df)
        # Make list of names and geometries of neighbourhoods in Utrecht
        nbh_list = list(zip(self.nbh_gdf.wijknaam, self.nbh_gdf.geometry))
        # Create column with name of neighbourhood
        gdf['nbh'] = gdf.geometry.apply(lambda x: tile_in_neighbourhood(x, nbh_list))
        
        return gdf
    
    def get_confusion_matrices_per_split(self, dim=None):
        """Get confusion matrices per split
        
        :param dim: dimension to normalize
        :type dim: int
        """
        df = self.join_split_and_agg_predictions()
        # Groupby and sum by split
        df_grouped = df.groupby('split').sum()
        # Create dict with confusion matrices
        cm = {i: df_grouped.loc[i].values.reshape((3, 3)) for i in ['train', 'val', 'test']}
        # Normalize confusion matrices if required
        if dim:
            cm = {key: val / val.sum(axis=dim, keepdims=True) for key, val in cm.items()}
        
        return cm
    
    def split_tiles_by_region(self, show_plot=False, write_to_file=False, dpi_write=200):
        """
        """
        # Get dataframe with tile aggregated data
        df = self.join_split_and_agg_predictions()
        # Add geomtries to tiles and return as geodataframe
        gdf = self.create_geodataframe(df)
        # Split geosataframe to tile inside and outside of geometry of region
        gdf_in = gdf.clip(self.geom)
        gdf_out = gdf[~gdf.index.isin(gdf_in.index)]
        # Plot if requested
        if show_plot:
            fig, ax = plt.subplots(1, 2, figsize=(15, 8))
            gdf_in.plot(column='split', legend=True, ax=ax[0])
            gdf_out.plot(column='split', ax=ax[1])
            # Set axis equal (assume ax[1] is leading)
            ax[0].set_xlim(ax[1].get_xlim())
            ax[0].set_ylim(ax[1].get_ylim())
            # Do not display labels
            ax[0].tick_params(left=False, labelleft=False, bottom=False, labelbottom=False)
            ax[0].margins(0)
            ax[1].tick_params(left=False, labelleft=False, bottom=False, labelbottom=False)
            ax[1].margins(0)
            # Set titles
            ax[0].set_title('Binnen gemeentegrens')
            ax[1].set_title('Buiten gemeentegrens')
        # Write to file if requested
        if write_to_file:
            path = pathlib.Path.cwd() / 'visuals' / 'split_tiles_by region.jpg'
            plt.savefig(path, pad_inches=0.1, bbox_inches='tight', dpi=dpi_write)
        elif show_plot:
            plt.show()
            
        return gdf_in, gdf_out
    
    def get_confusion_matrices_by_region(self, dim=None):
        """Get confusion matrices by region
        
        :param dim: dimension to normalize
        :type dim: int
        """
        gdf_in, gdf_out = self.split_tiles_by_region()
        # Groupby and sum by split
        gdf_in_grouped = gdf_in.groupby('split').sum()
        gdf_out_grouped = gdf_out.groupby('split').sum()
        # Create dict with confusion matrices
        cm_in = {i: gdf_in_grouped.loc[i].values.reshape((3, 3)) for i in ['train', 'val', 'test']}
        cm_out = {i: gdf_out_grouped.loc[i].values.reshape((3, 3)) for i in ['train', 'val', 'test']}
        # Normalize confusion matrices if required
        if dim:
            cm_in = {key: val / val.sum(axis=dim, keepdims=True) for key, val in cm_in.items()}
            cm_out = {key: val / val.sum(axis=dim, keepdims=True) for key, val in cm_out.items()}
              
        return cm_in, cm_out
    
    def categorize_tiles_by_neighbourhood(self):
        """Add column for neighbourhood per tile
        """
        # Get geodataframe with all tiles in municipality Utrecht
        gdf_in, _ = self.split_tiles_by_region()
        # Make list of names and geometries if neighbourhoods in Utrecht
        nbh_list = list(zip(self.nbh_gdf.wijknaam, self.nbh_gdf.geometry))
        # Create column with name of neighbourhood
        gdf_in['nbh'] = gdf_in.geometry.apply(lambda x: tile_in_neighbourhood(x, nbh_list))
        
        return gdf_in
    
    def get_confusion_matrices_per_neighbourhood(self, dim=None):
        """Get confusion matrices per neighbourhood
        
        :param dim: dimension to normalize
        :type dim: int
        """
        gdf = self.categorize_tiles_by_neighbourhood()
        # Groupby and sum by split
        df_grouped = gdf.groupby(['nbh', 'split']).sum()
        # Initiate dict for neighboorhood cm
        nbh_cm = {i[8:]: None for i in self.nbh_gdf.wijknaam}
        # Calc cm for each neighbourhoof
        for nbh_name in nbh_cm.keys():
            # Create dict with confusion matrices
            nbh_cm[nbh_name] = {i: df_grouped.loc[(nbh_name, i)].values.reshape((3, 3)) for i in ['train', 'val', 'test']}
            # Normalize confusion matrices if required
            if dim:
                nbh_cm[nbh_name] = {key: val / val.sum(axis=dim, keepdims=True) for key, val in nbh_cm[nbh_name].items()}

        return nbh_cm
    
    def pretty_print_confusion_matrices(self):
        """Print confusion matrices in nice format
        """
        # Get confusion matrices
        cm = self.get_confusion_matrices_per_split(dim=1)
        # Set class names
        classes = ['impervious', 'pervious', 'unknown']
        # Create multi-index for DataFrame
        index = pd.MultiIndex.from_arrays([['ground truth'] * 3, classes])
        # Display all matrices
        for key, val in cm.items():
            # Create multi-column for DataFrame
            columns = pd.MultiIndex.from_arrays([['prediction'] * 3, classes], names=[key + ' set', ''])
            display(pd.DataFrame(100 * val.round(3), index=index, columns=columns))
            
    def pretty_print_confusion_matrices_by_region(self):
        """Print confusion matrices in nice format
        """
        # Get confusion matrices
        cm_in, cm_out = self.get_confusion_matrices_by_region(dim=1)
        # Set class names
        classes = ['impervious', 'pervious', 'unknown']
        # Create multi-index for DataFrame
        index = pd.MultiIndex.from_arrays([['ground truth'] * 3, classes])
        # Display all matrices
        for key in cm_in.keys():
            # Create multi-column for DataFrame
            columns = pd.MultiIndex.from_arrays([['prediction'] * 6,
                                                 ['Binnen gemeentegrens'] * 3 + ['Buiten gemeentegrens'] * 3,
                                                 classes + classes], names=[key + ' set', '', ''])
            val = np.concatenate([cm_in[key], cm_out[key]], axis=1)
            display(pd.DataFrame(100 * val.round(3), index=index, columns=columns))
            
    def pretty_print_confusion_matrices_by_neighbourhood(self, nbh_1, nbh_2):
        """Print confusion matrices in nice format
        """
        nbh_cm = self.get_confusion_matrices_per_neighbourhood(dim=1)
        # Get confusion matrices
        cm_1, cm_2 = nbh_cm[nbh_1], nbh_cm[nbh_2]
        # Set class names
        classes = ['impervious', 'pervious', 'unknown']
        # Create multi-index for DataFrame
        index = pd.MultiIndex.from_arrays([['ground truth'] * 3, classes])
        # Display all matrices
        for key in cm_1.keys():
            # Create multi-column for DataFrame
            columns = pd.MultiIndex.from_arrays([['prediction'] * 6,
                                                 [nbh_1] * 3 + [nbh_2] * 3,
                                                 classes + classes], names=[key + ' set', '', ''])
            val = np.concatenate([cm_1[key], cm_2[key]], axis=1)
            display(pd.DataFrame(100 * val.round(3), index=index, columns=columns))
    
    def plot_performance_by_neighbourhood(self, metric, write_to_file=False, dpi_write=300):
        """Plot performance and borders of neighbourhoods
        
        :param metric: performance metric to plot 
        :type metric: str
        """
        # Get dataframe with tile aggregated data
        gdf_in, _ = self.split_tiles_by_region()
        # Calculate performance metrics
        gdf_in['recall_pervious'] = gdf_in['cm_1_1'] / (gdf_in['cm_1_0'] + gdf_in['cm_1_1'])
        gdf_in['recall_impervious'] = gdf_in['cm_0_0'] / (gdf_in['cm_0_0'] + gdf_in['cm_0_1'])
        # Copy dataframe with neighbourhoods to temporarily add coordinate column
        nbh_gdf = self.nbh_gdf.copy()
        nbh_gdf.geometry = nbh_gdf.geometry.boundary
        # Add coordinate column to position text labels with neighbourhood names
        nbh_gdf['coords'] = nbh_gdf['geometry'].apply(lambda x: x.centroid.coords[:])
        nbh_gdf['coords'] = [coords[0] for coords in nbh_gdf['coords']]
        # Create figure
        fig, ax = plt.subplots(1, 3, figsize=(15, 8), gridspec_kw={'width_ratios': [30, 30, 1]})
        # Plot neighbourhood borders on both
        nbh_gdf.plot(ax=ax[0], edgecolor='firebrick', linewidth=3)
        nbh_gdf.plot(ax=ax[1], edgecolor='firebrick', linewidth=3)
        # Plot performance metric
        gdf_in.plot(column=metric, legend=True, ax=ax[1], cax=ax[2], cmap='Greens')
        # Do not display labels
        ax[0].tick_params(left=False, labelleft=False, bottom=False, labelbottom=False)
        ax[1].tick_params(left=False, labelleft=False, bottom=False, labelbottom=False)
        ax[0].margins(0)
        ax[1].margins(0)
        ax[0].set_title('Wijkgrenzen en -namen')
        ax[1].set_title('Wijkgrenzen + ' + metric.replace('_', ' '))
        # Plot text labels with neighbourhood names
        for idx, row in nbh_gdf.iterrows():
            ax[0].annotate(text=row['wijknaam'][8:],
                           xy=row['coords'],
                           horizontalalignment='center',
                           fontsize=14)
        # Reposition text labels for better visibility
        ax[0].texts[4].set_y(ax[0].texts[4].xy[1] + 400) #Oost
        ax[0].texts[7].set_y(ax[0].texts[7].xy[1] + 400) #Zuidwest
        ax[0].texts[8].set_y(ax[0].texts[8].xy[1] + 500) #Leidsche Rijn
        ax[0].texts[9].set_y(ax[0].texts[9].xy[1] - 400) #Vleuten-De Meern
        # Manually adjust position of axis for colorbar since geopandas messes up
        bbox = ax[2].get_position()
        intervaly = ax[1].get_position().intervaly
        bbox.update_from_data_y(intervaly)
        ax[2].set_position(bbox)
        # Write to file if requested
        if write_to_file:
            path = pathlib.Path.cwd() / 'visuals' / f'plot_{metric}_by_neighbourhood.jpg'
            plt.savefig(path, pad_inches=0.1, bbox_inches='tight', dpi=dpi_write)
        else:
            plt.show()
    
    def get_input_output_correlations(self, split=None):
        """Get correlations between tile aggregates of inputs and outputs

        :param split: data split to include in calc 
        :type split: str
        """
        df = self.join_split_and_agg_tile_and_agg_predictions()
        # Select tiles based on split
        if split:
            df = df[df.split == split]
        # Define inputs and outputs
        output_cols = ['cm_0_0', 'cm_1_1', 'cm_1_0', 'cm_0_1', 'cm_2_0', 'cm_2_1']
        input_cols = self.agg_tile.columns[3:]
        # Calculates correlation coefficients
        df_corr = df.corr().loc[input_cols, output_cols]
        # Make column names more expressive
        multi_col = pd.MultiIndex.from_arrays([
            ['output'] * 6,
            ['correct'] * 2 + ['incorrect'] * 2 + ['unknown'] * 2,
            ['impervious', 'pervious'] * 3,
        ])
        df_corr.columns = multi_col
        # Make index names more expressive
        multi_ind = pd.MultiIndex.from_arrays([
            ['input'] * len(df_corr.index),
            df_corr.index,
        ])
        df_corr.index = multi_ind
        

        return df_corr
    
    def pretty_print_input_output_correlations(self, split=None):
        """Print input-output correlations in heatmap format
        
        :param split: data split to include in calc 
        :type split: str
        """
        df = self.get_input_output_correlations(split=split)
        # Set heatmap-like format
        display(df.style.background_gradient(cmap='RdYlGn', axis=None).set_precision(2))
        
    def plot_input_labels_and_predicted_labels(self, tile_id, write_to_file=False, dpi_write=300):
        """Plot the input image, input labels and predicted labels
        side-by-side
        
        :param tile_id: id of tile to plot 
        :type tile_id: int
        :param write_to_file: indicator to write plot to file 
        :type write_to_file: bool
        :param dpi_write: dots per inch for writing 
        :type dpi_write: int
        """
        # Set file name to read inputs and outputs
        file_name = f'tile_{str(tile_id).zfill(5)}.nc'
        # Read inputs
        input_path = self.tile_input_dir / file_name
        input_ds = xr.open_dataset(input_path)
        input_ds.close()
        # Read outputs
        output_path = self.tile_output_dir / file_name
        output_ds = xr.open_dataset(output_path)
        output_ds.close()
        # Initialize figure
        fig, ax = plt.subplots(1, 3, figsize=(17, 5))
        # Plot rgb of satelite image as base layer
        ep.plot_rgb(input_ds[['red', 'green', 'blue']].to_array().values, ax=ax[0])
        ep.plot_rgb(input_ds[['red', 'green', 'blue']].to_array().values, ax=ax[1])
        ep.plot_rgb(input_ds[['red', 'green', 'blue']].to_array().values, ax=ax[2])
        # Transform labels to masks in order to plot on top of base layer
        input_mask = input_ds[['impervious', 'pervious', 'unknown']].to_array().values
        input_mask = np.argmax(input_mask, axis=0)
        output_mask = output_ds.to_array().values.squeeze()
        # Plot masks on top of base layer
        im = ax[1].imshow(input_mask, alpha=0.7, vmin=0, vmax=2)
        ax[2].imshow(output_mask, alpha=0.7, vmin=0, vmax=2)
        # Formatting
        ax[0].set_title('image')
        ax[1].set_title('input labels')
        ax[2].set_title('predicted labels')
        class_dict = {0: 'impervious', 1: 'pervious', 2: 'unknown'}
        patches = [Patch(color=im.cmap(im.norm(key)), label=value) for key, value in class_dict.items()]
        ax[2].legend(handles=patches, ncol=3, bbox_to_anchor=(1.06, -0.025), loc=1, frameon=False)

        if write_to_file:
            path = pathlib.Path.cwd() / 'visuals' / f'input_labels_and_predicted_labels_tile_{tile_id}.jpg'
            plt.savefig(path, pad_inches=0.1, bbox_inches='tight', dpi=dpi_write)
        else:
            plt.show()
    
    def create_geodataframe(self, df):
        """
        """
        # Create geometry for each tile for visualization purposes
        tile_geoms = [geometry.box(*tile_bbox) for i, tile_bbox in enumerate(self.tbbox.iter_bbox()) if i in df.index]
        # Create Geodataframe to facilitate analysis
        gdf = gpd.GeoDataFrame(data=df, geometry=tile_geoms, crs=self.tbbox.crs)
        
        return gdf