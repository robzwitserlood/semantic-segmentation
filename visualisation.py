import argparse
import json
import pathlib
import h5py

import geopandas as gpd
import matplotlib
import matplotlib.pyplot as plt
from matplotlib.patches import Patch
import numpy as np
import pandas as pd
import PIL as pil
import psycopg
import rasterio as rs
import sqlalchemy
import plotly.graph_objects as go
import plotly.offline as py
from rasterio.plot import show
from rasterio.mask import mask

from config import config_visualisation_aerial, config_visualisation_satellite

matplotlib.rcParams.update({'font.size': 16})

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
        PARAMS.update(config_visualisation_satellite)
    elif args.imagery == 'aerial':
        PARAMS.update(config_visualisation_aerial)
        

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


def create_rgb_overview():
    path = pathlib.Path(PARAMS['path_downsampled_overview'])
    dataset = rs.open(path)
    fig, ax = plt.subplots(1, 1, figsize=(15, 15))
    show(dataset, ax=ax)
    ax.tick_params(left=False,
                   labelleft=False,
                   bottom=False,
                   labelbottom=False)
    ax.margins(0)
    ax.axis('off')
    path = pathlib.Path(PARAMS['storage_dir'],
                        PARAMS['filenames']['downsampled_overview'])
    plt.savefig(path, pad_inches=0.1, bbox_inches='tight', dpi=300)


def plot_training_process():
    # Initiate dictionary
    log_dict = dict()
    # Loop through paths
    log_dir = pathlib.Path(PARAMS['log_storage_dir'])
    log_paths = [log_dir / i for i in PARAMS['log_files']]
    for log_path in log_paths:
        # Get name from fixed length filename
        phase_name = log_path.name[15:22]
        with open(log_path, 'r') as fp:
            log_dict[phase_name] = json.load(fp)
    # Initiate a bunch of list for metrics
    phase = list()
    lr = list()
    train_loss = list()
    val_loss = list()
    train_f1 = list()
    val_f1 = list()
    # Extract metrics from log dictionary
    for key1, val1 in log_dict.items():
        for key2, val2 in val1.items():
            phase.append(key1)
            lr.append(val2['lr'])
            train_loss.append(val2['train']['loss'])
            val_loss.append(val2['validate']['loss'])
            train_f1.append(val2['train']['f1'])
            val_f1.append(val2['validate']['f1'])
    fig, ax = plt.subplots(3, 1,
                           figsize=(15, 8),
                           gridspec_kw={'height_ratios': [.5, 4, 4]})
    ax[1].plot(lr, label='learning rate')
    ax[2].plot(train_f1, 'g--', label='train F1')
    ax[2].plot(val_f1, 'g', label='validation F1')
    ax[2].plot(train_loss, 'r--', label='train loss')
    ax[2].plot(val_loss, 'r', label='validation loss')
    ax[0].axis('off')
    ax[2].set_xlabel('Epoch [-]')
    ax[1].set_yscale('log')
    phase_color_map = {'phase_1': '0.8',
                       'phase_2': '0.7',
                       'phase_3': '0.6'}
    for phase_name, color in phase_color_map.items():
        phase_idxs = [i for i, item in enumerate(phase) if item == phase_name]
        xmin = max(min(phase_idxs) - 1, 0)
        xmax = max(phase_idxs)
        ax[0].axvspan(xmin, xmax, facecolor=color, alpha=0.3)
        ax[0].text((xmin - 1 + xmax) / 2, .3, 'Phase ' + phase_name[-1],
                   ma='center')
        ax[1].axvspan(xmin, xmax, facecolor=color, alpha=0.3)
        ax[2].axvspan(xmin, xmax, facecolor=color, alpha=0.3)
    ax[0].set_xmargin(0)
    ax[1].set_xmargin(0)
    ax[2].set_xmargin(0)
    ax[1].legend()
    ax[2].legend()
    path = pathlib.Path(PARAMS['storage_dir'],
                        PARAMS['filenames']['training_process'])
    plt.savefig(path, pad_inches=0.1, bbox_inches='tight', dpi=300)


def trim(source_filepath, target_filepath=None, background=None):
    if not target_filepath:
        target_filepath = source_filepath
    img = pil.Image.open(source_filepath)
    if background is None:
        background = img.getpixel((0, 0))
    border = pil.Image.new(img.mode, img.size, background)
    diff = pil.ImageChops.difference(img, border)
    bbox = diff.getbbox()
    img = img.crop(bbox) if bbox else img
    img.save(target_filepath)


def save_confusion_matrix_as_image(cell_values, path):
    # Convert to string and add html tag for alignment
    str_val = [f'<br>{i:.1%}' for j in cell_values for i in j]
    # Set header and index
    table_values = np.full((5, 5), '', dtype=object)
    table_values[0, 2] = '<b>prediction</b><br>'
    table_values[1, 2:] = ['<b>volledig</b><br><b>verhard</b>',
                            '<b>volledig</b><br><b>onverhard</b>',
                            '<b>onbekend</b><br> ']
    table_values[2, 0] = '<b>ground</b><br><b>truth</b>'
    table_values[2:, 1] = ['<b>volledig</b><br><b>verhard</b>',
                            '<b>volledig</b><br><b>onverhard</b>',
                            '<b>onbekend</b><br> ']
    # Include cell values
    table_values[2:, 2:] = np.array(str_val).reshape(3, 3)
    # Set coloring schema
    fill_color = np.full((5, 5),
                        'rgb(119, 124, 0)',
                        dtype=object)
    fill_color[2:, 2:] = 'rgb(255, 255, 255)'
    fill_color[1:, 1] = 'rgb(191, 191, 191)'
    fill_color[1, 1:] = 'rgb(191, 191, 191)'
    # Create and style figure
    fig = go.Figure(
        layout=go.Layout(
            autosize=False,
            width=300,
            height=210,
            margin=go.layout.Margin(
                l=0,
                r=0,
                b=0,
                t=0,
            )
        )
    )
    # Add table to figure
    fig.add_trace(go.Table(
        header=go.table.Header(
            fill_color='rgb(255, 255, 255)'
        ),
        columnwidth=[0.16, 0.21, 0.21, 0.21, 0.21],
        cells=go.table.Cells(
            align=['left', 'left', 'center', 'center', 'center'],
            values=table_values.T,
            height=21,
            fill_color=fill_color,
            line_color='rgb(119, 124, 0)',
            font=dict(size=10.5, family='Arial')
            )
        )
    )
    # Savev as image
    fig.write_image(path, scale=8)


def export_confusion_matrices(engine):
    # Get confusion matrices
    df = pd.read_sql("""
                     SELECT a0p0,
                     a0p1,
                     a1p0,
                     a1p1,
                     a2p0,
                     a2p1,
                     split 
                     FROM dimTiles
                     """,
                     con=engine)
    # Groupby and sum by split
    df_grouped = df.groupby('split').sum()
    # Create dict with confusion matrices
    cm = {i: np.concatenate([df_grouped.loc[i].values.reshape((3, 2)),
                             np.zeros([3, 1])], axis=1)
          for i in ['train', 'validate', 'test']}
    # Normalize confusion matrices
    cm = {key: val / val.sum(axis=1, keepdims=True) for key, val in cm.items()}
    # Save as image
    for key, val in cm.items():
        path = pathlib.Path(PARAMS['storage_dir'],
                            PARAMS['filenames']['cm_by_split'][key])
        save_confusion_matrix_as_image(val, path)


def get_nbh_gdf():
    """Get geodataframe with geometries of neighbourhoods
    """
    path = pathlib.Path(PARAMS['nbh']['storage_dir'],
                        PARAMS['nbh']['filename'])
    # Open file as geodataframe
    gdf = gpd.read_file(path, driver='GML')
    # Select municipality Utrecht only
    gdf = gdf[gdf.gemeentenaam == 'Utrecht']
    # Select name and geometry column only
    gdf = gdf[['wijknaam', 'geometry']]

    return gdf


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


def export_ground_truth_label_distribution(engine):
    """Print confusion matrices in nice format
    """
    tile_gdf = gpd.read_postgis("""
                                SELECT count_0,
                                count_1,
                                count_2,
                                bbox_geom
                                FROM dimTiles
                                """,
                                con=engine,
                                geom_col='bbox_geom')
    nbh_gdf = get_nbh_gdf()
    # Make list of names and geometries if neighbourhoods in Utrecht
    nbh_list = list(zip(nbh_gdf.wijknaam, nbh_gdf.geometry))
    # Create column with name of neighbourhood
    tile_gdf['nbh'] = tile_gdf.bbox_geom.apply(
        lambda x: tile_in_neighbourhood(x, nbh_list))
    # Groupby and sum by split
    df_grouped = tile_gdf.groupby('nbh').sum()
    labels = list(df_grouped.index) + ['Total']
    data = np.concatenate([df_grouped.values,
                           df_grouped.values.sum(axis=0)[np.newaxis, :]])
    labels_2nd = ['{0:.1E}'.format(pixels) for pixels in data.sum(axis=1)]
    data = 100 * data / data.sum(axis=1, keepdims=True)
    data_cum = data.cumsum(axis=1)
    category_colors = plt.colormaps['viridis'](
        np.linspace(0.01, 0.99, data.shape[1]))

    fig, ax = plt.subplots(figsize=(15, 8))
    ax2 = ax.twinx()
    ax.invert_yaxis()
    ax2.invert_yaxis()
    ax.tick_params(bottom=False,
                   labelbottom=False)
    ax2.xaxis.set_visible(False)
    ax.set_xlim(0, np.sum(data, axis=1).max())
    category_names = ['volledig verhard',
                      'volledig onverhard',
                      'onbekend']
    for i, (colname, color) in enumerate(zip(category_names, category_colors)):
        widths = data[:, i]
        starts = data_cum[:, i] - widths
        rects = ax.barh(labels, widths, left=starts, height=0.8,
                        label=colname, color=color)
        r, g, b, _ = color
        text_color = 'white' if r * g * b < 0.1 else 'darkgrey'
        ax.bar_label(rects, fmt='%.0f%%', label_type='center',
                     color=text_color)
    ax.legend(labels=category_names, bbox_to_anchor=(0, 1),
              loc='lower left', ncol=3)
    ax2.set_ylim(ax.get_ylim())
    ax2.set_yticks(ax.get_yticks())
    ax2.set_yticklabels(labels_2nd)
    ax2.set_ylabel('Pixel count [-]')
    ax.set_xlabel(r'Label share [% of pixel count]')
    path = str(pathlib.Path(PARAMS['storage_dir'],
                            PARAMS['filenames']['pixels_and_shares']))
    plt.savefig(path, pad_inches=0.1, bbox_inches='tight', dpi=300)


def export_confusion_matrices_by_neighbourhood(engine):
    """Print confusion matrices in nice format
    """
    tile_gdf = gpd.read_postgis("""
                                SELECT a0p0,
                                a0p1,
                                a1p0,
                                a1p1,
                                a2p0,
                                a2p1,
                                split,
                                bbox_geom
                                FROM dimTiles
                                """,
                                con=engine,
                                geom_col='bbox_geom')
    nbh_gdf = get_nbh_gdf()
    # Make list of names and geometries if neighbourhoods in Utrecht
    nbh_list = list(zip(nbh_gdf.wijknaam, nbh_gdf.geometry))
    # Create column with name of neighbourhood
    tile_gdf['nbh'] = tile_gdf.bbox_geom.apply(
        lambda x: tile_in_neighbourhood(x, nbh_list))
    # Groupby and sum by split
    df_grouped = tile_gdf.groupby(['nbh', 'split']).sum()
    # Initiate dict for neighboorhood cm
    nbh_names = [i[8:] for i in nbh_gdf.wijknaam]
    # Calc cm for each neighbourhoof
    for nbh_name in nbh_names:
        for i in ['train', 'validate', 'test']:
            cm_nonzero = df_grouped.loc[(nbh_name, i)].values.reshape((3, 2))
            val = np.concatenate([cm_nonzero, np.zeros([3, 1])], axis=1)
            # Normalize confusion matrices if required
            val = val / val.sum(axis=1, keepdims=True)
            path = str(pathlib.Path(PARAMS['storage_dir'],
                                    PARAMS['filenames']['cm_by_nbh'][i]))
            path = path.replace('NBH', nbh_name)
            save_confusion_matrix_as_image(val, path)


def plot_performance_by_neighbourhood(engine):
    """Plot performance and borders of neighbourhoods
    
    :param metric: performance metric to plot 
    :type metric: str
    """
    tile_gdf = gpd.read_postgis("""
                            SELECT a0p0,
                            a0p1,
                            a1p0,
                            a1p1,
                            a2p0,
                            a2p1,
                            split,
                            bbox_geom
                            FROM dimTiles
                            """,
                            con=engine,
                            geom_col='bbox_geom')
    nbh_gdf = get_nbh_gdf()
    # Make list of names and geometries if neighbourhoods in Utrecht
    nbh_list = list(zip(nbh_gdf.wijknaam, nbh_gdf.geometry))
    # Create column with name of neighbourhood
    tile_gdf['nbh'] = tile_gdf.bbox_geom.apply(
        lambda x: tile_in_neighbourhood(x, nbh_list))
    # Calculate performance metrics
    tile_gdf['recall_onverhard'] = tile_gdf['a1p1']\
        / (tile_gdf['a1p0'] + tile_gdf['a1p1'])
    tile_gdf['recall_verhard'] = tile_gdf['a0p0']\
        / (tile_gdf['a0p0'] + tile_gdf['a0p1'])
    # Set boundary as geometry
    nbh_gdf.geometry = nbh_gdf.geometry.boundary
    # Add coordinate column to position text labels
    nbh_gdf['coords'] = nbh_gdf['geometry'].apply(
        lambda x: x.centroid.coords[:])
    nbh_gdf['coords'] = [coords[0] for coords in nbh_gdf['coords']]
    for metric in ['recall_verhard', 'recall_onverhard']:
        # Create figure
        fig, ax = plt.subplots(1, 3, figsize=(15, 8),
                                gridspec_kw={'width_ratios': [30, 30, 1]})
        # Plot neighbourhood borders on both
        nbh_gdf.plot(ax=ax[0], edgecolor='0.65', linewidth=3)
        nbh_gdf.plot(ax=ax[1], edgecolor='0.65', linewidth=3)
        # Plot performance metric
        tile_gdf.plot(column=metric,
                        legend=True,
                        ax=ax[1],
                        cax=ax[2],
                        cmap='RdYlGn')
        # Do not display labels
        ax[0].tick_params(left=False,
                            labelleft=False,
                            bottom=False,
                            labelbottom=False)
        ax[1].tick_params(left=False,
                            labelleft=False,
                            bottom=False,
                            labelbottom=False)
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
        ax[0].texts[4].set_y(ax[0].texts[4].xy[1] + 400)  # Oost
        ax[0].texts[7].set_y(ax[0].texts[7].xy[1] + 400)  # Zuidwest
        ax[0].texts[8].set_y(ax[0].texts[8].xy[1] + 500)  # Leidsche Rijn
        ax[0].texts[9].set_y(ax[0].texts[9].xy[1] - 400)  # Vleuten-De Meern
        # Manually adjust position of axis for colorbar
        bbox = ax[2].get_position()
        intervaly = ax[1].get_position().intervaly
        bbox.update_from_data_y(intervaly)
        ax[2].set_position(bbox)
        # Write to file
        path = str(pathlib.Path(PARAMS['storage_dir'],
                                PARAMS['filenames']['performance_overview']))
        path = path.replace('METRIC', metric)
        plt.savefig(path, pad_inches=0.1, bbox_inches='tight', dpi=1600)


def export_example_tiles():
    """Export example tiles
    """
    # Set file name to read inputs and outputs
    f = h5py.File(PARAMS['hdf5']['path'], 'r')
    # Initialize figure
    fig, ax = plt.subplots(1, 3, figsize=(26, 8))
    ax[0].set_title('image')
    ax[1].set_title('input labels')
    ax[2].set_title('predicted labels')
    class_dict = {0: 'volledig_verhard',
                  1: 'volledig_onverhard',
                  2: 'onbekend'}
    # Formatting
    for axis in ax:
        axis.tick_params(left=False,
                         labelleft=False,
                         bottom=False,
                         labelbottom=False)
    for tile_id in PARAMS['example_tile_ids']:
        rgb = np.moveaxis(f['rgb'][tile_id, :, :, :], 0, -1)
        gt = np.moveaxis(f['gt'][tile_id, :, :, :], 0, -1)
        pred = np.moveaxis(f['pred'][tile_id, :, :, :], 0, -1)
        # Plot rgb of satelite image as base layer
        ax[0].imshow(rgb)
        ax[1].imshow(rgb)
        ax[2].imshow(rgb)
        # Plot masks on top of base layer
        im = ax[1].imshow(gt, alpha=0.5, vmin=0, vmax=2)
        ax[2].imshow(pred, alpha=0.5, vmin=0, vmax=2)
        patches = [Patch(color=im.cmap(im.norm(key)), label=value)
                   for key, value in class_dict.items()]
        ax[2].legend(handles=patches, ncol=3,
                     bbox_to_anchor=(1.06, -0.025), loc=1,
                     frameon=False)
        path = str(pathlib.Path(PARAMS['storage_dir'],
                                PARAMS['filenames']['example_tiles']))
        path = path.replace('TILE_ID', str(tile_id))
        plt.savefig(path, pad_inches=0.1, bbox_inches='tight', dpi=800)
    f.close()


def plot_bgt_object_type_count(engine):
    """Plot object count in the bgt

    :param metric: performance metric to plot 
    :type metric: str
    """
    df = pd.read_sql(
        """
        SELECT layer as "object type",
        COUNT(element_id) as "object count [-]"
        FROM dimGroundTruth
        GROUP BY layer
        ORDER BY "object count [-]"
        """,
        engine)
    df['object count [x1.000]'] = df['object count [-]'] / 1000
    fig, ax = plt.subplots(1, 1,
                           figsize=(10, 5))
    df.plot.barh(x='object type',
                 y='object count [x1.000]',
                 ax=ax,
                 legend=False,
                 width=0.7)
    ax.set_xlabel('object count [x1.000]')
    path = pathlib.Path(PARAMS['storage_dir'],
                        PARAMS['filenames']['bgt_object_type_count'])
    plt.savefig(path, pad_inches=0.1, bbox_inches='tight', dpi=300)


def plot_rgb_and_contrast(engine, geom):
    """Plot rgd and contrast side by side

    :param metric: performance metric to plot 
    :type metric: str
    :param geom: geometry 
    :type geom: geom
    """
    gdf = gpd.read_postgis(
        """
        SELECT contrast,
        bbox_geom
        FROM dimTiles
        """,
        engine,
        geom_col='bbox_geom')
    path = PARAMS['path_downsampled_overview']
    dataset = rs.open(path)
    data, _ = mask(dataset, [geom], crop=True, nodata=255)
    fig, ax = plt.subplots(1, 3,
                           figsize=(15, 8),
                           gridspec_kw={'width_ratios': [30, 30, 1]})
    show(data, ax=ax[0])
    gdf.plot(column='contrast', ax=ax[1], legend=True, cax=ax[2])
    ax[0].tick_params(left=False,
                      labelleft=False,
                      bottom=False,
                      labelbottom=False)
    ax[1].tick_params(left=False,
                      labelleft=False,
                      bottom=False,
                      labelbottom=False)
    ax[1].margins(0)
    ax[0].set_title('Satelliet beeld')
    ax[1].set_title('contrast (tile level)')
    bbox = ax[2].get_position()
    intervaly = ax[1].get_position().intervaly
    bbox.update_from_data_y(intervaly)
    ax[2].set_position(bbox)
    path = pathlib.Path(PARAMS['storage_dir'],
                        PARAMS['filenames']['rgb_and_contrast'])
    plt.savefig(path, pad_inches=0.1, bbox_inches='tight', dpi=300)


def plot_rgb_and_excluded(engine, geom):
    """Plot rgd and contrast side by side

    :param metric: performance metric to plot 
    :type metric: str
    :param geom: geometry 
    :type geom: geom
    """
    gdf = gpd.read_postgis(
        """
        SELECT contrast,
        bbox_geom,
        split
        FROM dimTiles
        WHERE split = 'excluded'
        """,
        engine,
        geom_col='bbox_geom')
    path = PARAMS['path_downsampled_overview']
    dataset = rs.open(path)
    data, transform = mask(dataset, [geom], crop=True, nodata=255)
    fig, ax = plt.subplots(1, 1, figsize=(8, 8))
    show(data, transform=transform, ax=ax)
    if not gdf.empty:
        gdf.plot(column='split', ax=ax, legend=True)
    ax.tick_params(left=False,
                labelleft=False,
                bottom=False,
                labelbottom=False)
    ax.margins(0)
    path = pathlib.Path(PARAMS['storage_dir'],
                        PARAMS['filenames']['rgb_and_excluded'])
    plt.savefig(path, pad_inches=0.1, bbox_inches='tight', dpi=300)


def main():
    """
    """
    set_config_params()
    #conn, cur = get_connection_and_cursor(PARAMS['postgres']['db_name'])
    engine = get_engine(PARAMS['postgres']['db_name'])
    geom = get_geometry_to_be_tiled()
    #plot_bgt_object_type_count(engine)
    #create_rgb_overview()
    #plot_rgb_and_contrast(engine, geom)
    plot_rgb_and_excluded(engine, geom)
    #export_ground_truth_label_distribution(engine)
    #plot_training_process()
    #export_confusion_matrices(engine)
    #export_confusion_matrices_by_neighbourhood(engine)
    #plot_performance_by_neighbourhood(engine)
    #export_example_tiles()


if __name__ == "__main__":
    main()
