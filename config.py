import copy
# Config file
# This file contains all configuration settings for the pilot_verharding_stad repository
# The file is devided into section that correspond to modules of this repository

# Module download_data.py
config_download_data = {
    'region_name': 'Utrecht',
    'process_bestuurlijkegebieden': {
        'wfs_url': 'https://service.pdok.nl/kadaster/bestuurlijkegebieden/wfs/v1_0?request=GetCapabilities&service=WFS',
        'layer_name': 'Gemeentegebied',
        'storage_dir': 'X:/Kennisbasis/Datascience/AVIVL/data/pilot_verharding_stad/storage/spatial_scope',
        'file_name': 'bestuurlijkegebieden.gml',
    },
    'process_wijkenbuurten': {
        'wfs_url': 'https://service.pdok.nl/cbs/wijkenbuurten/2021/wfs/v1_0?request=getcapabilities&service=wfs',
        'layer_name': 'cbs_wijken_2021',
        'storage_dir': 'X:/Kennisbasis/Datascience/AVIVL/data/pilot_verharding_stad/storage/spatial_scope',
        'file_name': 'cbs_wijken_2021.gml',
    },
    'process_bgt': {
        'post_url': 'https://api.pdok.nl/lv/bgt/download/v1_0/full/custom',
        'get_url': 'https://api.pdok.nl/lv/bgt/download/v1_0/full/custom/RequestId/status',
        'download_url': 'https://api.pdok.nl/downloadLink',
        'api_params': {
            'featuretypes': [
                'ondersteunendwegdeel',
                'pand',
                'onbegroeidterreindeel',
                'ondersteunendwaterdeel',
                'begroeidterreindeel',
                'vegetatieobject',
                'waterdeel',
                'wegdeel',
                'tunneldeel',
            ],
            'geofilter': '',  # Initialize as empty string
            'format': 'gmllight',
        },
        'storage_dir': 'X:/Kennisbasis/Datascience/AVIVL/data/pilot_verharding_stad/storage/bgt',
        'file_name': 'bgtextract.zip',
    },
    'process_luchtfotos': {
        'url_format': 'https://ns_hwh.fundaments.nl/hwh-ortho/2022/Ortho/1/04/beelden_tif_tegels/2022_X_Y_RGB_hrl.tif',
        'bbox': (None, None, None, None),  # Initialize as tuple of None
        'storage_dir': 'X:/Kennisbasis/Datascience/AVIVL/data/pilot_verharding_stad/storage/aerial_imagery',
        'file_name': '2022_X_Y_RGB_hrl.tif',
    },
}

# Module etl.py
config_etl_aerial = {
    'postgres': {
        'user': 'postgres',
        'password': 'password',
        'db_name': 'aerial_imagery',
        'host': '127.0.0.1',
        'create_table_queries': [
            """
            DROP TABLE IF EXISTS dimImageFiles;
            CREATE TABLE dimImageFiles(
            file_id SERIAL PRIMARY KEY,
            file_name VARCHAR NOT NULL,
            bound_left NUMERIC(8, 2) NOT NULL,
            bound_bottom NUMERIC(8, 2) NOT NULL,
            bound_right NUMERIC(8, 2) NOT NULL,
            bound_top NUMERIC(8, 2) NOT NULL,
            bbox_geom GEOMETRY NOT NULL)
            """,
            """
            CREATE TYPE split_cat AS ENUM ('train', 'validate', 'test', 'excluded');
            DROP TABLE IF EXISTS dimTiles;
            CREATE TABLE dimTiles(
            tile_id SERIAL PRIMARY KEY,
            row INTEGER NOT NULL,
            col INTEGER NOT NULL,
            bound_left NUMERIC(8, 2) NOT NULL,
            bound_bottom NUMERIC(8, 2) NOT NULL,
            bound_right NUMERIC(8, 2) NOT NULL,
            bound_top NUMERIC(8, 2) NOT NULL,
            bbox_geom GEOMETRY NOT NULL,
            count_0 INTEGER,
            count_1 INTEGER,
            count_2 INTEGER,
            entropy NUMERIC(5, 2),
            psnr NUMERIC(5, 2),
            avg_diff NUMERIC(3, 2),
            avg_edge NUMERIC(3, 2),
            avg_grad NUMERIC(3, 2),
            contrast NUMERIC(5, 4),
            split split_cat,
            a0p0 INTEGER,
            a0p1 INTEGER,
            a1p0 INTEGER,
            a1p1 INTEGER,
            a2p0 INTEGER,
            a2p1 INTEGER)
            """,
            """
            DROP TABLE IF EXISTS factTilesFiles;
            CREATE TABLE factTilesFiles(
            intersect_id SERIAL PRIMARY KEY,
            file_id INTEGER NOT NULL,
            tile_id INTEGER NOT NULL,
            overlap NUMERIC(9, 4) NOT NULL,
            FOREIGN KEY(file_id) REFERENCES dimImageFiles(file_id),
            FOREIGN KEY(tile_id) REFERENCES dimTiles(tile_id))
            """,
            """
            DROP TABLE IF EXISTS dimGroundTruth;
            CREATE TABLE dimGroundTruth(
            element_id SERIAL PRIMARY KEY,
            lokaalID VARCHAR NOT NULL UNIQUE,
            layer VARCHAR NOT NULL,
            label INTEGER NOT NULL,
            geom GEOMETRY NOT NULL)
            """,
            """
            DROP TABLE IF EXISTS factTilesGroundTruth;
            CREATE TABLE factTilesGroundTruth(
            intersect_id SERIAL PRIMARY KEY,
            element_id INTEGER NOT NULL,
            tile_id INTEGER NOT NULL,
            overlap NUMERIC(9, 4) NOT NULL,
            FOREIGN KEY(element_id) REFERENCES dimGroundTruth(element_id),
            FOREIGN KEY(tile_id) REFERENCES dimTiles(tile_id))
            """
        ],
        'dir_imagery': config_download_data['process_luchtfotos']['storage_dir'],
        'file_format_imagery': '2022_*000_*000_RGB_hrl.tif',
        'insert_query_dimImageFiles':
            """
            INSERT INTO dimImageFiles(
            file_id, file_name, bound_left, bound_bottom, bound_right, bound_top, bbox_geom)
            VALUES (DEFAULT, %s, %s, %s, %s, %s, ST_GeomFromText(%s))
            """,
        'dir_geom_to_be_tiled': config_download_data['process_bestuurlijkegebieden']['storage_dir'],
        'file_geom_to_be_tiled': config_download_data['process_bestuurlijkegebieden']['file_name'],
        'region_name': config_download_data['region_name'],
        'select_image_file_by_point':
            """
            SELECT file_name 
            FROM dimImageFiles 
            WHERE ST_Contains(bbox_geom, ST_Point(%s, %s))
            """,
        'tile_width_in_pixels': 256,
        'tile_height_in_pixels': 256,
        'insert_query_dimTiles':
            """
            INSERT INTO dimTiles(
            tile_id, row, col, bound_left, bound_bottom, bound_right, bound_top, bbox_geom)
            VALUES (DEFAULT, %s, %s, %s, %s, %s, %s, ST_GeomFromText(%s))
            """,
        'insert_query_factTilesFiles':
            """
            INSERT INTO factTilesFiles(
            file_id, tile_id, overlap)
            SELECT 
            di.file_id as file_id,
            dt.tile_id as tile_id, 
            ST_Area(ST_Intersection(dt.bbox_geom, di.bbox_geom)) as overlap 
            FROM dimImageFiles AS di
            JOIN dimTiles AS dt
            ON ST_Intersects(dt.bbox_geom, di.bbox_geom)
            """,
        'bgt_storage_dir': config_download_data['process_bgt']['storage_dir'],
        'bgt_used_layers': config_download_data['process_bgt']['api_params']['featuretypes'],
        'label_id_to_name': {
            0: 'volledig_verhard',
            1: 'volledig_onverhard',
            2: 'onbekend'},
        'layer_to_label': {
            'pand': 0,
            'ondersteunendwaterdeel': 1,
            'vegetatieobject': 1,
            'waterdeel': 1,
            'tunneldeel': 0
        },
        'fysiekVoorkomen_to_label': {
            'groenvoorziening': 1,
            'open verharding': 0,
            'gesloten verharding': 0,
            'half verhard': 2,
            'onverhard': 1,
            'transitie': 2,
            'erf': 2,
            'zand': 1,
            'grasland overig': 1,
            'grasland agrarisch': 1,
            'struiken': 1,
            'loofbos': 1,
            'bouwland': 1,
            'gemengd bos': 1,
            'houtwal': 1,
            'fruitteelt': 1,
            'rietland': 1,
            'boomteelt': 1,
            'naaldbos': 1,
            'moeras': 1,
            'heide': 1
        },
        'insert_query_dimGroundTruth':
            """
            INSERT INTO dimGroundTruth(
            element_id, lokaalID, layer, label, geom)
            VALUES (DEFAULT, %s, %s, %s, ST_GeomFromText(%s))
            """,
        'select_distinct_row_dimTiles':
            """
            SELECT DISTINCT row FROM dimTiles
            """,
        'drop_constraints_factTilesGroundTruth':
            """
            ALTER TABLE factTilesGroundTruth
            DROP CONSTRAINT factTilesGroundTruth_pkey, 
            DROP CONSTRAINT factTilesGroundTruth_element_id_fkey,
            DROP CONSTRAINT factTilesGroundTruth_tile_id_fkey
            """,
        'insert_query_factTilesGroundTruth':
            """
            INSERT INTO factTilesGroundTruth(
            element_id, tile_id, overlap)
            SELECT 
            dg.element_id as element_id,
            dt.tile_id as tile_id, 
            ST_Area(ST_Intersection(dg.geom, dt.bbox_geom)) as overlap 
            FROM dimGroundTruth AS dg
            JOIN dimTiles AS dt
            ON ST_Intersects(dg.geom, dt.bbox_geom)
            WHERE dt.row = %s
            """,
        'reinst_constraints_factTilesGroundTruth':
            """
            ALTER TABLE factTilesGroundTruth
            ADD CONSTRAINT factTilesGroundTruth_pkey PRIMARY KEY(intersect_id), 
            ADD CONSTRAINT factTilesGroundTruth_element_id_fkey FOREIGN KEY(element_id) REFERENCES dimGroundTruth(element_id),
            ADD CONSTRAINT factTilesGroundTruth_tile_id_fkey FOREIGN KEY(tile_id) REFERENCES dimTiles(tile_id)
            """,
        'insert_characteristics_dimTiles':
            """
            UPDATE dimTiles
            SET
            count_0 = %s,
            count_1 = %s,
            count_2 = %s,
            entropy = %s,
            psnr = %s,
            avg_diff = %s,
            avg_edge = %s,
            avg_grad = %s,
            contrast = %s
            WHERE tile_id = %s
            """,
        'split_tiles':
            """
            SELECT setseed(0.42);
            UPDATE dimTiles
            SET split = (ARRAY['train',
                               'train',
                               'train',
                               'train',
                               'train',
                               'train',
                               'train',
                               'train',
                               'validate',
                               'test']::split_cat[])[floor(random()*10)+1]
            """,
        'exclude_tiles': None
    },
    'hdf5': {
        'path': 'X:/Kennisbasis/Datascience/AVIVL/data/pilot_verharding_stad/storage/aerial_dataset.hdf5',
        'dset_dict': {
            'rgb': {
                'shape': [None, 3, 256, 256],
                'chunks': (1, 3, 256, 256),
                'dtype': 'uint8'
            },
            'gt': {
                'shape': [None, 1, 256, 256],
                'chunks': (1, 1, 256, 256),
                'dtype': 'uint8'
            },
            'pred': {
                'shape': [None, 1, 256, 256],
                'chunks': (1, 1, 256, 256),
                'dtype': 'uint8'
            }
        }
    },
    'downsample_for_plot': 125,
    'path_downsampled_overview': 'X:/Kennisbasis/Datascience/AVIVL/data/pilot_verharding_stad/visuals/aerial_downsampled_overview.tif'
}

# Initialize config settings for satellite as copy of aerial
config_etl_satellite = copy.deepcopy(config_etl_aerial)
# Change config settings for satellite where required
config_etl_satellite['postgres']['db_name'] = 'satellite_imagery'
config_etl_satellite['postgres']['dir_imagery'] = 'X:/Kennisbasis/Datascience/AVIVL/data/pilot_verharding_stad/storage/satellite_imagery'
config_etl_satellite['postgres']['file_format_imagery'] = 'QEpspUbnkZrdMGvBQiKb2W.tif'
# Workaround for fact that sat image does not contain top-left point of geometry of Utrecht
config_etl_satellite['postgres']['select_image_file_by_point'] = """
                                                                 SELECT file_name 
                                                                 FROM dimImageFiles 
                                                                 ORDER BY ST_Distance(bbox_geom, ST_Point(%s, %s)) ASC
                                                                 """
config_etl_satellite['hdf5']['path'] = 'X:/Kennisbasis/Datascience/AVIVL/data/pilot_verharding_stad/storage/satellite_dataset.hdf5'
config_etl_satellite['downsample_for_plot'] = 20
config_etl_satellite['path_downsampled_overview'] = 'X:/Kennisbasis/Datascience/AVIVL/data/pilot_verharding_stad/visuals/satellite_downsampled_overview.tif'
config_etl_satellite['postgres']['exclude_tiles'] = """
                                                    UPDATE dimTiles
                                                    SET split = 'excluded'
                                                    WHERE avg_edge < 0.025
                                                    """

# Module utils.py
config_utils = {
    'postgres': {
        'user': config_etl_aerial['postgres']['user'],
        'password': config_etl_aerial['postgres']['password'],
        'host': config_etl_aerial['postgres']['host']
    }
}

# Module modelling.py
config_modelling_aerial = {
    'postgres': {
        'db_name': config_etl_aerial['postgres']['db_name'], 
        'user': config_etl_aerial['postgres']['user'],
        'password': config_etl_aerial['postgres']['password'],
        'host': config_etl_aerial['postgres']['host'],
        'insert_agg_pred_data':
            """
            UPDATE dimTiles
            SET
            a0p0 = %s,
            a0p1 = %s,
            a1p0 = %s,
            a1p1 = %s,
            a2p0 = %s,
            a2p1 = %s
            WHERE tile_id = %s
            """
    },
    'hdf5': {
        'path': config_etl_aerial['hdf5']['path']
    },
    'low_int_seed': 2147483647,
    'seed': 42,
    'batch_size': 64,
    'model': {
        'encoder_name': 'resnet50',
        'pretrained': 'imagenet',
        'activation': 'softmax',
        'num_classes': 3,
        'storage_dir': 'X:/Kennisbasis/Datascience/AVIVL/data/pilot_verharding_stad/storage/aerial_models',
    },
    'training': {
        'phase_1': {
            'requires_grad': [
                'segmentation_head'
            ],
            'cross_entropy_loss_class_weights': [
                0.62553,
                0.37447,
                0
            ],
            'initial_learning_rate': 0.001,
            'scheduler': {
                'mode': 'max',
                'patience': 1,
                'threshold_mode': 'abs',
                'threshold': 0.01,
                'factor': 0.2,
                'verbose': True
            },
            'num_epochs': 3
        },
        'log_storage_dir': 'X:/Kennisbasis/Datascience/AVIVL/data/pilot_verharding_stad/storage/aerial_train_log',
        'batch_size': 64,
        'phase_2': {
            'requires_grad': [
                'segmentation_head',
                'decoder'
            ],
            'cross_entropy_loss_class_weights': [
                0.62553,
                0.37447,
                0
            ],
            'initial_learning_rate': 0.0001,
            'scheduler': {
                'mode': 'max',
                'patience': 1,
                'threshold_mode': 'abs',
                'threshold': 0.01,
                'factor': 0.2,
                'verbose': True
            },
            'num_epochs': 3
        },
        'phase_3': {
            'requires_grad': [
                'segmentation_head',
                'decoder',
                'encoder'
            ],
            'cross_entropy_loss_class_weights': [
                0.62553,
                0.37447,
                0
            ],
            'initial_learning_rate': 0.00001,
            'scheduler': {
                'mode': 'max',
                'patience': 1,
                'threshold_mode': 'abs',
                'threshold': 0.01,
                'factor': 0.2,
                'verbose': True
            },
            'num_epochs': 3
        },
        'phase_names': [
            'phase_1',
            'phase_2',
            'phase_3'
        ],
        'seed': 42
    }
}

# Initialize config settings for satellite as copy of aerial
config_modelling_satellite = copy.deepcopy(config_modelling_aerial)
config_modelling_satellite['postgres']['db_name'] = config_etl_satellite['postgres']['db_name']
config_modelling_satellite['hdf5']['path'] = config_etl_satellite['hdf5']['path']
config_modelling_satellite['model']['storage_dir'] = 'X:/Kennisbasis/Datascience/AVIVL/data/pilot_verharding_stad/storage/satellite_models'
config_modelling_satellite['training']['log_storage_dir'] = 'X:/Kennisbasis/Datascience/AVIVL/data/pilot_verharding_stad/storage/satellite_train_log'
config_modelling_satellite['training']['phase_1']['cross_entropy_loss_class_weights'] = [0.62553, 0.37447, 0]
config_modelling_satellite['training']['phase_2']['cross_entropy_loss_class_weights'] = [0.62553, 0.37447, 0]
config_modelling_satellite['training']['phase_3']['cross_entropy_loss_class_weights'] = [0.62553, 0.37447, 0]
config_modelling_satellite['training']['phase_1']['initial_learning_rate'] = 0.001
config_modelling_satellite['training']['phase_2']['initial_learning_rate'] = 0.0001
config_modelling_satellite['training']['phase_3']['initial_learning_rate'] = 0.00001
config_modelling_satellite['training']['phase_1']['num_epochs'] = 4
config_modelling_satellite['training']['phase_2']['num_epochs'] = 4
config_modelling_satellite['training']['phase_3']['num_epochs'] = 10


# Module visualisation.py
config_visualisation_aerial = {
    'postgres': {
        'db_name': config_etl_aerial['postgres']['db_name'], 
        'user': config_etl_aerial['postgres']['user'],
        'password': config_etl_aerial['postgres']['password'],
        'host': config_etl_aerial['postgres']['host'],
        'dir_geom_to_be_tiled': config_etl_aerial['postgres']['dir_geom_to_be_tiled'],
        'file_geom_to_be_tiled': config_etl_aerial['postgres']['file_geom_to_be_tiled'],
        'region_name': config_etl_aerial['postgres']['region_name']
    },
    'hdf5': {
        'path': config_etl_aerial['hdf5']['path']
    },
    'nbh': {
        'storage_dir': config_download_data['process_wijkenbuurten']['storage_dir'],
        'filename': config_download_data['process_wijkenbuurten']['file_name']
    },
    'path_downsampled_overview': config_etl_aerial['path_downsampled_overview'],
    'storage_dir': 'X:/Kennisbasis/Datascience/AVIVL/data/pilot_verharding_stad/visuals',
    'log_storage_dir': config_modelling_aerial['training']['log_storage_dir'],
    'log_files': [
        '20230405094352_phase_1_logfile.json',
        '20230405112131_phase_2_logfile.json',
        '20230405125634_phase_3_logfile.json'
    ],
    'example_tile_ids': [
        124756,
        52907,
        40958,
        170051,
        186748,
        37671,
        43300,
        9554,
        181349,
        37946,
        69177,
        46669,
        48484,
        84042,
        62228,
        180828,
        155298,
        143519,
        99455,
        207159,
        58825,
        21596,
        113476,
        154277,
        205069,
        176746,
        115952,
        20832,
        206169,
        184020,
        191653,
        96910,
        73197,
        125071,
        57459,
        34070,
        53166],
    'filenames': {
        'downsampled_overview': 'aer_rgb_ovv.jpg',
        'pixels_and_shares': 'aer_eda_pixels_and_shares.jpg',
        'training_process': 'aer_training_process.jpg',
        'cm_by_split': {
            'train': 'confusion_matrices/aer_cm_train.jpg',
            'validate': 'confusion_matrices/aer_cm_val.jpg',
            'test': 'confusion_matrices/aer_cm_test.jpg',
        },
        'cm_by_nbh': {
            'train': 'confusion_matrices/aer_cm_train_NBH.jpg',
            'validate': 'confusion_matrices/aer_cm_val_NBH.jpg',
            'test': 'confusion_matrices/aer_cm_test_NBH.jpg',
        },
        'performance_overview': 'aer_METRIC_by_neighbourhood.jpg',
        'example_tiles': 'example_tiles/aer_tile_TILE_ID.jpg',
        'bgt_object_type_count': 'aer_object_type_count.jpg',
        'rgb_and_contrast': 'aer_rgb_and_contrast.jpg',
        'rgb_and_excluded': 'aer_rgb_and_excluded.jpg'
    }
}


# Initialize config settings for satellite as copy of aerial
config_visualisation_satellite = copy.deepcopy(config_visualisation_aerial)
config_visualisation_satellite['postgres']['db_name'] = config_etl_satellite['postgres']['db_name']
config_visualisation_satellite['hdf5']['path'] = config_etl_satellite['hdf5']['path']
config_visualisation_satellite['path_downsampled_overview'] = config_etl_satellite['path_downsampled_overview']
config_visualisation_satellite['log_storage_dir'] = config_modelling_satellite['training']['log_storage_dir']
config_visualisation_satellite['log_files'] = [
    '20230405091957_phase_1_logfile.json',
    '20230405092317_phase_2_logfile.json',
    '20230405092621_phase_3_logfile.json'
]
config_visualisation_satellite['filenames']['downsampled_overview'] = 'sat_rgb_ovv.jpg'
config_visualisation_satellite['filenames']['pixels_and_shares'] = 'sat_eda_pixels_and_shares.jpg'
config_visualisation_satellite['filenames']['training_process'] = 'sat_training_process.jpg'
config_visualisation_satellite['filenames']['cm_by_split'] = {
    'train': 'confusion_matrices/sat_cm_train.jpg',
    'validate': 'confusion_matrices/sat_cm_val.jpg',
    'test': 'confusion_matrices/sat_cm_test.jpg'
}
config_visualisation_satellite['filenames']['cm_by_nbh'] = {
    'train': 'confusion_matrices/sat_cm_train_NBH.jpg',
    'validate': 'confusion_matrices/sat_cm_val_NBH.jpg',
    'test': 'confusion_matrices/sat_cm_test_NBH.jpg'
}
config_visualisation_satellite['filenames']['performance_overview'] = 'sat_METRIC_by_neighbourhood.jpg'
config_visualisation_satellite['example_tile_ids'] = [
    5900,
    5987,
    4458,
    6113,
    1782,
    4005,
    4461,
    2582,
    488,
    3126,
    2362,
    2347,
    4810,
    1615,
    3792,
    1031,
    2702,
    5001,
    106,
    5786,
    5938,
    2078,
    3064]
config_visualisation_satellite['filenames']['example_tiles'] = 'example_tiles/sat_tile_TILE_ID.jpg'
config_visualisation_satellite['filenames']['bgt_object_type_count'] = 'sat_object_type_count.jpg'
config_visualisation_satellite['filenames']['rgb_and_contrast'] = 'sat_rgb_and_contrast.jpg'
config_visualisation_satellite['filenames']['rgb_and_excluded'] = 'sat_rgb_and_excluded.jpg'
