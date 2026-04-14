# CLAUDE.md

## Project

Semantic segmentation of urban surface hardening (pavement vs. greenery) in Utrecht, Netherlands. Classifies aerial/satellite imagery pixels into three classes using a U-Net model with ground truth derived from the Dutch BGT topographic registry.

## Architecture

| Module | Purpose |
|---|---|
| `src/config.py` | All configuration. Separate dicts for aerial and satellite (satellite config is a deep copy of aerial with overrides). Never hardcode paths or parameters outside this file. |
| `src/download_data.py` | Downloads raw data and store locally in `./data/raw`: BGT (PDOK API), municipality boundaries (WFS), aerial imagery (GeoTIFF tiles) |
| `src/spatial_scope.py` | Tiled bounding box logic (`NlRegionToGeom`, `TiledBbox` classes) |
| `src/etl.py` | ETL for aerial imagery: tiles rasters, rasterizes BGT ground truth, stores metadata in PostGIS and arrays in HDF5 locally in `./data/processed` |
| `src/etl_bgt.py` | BGT-specific ETL — downloads, transforms, and labels BGT geometries; `process(geo_filter, crs, config)` takes `config_etl_bgt` from `src/config.py` |
| `src/dataset.py` | `DataSetCreator` (tile .nc files + parquet metadata), `ExploratoryDataAnalysis`, `DataSetSplitter` |
| `src/modelling.py` | U-Net training: `VerhardingDataset`, `phased_model_training`, `write_predictions`; set `modelling.PARAMS` directly in notebooks to bypass argparse |
| `src/evaluation.py` | `PerformanceEvaluation` — takes explicit path params (not a config dict); `recall_pervious` / `recall_impervious` metric names |
| `src/visualisation.py` | Confusion matrices, training curves, per-neighbourhood performance plots |
| `src/utils.py` | Shared DB connection helpers |

`pipeline.ipynb` orchestrates the full pipeline interactively. Set `IMAGERY_MODE = 'aerial'` or `'satellite'` in the first cell to switch branches.

## Labels

| ID | Name | Examples |
|---|---|---|
| 0 | `impervious` | roads, buildings, tunnels |
| 1 | `pervious` | vegetation, water, sand |
| 2 | `unknown` | half-paved, transitional (excluded from F1) |

## Model

- Architecture: U-Net (`segmentation_models_pytorch`) with ResNet50 encoder, ImageNet pretrained weights, softmax activation, 3 output classes
- Training: 3-phase progressive unfreezing — phase 1: segmentation head only, phase 2: head + decoder, phase 3: full model
- Optimizer: Adam with `ReduceLROnPlateau` scheduler (patience=1, factor=0.2)
- Loss: weighted CrossEntropyLoss (class 2 weight=0, only classes 0 and 1 matter)
- Metric: macro F1 on classes 0 and 1 (class 2 is ignored)
- Entry point: `python src/modelling.py aerial` or `python src/modelling.py satellite`

## Storage

- **PostgreSQL/PostGIS**: tile metadata, spatial queries, split assignments. Databases: `aerial_imagery` and `satellite_imagery`
- **HDF5**: image arrays — datasets `rgb` (uint8, C×H×W), `gt` (uint8, ground truth), `pred` (uint8, predictions), shape `[N, channels, 256, 256]`; stored in `./data/processed/aerial_dataset.hdf5` and `./data/processed/satellite_dataset.hdf5`
- **Raw data (local)**: `./data/raw/` — BGT extract, municipality boundaries, neighbourhoods; `./data/raw/aerial_imagery/` — aerial GeoTIFF tiles; `./data/raw/satellite_imagery/` — satellite GeoTIFF
- **Model artifacts**: `./artifacts/aerial_models/` and `./artifacts/satellite_models/`
- **Training logs**: `./logs/aerial_train_log/` and `./logs/satellite_train_log/`
- **Visualisations**: `./visuals/`
- **Notebook pipeline** (`pipeline.ipynb`): `storage/bgt/` — BGT parquet; `storage/dataset/` — tile .nc files + parquet metadata; `storage/predictions/` — prediction .nc files; `storage/models/`; `storage/train_log/`
- Tile size: 256×256 pixels

## Config pattern

Scripts select their config via a CLI argument:

```bash
python src/etl.py aerial      # uses config_etl_aerial
python src/etl.py satellite   # uses config_etl_satellite
```

`config_etl_satellite` / `config_modelling_satellite` are deep copies of the aerial configs with specific fields overridden. Follow this same pattern when adding new configuration.

`pipeline.ipynb` imports configs directly and selects the active one via `IMAGERY_MODE`:
```python
from config import config_etl_aerial, config_etl_satellite, config_etl_bgt, ...
config_etl = config_etl_satellite if IMAGERY_MODE == 'satellite' else config_etl_aerial
```

`config_etl_bgt` is a derived dict (built from `config_download_data` and `config_etl_aerial`) passed to `etl_bgt.process()`. `config_etl_satellite['dataset']` and `config_etl_satellite['predictions']` hold the notebook-pipeline storage paths.

## Environment

uv project (Python 3.11+). Install dependencies with `uv sync`. PyTorch CUDA wheels are pulled from the PyTorch index configured in `pyproject.toml` (currently cu121 — verify your driver supports it with `nvidia-smi`). The project runs on WSL2 but data lives on a Windows network share (`X:`).

PostgreSQL with PostGIS must be available separately. Start it with `docker compose up -d` (see `docker-compose.yml`) or install via `sudo apt install postgresql postgis`.

## Conventions

- All file paths are constructed with `pathlib.Path` — never use string concatenation for paths
- CRS for Dutch spatial data is RD New (EPSG:28992)
- BGT feature type names use Dutch (`wegdeel`, `pand`, `begroeidterreindeel`, etc.)
- Label names use English (`impervious`, `pervious`, `unknown`)
- Data split categories: `train`, `validate`, `test`, `excluded` (PostgreSQL ENUM `split_cat`)
