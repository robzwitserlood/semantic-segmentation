# pilot-verharding-stad

Semantic segmentation of urban surface hardening (pavement vs. greenery) in Utrecht, Netherlands. Classifies aerial and satellite imagery at pixel level into three classes using a U-Net model with ResNet50 encoder. Ground truth is derived from the Dutch BGT topographic registry.

## Labels

| ID | Name | Examples |
| --- | --- | --- |
| 0 | `impervious` | roads, buildings, tunnels |
| 1 | `pervious` | vegetation, water, sand |
| 2 | `unknown` | half-paved, transitional surfaces (excluded from evaluation) |

## Architecture

| Module | Purpose |
| --- | --- |
| `src/config.py` | All configuration (paths, DB credentials, model hyperparameters) |
| `src/download_data.py` | Downloads BGT, municipality boundaries, and imagery from PDOK |
| `src/spatial_scope.py` | Tiled bounding box logic |
| `src/etl.py` | Tiles rasters, rasterizes BGT ground truth, stores metadata in PostGIS and arrays in HDF5 |
| `src/etl_bgt.py` | BGT-specific ETL — downloads, transforms, and labels BGT geometries |
| `src/dataset.py` | `DataSetCreator` (tile .nc files + parquet metadata), `ExploratoryDataAnalysis`, `DataSetSplitter` |
| `src/modelling.py` | U-Net training with 3-phase progressive unfreezing |
| `src/evaluation.py` | `PerformanceEvaluation` — confusion matrices, per-neighbourhood analysis, tile plots |
| `src/visualisation.py` | Confusion matrices, training curves, per-neighbourhood performance (aerial pipeline) |
| `src/utils.py` | Shared database connection helpers |

`pipeline.ipynb` orchestrates the full pipeline interactively with a single `IMAGERY_MODE` toggle at the top (`'aerial'` or `'satellite'`).

## Prerequisites

The following must be installed on the machine before running `uv sync` or the pipeline.

### 1. NVIDIA driver and CUDA toolkit

Install the NVIDIA driver and CUDA 12.1 toolkit:

```bash
# Add NVIDIA package repository
wget https://developer.download.nvidia.com/compute/cuda/repos/ubuntu2204/x86_64/cuda-keyring_1.1-1_all.deb
sudo dpkg -i cuda-keyring_1.1-1_all.deb
sudo apt update

# libtinfo5 was dropped in Ubuntu 22.04; Nsight Systems (bundled in cuda-toolkit-12-1) requires it
wget http://archive.ubuntu.com/ubuntu/pool/universe/n/ncurses/libtinfo5_6.3-2ubuntu0.1_amd64.deb
sudo dpkg -i libtinfo5_6.3-2ubuntu0.1_amd64.deb

# Install CUDA toolkit (includes driver)
sudo apt install -y cuda-toolkit-12-1
```

Verify the installation:

```bash
nvidia-smi   # "CUDA Version" in the top-right should be >= 12.1
nvcc --version
```

If your driver only supports CUDA < 12.1, change `pytorch-cu121` to `pytorch-cu118` in both places in `pyproject.toml` before running `uv sync`.

### 2. Docker

```bash
sudo apt update
sudo apt install -y ca-certificates curl gnupg
sudo install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list
sudo apt update
sudo apt install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin

# Allow running Docker without sudo (log out and back in after this)
sudo usermod -aG docker $USER
```

### 3. uv

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
source $HOME/.local/bin/env   # or restart your shell
```

## Getting started

### 1. Start the database

```bash
docker compose up -d
```

This starts a single PostgreSQL 16 + PostGIS container on port 5432. Both databases (`aerial_imagery` and `satellite_imagery`) are created automatically on the first run.

Check that it is healthy:

```bash
docker compose ps
```

### 2. Install Python dependencies

```bash
uv sync
```

This creates a virtual environment at `.venv/` and installs all packages including PyTorch with CUDA support.

### 3. Activate the environment

```bash
source .venv/bin/activate
```

## Running the pipeline

Each script accepts `aerial` or `satellite` as a positional argument to select the corresponding configuration:

```bash
# Download raw data (BGT, municipality boundary, aerial imagery)
python src/download_data.py aerial

# Run ETL: tile imagery, rasterize ground truth, populate PostGIS and HDF5
python src/etl.py aerial

# Train the model
python src/modelling.py aerial

# Evaluate
python src/evaluation.py aerial

# Generate visualisations
python src/visualisation.py aerial
```

Replace `aerial` with `satellite` for the satellite imagery pipeline.

Alternatively, run the full pipeline interactively through `pipeline.ipynb`. Set `IMAGERY_MODE = 'aerial'` or `'satellite'` in the first cell to switch between pipeline branches.

## Storage layout

**Script pipeline (aerial / satellite CLI):**

```text
./data/raw/                  BGT extract, municipality boundaries, neighbourhoods
./data/raw/aerial_imagery/   Aerial GeoTIFF tiles (downloaded)
./data/raw/satellite_imagery/Satellite GeoTIFF
./data/processed/            HDF5 datasets (aerial_dataset.hdf5, satellite_dataset.hdf5)
./artifacts/aerial_models/   Saved model checkpoints
./artifacts/satellite_models/
./logs/aerial_train_log/     Training logs (JSON)
./logs/satellite_train_log/
./visuals/                   Output plots and confusion matrices
```

**Notebook pipeline (pipeline.ipynb):**

```text
storage/spatial_scope/       Municipality boundary GML
storage/bgt/                 Downloaded and labelled BGT geometries (parquet)
storage/dataset/             Per-tile .nc files and parquet metadata
storage/predictions/         Per-tile prediction .nc files and aggregated parquet
storage/models/              Saved model checkpoints
storage/train_log/           Training logs (JSON)
```

PostgreSQL databases `aerial_imagery` and `satellite_imagery` store tile metadata and spatial indices (managed by PostGIS).

## Model

- **Architecture**: U-Net (`segmentation-models-pytorch`) with ResNet50 encoder, ImageNet pretrained weights, softmax activation, 3 output classes
- **Training**: 3-phase progressive unfreezing — phase 1: segmentation head only; phase 2: head + decoder; phase 3: full model
- **Optimizer**: Adam with `ReduceLROnPlateau` (patience=1, factor=0.2)
- **Loss**: weighted CrossEntropyLoss (class 2 weight = 0)
- **Metric**: macro F1 on classes 0 and 1
