import time
import copy
import json
import h5py
import pathlib
import torch
import random
import argparse
import psycopg
import sqlalchemy

import numpy as np
import pandas as pd
import xarray as xr
import segmentation_models_pytorch as smp

from datetime import datetime
from tqdm import tqdm
from torch import nn
from torch.utils.data import Dataset, DataLoader
from torch.optim.lr_scheduler import ReduceLROnPlateau
from torchvision import transforms
from torchmetrics import ConfusionMatrix
from segmentation_models_pytorch.encoders import get_preprocessing_fn

from config import config_modelling_aerial, config_modelling_satellite

device = torch.device("cuda:0" if torch.cuda.is_available() else "cpu")
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
        PARAMS.update(config_modelling_satellite)
    elif args.imagery == 'aerial':
        PARAMS.update(config_modelling_aerial)


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


class VerhardingDataset(Dataset):
    """Reads in images and transforms pixel values.

    :param cur: cursor to the database
    :type cur: psycopg.extensions.cursor
    :param split: name of split (train, val or test)
    :type split: str
    :param input_transforms: transformation sequence for input
    :type input_transforms: torchvision.transforms
    :param target_transforms: transformation sequence for target
    :type target_transforms: torchvision.transforms
    """
    def __init__(self, cur, split=None, input_transforms=None,
                 target_transforms=None):
        self.f = h5py.File(PARAMS['hdf5']['path'], 'r')
        if split:
            cur.execute("""
                        SELECT tile_id FROM dimTiles
                        WHERE split = %s::split_cat
                        """, (split,))
            self.tile_ids = [i[0] for i in cur.fetchall()]
        else:
            cur.execute("""
                        SELECT tile_id FROM dimTiles
                        WHERE split != 'excluded'::split_cat
                        """)
            self.tile_ids = [i[0] for i in cur.fetchall()]
        # Get seed to make sure tansforms of input and target are aligned
        self.seed = np.random.randint(PARAMS['low_int_seed'])
        self.input_transforms = input_transforms
        self.target_transforms = target_transforms

    def __len__(self):
        return len(self.tile_ids)

    def __getitem__(self, key):
        # Get id
        tile_id = self.tile_ids[key]
        # Get inputs (x) and labels (y)
        x = self.f['rgb'][tile_id,]
        y = self.f['gt'][tile_id,]
        # Apply input transformation
        if self.input_transforms:
            # Set seed for input transformations
            random.seed(self.seed)  # apply this seed to img tranfsorms
            torch.manual_seed(self.seed)  # needed for torchvision 0.7
            x = self.input_transforms(x)
        # Apply input transformation
        if self.target_transforms:
            # Set seed for target transformations
            random.seed(self.seed)  # apply this seed to img tranfsorms
            torch.manual_seed(self.seed)  # needed for torchvision 0.7
            y = self.target_transforms(y)
                        
        return tile_id, x, y


def precision_along_dimension(cm, dim):
    """Calculate precision based on a confusion matrix
    along user specified dimension.

    :param cm: confusion matrix
    :type cm: numpy array
    :param dim: dimension to incude in calculations
    :type dim: int
    ...
    :return: precision
    :rtype: float
    """
    precision = cm[dim, dim] / np.sum(cm[:, dim])

    return precision


def recall_along_dimension(cm, dim):
    """Calculate recall based on a confusion matrix
    along user specified dimension.

    :param cm: confusion matrix
    :type cm: numpy array
    :param dim: dimension to incude in calculations
    :type dim: int
    ...
    :return: recall
    :rtype: float
    """
    recall = cm[dim, dim] / np.sum(cm[dim, :])

    return recall


def macro_f1_score(cm, dim):
    """Calculate macro f1 score based on a confusion matrix
    along user specified dimensions.

    :param cm: confusion matrix
    :type cm: numpy array
    :param dim: dimensions to incude in calculations
    :type dim: list
    ...
    :return: macro f1-score
    :rtype: float
    """
    # Initiate lists for precision and recall
    precision = list()
    recall = list()
    # Calculate precision and recall along all dimensions
    for i in dim:
        precision.append(precision_along_dimension(cm, i))
        recall.append(recall_along_dimension(cm, i))
    precision = np.array(precision)
    recall = np.array(recall)
    # Calc f1 score
    f1 = np.mean(2 * precision * recall / (precision + recall))

    return f1


def set_parameter_requires_grad(model, module_names):
    """
    """
    params_to_update = []
    for module, _ in model.named_children():
        if module in module_names:
            for param in model.get_submodule(module).parameters():
                param.requires_grad = True
                params_to_update.append(param)
        else:
            for param in model.get_submodule(module).parameters():
                param.requires_grad = False
    n_param = sum([p.numel() for p in model.parameters()])
    n_param_trainable = sum([p.numel() for p in model.parameters()
                             if p.requires_grad])
    print(f'{n_param} parameters of which {n_param_trainable} trainable')

    return model, params_to_update


def train_model(model, dataloaders, criterion, optimizer, scheduler,
                num_epochs, log_dir, model_dir, phase):
    """Train model, save best version and log training process
    """
    # Get date and time for duration measurement and unique filename
    since = time.time()
    now = datetime.now()
    # Create unique filename for model
    model_filename = now.strftime("%Y%m%d%H%M%S") + '_' + phase + '_model.pt'
    model_path = model_dir / model_filename
    # Create unique filename for logging
    log_filename = now.strftime("%Y%m%d%H%M%S") + '_' + phase + '_logfile.json'
    log_path = log_dir / log_filename
    # Write initial logfile as emtpy dict
    with open(log_path, 'w') as fp:
        json.dump(dict(), fp)
    # Initialize ConfusionMatrix instance
    confmat = ConfusionMatrix(num_classes=3).to(device)
    # Initialize list to remember performance
    val_f1_history = []
    # Initialize best version of model and best f1
    best_model_wts = copy.deepcopy(model.state_dict())
    best_f1 = 0.0
    # Train model
    for epoch in range(num_epochs):
        # Print and log
        print('Epoch {}/{}'.format(epoch, num_epochs - 1))
        print('-' * 20)
        epoch_log = dict()
        epoch_log['lr'] = float(optimizer
                                .state_dict()['param_groups'][0]['lr'])
        # Each epoch has a training and validation phase
        for phase in ['train', 'validate']:
            if phase == 'train':
                model.train()  # Set model to training mode
            else:
                model.eval()   # Set model to evaluate mode
            # Initialize running metrics for each phase in each epoch
            running_loss = 0.0
            running_confmat = torch.zeros((3, 3)).to(device)
            # Iterate over data.
            for idxs, inputs, labels in tqdm(dataloaders[phase]):
                inputs = inputs.to(device)
                labels = labels.to(device)
                # zero the parameter gradients
                optimizer.zero_grad()
                # forward
                with torch.set_grad_enabled(phase == 'train'):
                    # Get model outputs and calculate loss
                    outputs = model(inputs)
                    loss = criterion(outputs, labels)
                    _, preds = torch.max(outputs, 1)
                    # backward + optimize only if in training phase
                    if phase == 'train':
                        loss.backward()
                        optimizer.step()
                # Update running metrics
                running_loss += loss.item() * inputs.size(0)
                targets = torch.argmax(labels, dim=1)
                running_confmat += confmat(preds, targets)
            # Calculate epoch metrics
            epoch_loss = running_loss / len(dataloaders[phase].dataset)
            epoch_confmat = running_confmat / (len(dataloaders[phase].dataset)
                                               * inputs.size(2)
                                               * inputs.size(3))
            epoch_f1 = macro_f1_score(epoch_confmat.cpu().numpy(), [0, 1])
            # Log and print epoch metrics
            epoch_log[phase] = {'loss': float(epoch_loss),
                                'confmat': epoch_confmat.cpu()
                                .numpy().round(5).tolist(),
                                'f1': float(epoch_f1)}
            print('{} Loss: {:.3f}, F1: {:.3f}'.format(phase,
                                                       epoch_loss,
                                                       epoch_f1))
            print('Confusion matrix:')
            print(epoch_confmat.cpu().numpy().round(2))
            # Deep copy the model if better than best so far
            if phase == 'validate' and epoch_f1 > best_f1:
                best_f1 = epoch_f1
                best_model_wts = copy.deepcopy(model.state_dict())
                print('Best f1 score so far: saved model weights')
            if phase == 'validate':
                val_f1_history.append(epoch_f1)
        # Write logged data to file
        with open(log_path, 'r') as fp:
            log_dict = json.load(fp)
        log_dict[epoch] = epoch_log
        with open(log_path, 'w') as fp:
            json.dump(log_dict, fp)
        # Update learning rate if improvement is insufficient
        scheduler.step(epoch_f1)
        print()
    # Print info about training run
    time_elapsed = time.time() - since
    print('Training complete in {:.0f}m {:.0f}s'.format(time_elapsed // 60,
                                                        time_elapsed % 60))
    print('Best val F1: {:4f}'.format(best_f1))
    # load best model weights
    torch.save(best_model_wts, model_path)

    return model_filename


def seed_worker(worker_id):
    worker_seed = torch.initial_seed() % 2**32
    np.random.seed(worker_seed)
    random.seed(worker_seed)


def create_dataloaders(engine):
    """
    """
    preprocess_input = get_preprocessing_fn(PARAMS['model']['encoder_name'],
                                            PARAMS['model']['pretrained'])

    input_transforms = {
        'train': transforms.Compose([
            transforms.Lambda(lambda x: torch.from_numpy(x)),
            transforms.RandomHorizontalFlip(),
            transforms.Lambda(lambda x: x.type(torch.FloatTensor)),
            transforms.Normalize(preprocess_input.keywords['mean'],
                                 preprocess_input.keywords['std'])
        ]),
        'validate': transforms.Compose([
            transforms.Lambda(lambda x: torch.from_numpy(x)),
            transforms.Lambda(lambda x: x.type(torch.FloatTensor)),
            transforms.Normalize(preprocess_input.keywords['mean'],
                                 preprocess_input.keywords['std'])
        ]),
    }
    target_transforms = {
        'train': transforms.Compose([
            transforms.Lambda(lambda x: torch.from_numpy(x).long().squeeze()),
            transforms.Lambda(lambda x: nn.functional.one_hot(x, 3)),
            transforms.Lambda(lambda x: torch.permute(x, (2, 0, 1))),
            transforms.RandomHorizontalFlip(),
            transforms.Lambda(lambda x: x.type(torch.FloatTensor))
        ]),
        'validate': transforms.Compose([
            transforms.Lambda(lambda x: torch.from_numpy(x).long().squeeze()),
            transforms.Lambda(lambda x: nn.functional.one_hot(x, 3)),
            transforms.Lambda(lambda x: torch.permute(x, (2, 0, 1))),
            transforms.Lambda(lambda x: x.type(torch.FloatTensor))
        ]),
    }

    # Create training and validation datasets
    train_set = VerhardingDataset(engine,
                                  split='train',
                                  input_transforms=input_transforms['train'],
                                  target_transforms=target_transforms['train'])
    val_set = VerhardingDataset(engine,
                                split='validate',
                                input_transforms=input_transforms['validate'],
                                target_transforms=target_transforms['validate'])
    # Set seed for random data shuffle
    g = torch.Generator()
    g.manual_seed(PARAMS['seed'])
    # Create training and validation dataloaders
    train_dataloader = DataLoader(train_set,
                                  batch_size=PARAMS['batch_size'],
                                  shuffle=True,
                                  num_workers=0,
                                  worker_init_fn=seed_worker,
                                  generator=g)
    val_dataloader = DataLoader(val_set,
                                batch_size=PARAMS['batch_size'],
                                shuffle=False,
                                num_workers=0)
    dataloaders = {'train': train_dataloader,
                   'validate': val_dataloader}

    return dataloaders


def phased_model_training(engine):
    """
    """
    # Create dataloaders
    dataloaders = create_dataloaders(engine)
    # Initialize filename for best model parameters after each phase
    filename_model_parameters = None
    # Set log directory
    log_dir = pathlib.Path(PARAMS['training']['log_storage_dir'])
    model_dir = pathlib.Path(PARAMS['model']['storage_dir'])
    # Train model in phases
    for phase in PARAMS['training']['phase_names']:
        # Print info about phase
        print('Phase {}/{}'.format(phase.split('_')[-1],
                                   len(PARAMS['training']['phase_names'])))
        print('Train module(s): {}'.format(PARAMS['training'][phase]
                                           ['requires_grad']))
        print('-' * 30)
        # Load model architecture and initialize weights
        model = smp.Unet(encoder_name=PARAMS['model']['encoder_name'],
                         encoder_weights=PARAMS['model']['pretrained'],
                         classes=PARAMS['model']['num_classes'],
                         activation=PARAMS['model']['activation'])
        # Set weights to best values found in previous phase (if exists)
        if filename_model_parameters:
            path = model_dir / filename_model_parameters
            model.load_state_dict(torch.load(path))
            print('Loaded best weigts values found in previous phase')
        # Select which parameters to update in this phase
        m_to_upd = PARAMS['training'][phase]['requires_grad']
        model, params_to_update = set_parameter_requires_grad(model,
                                                              m_to_upd)
        # Send model to gpu (if available)
        model.to(device)
        # Set loss function
        weights = PARAMS['training'][phase]['cross_entropy_loss_class_weights']
        loss_fn = nn.CrossEntropyLoss(weight=torch.Tensor(weights).to(device))
        # Set optimizer
        optimizer = torch.optim.Adam(params_to_update, 
                                    lr=PARAMS['training'][phase]
                                    ['initial_learning_rate'])
        # Set learning rate scheduler
        scheduler = ReduceLROnPlateau(optimizer,
                                      **PARAMS['training'][phase]['scheduler'])
        # Train model
        filename_model_parameters = train_model(model=model,
                                                dataloaders=dataloaders,
                                                criterion=loss_fn,
                                                optimizer=optimizer,
                                                scheduler=scheduler,
                                                num_epochs=PARAMS['training']
                                                [phase]['num_epochs'],
                                                log_dir=log_dir,
                                                model_dir=model_dir,
                                                phase=phase)

    return filename_model_parameters


def write_predictions(conn, cur, filename_model_parameters):
    """Write the predictions for all tiles in the dataset
    to files and create datafile with aggregated data per file
    """
    print('Write the predictions for all tiles to disk')
    print('-' * 30)
    running_confmat = torch.zeros((3, 3)).to(device)
    # Get file to write predictions to
    f = h5py.File(PARAMS['hdf5']['path'], 'r+')
    dset = f['pred']
    # Get same preprocessing parameters as during training
    preprocess_input = get_preprocessing_fn(PARAMS['model']['encoder_name'],
                                            PARAMS['model']['pretrained'])
    # Create same input transforms as during training (except flipping)
    input_transforms = transforms.Compose([
        transforms.Lambda(lambda x: torch.from_numpy(x)),
        transforms.Lambda(lambda x: x.type(torch.FloatTensor)),
        transforms.Normalize(preprocess_input.keywords['mean'],
                             preprocess_input.keywords['std'])])
    # Create same outout transforms as during training (except flipping)
    target_transforms = transforms.Compose([
        transforms.Lambda(lambda x: torch.from_numpy(x).long().squeeze()),
        transforms.Lambda(lambda x: nn.functional.one_hot(x, 3)),
        transforms.Lambda(lambda x: torch.permute(x, (2, 0, 1))),
        transforms.Lambda(lambda x: x.type(torch.FloatTensor))])
    # Create dataset instance that contains all data in dataset
    complete_set = VerhardingDataset(cur=cur,
                                     split=None,
                                     input_transforms=input_transforms,
                                     target_transforms=target_transforms)
    # Create dataloader (please note the hardcoded batch size)
    dataloader = DataLoader(complete_set,
                            batch_size=32,
                            shuffle=False,
                            num_workers=0)
    # Load model architecture
    model = smp.Unet(encoder_name=PARAMS['model']['encoder_name'],
                     encoder_weights=PARAMS['model']['pretrained'],
                     classes=PARAMS['model']['num_classes'],
                     activation=PARAMS['model']['activation'])
    # Load parameter settings
    model_dir = pathlib.Path(PARAMS['model']['storage_dir'])
    path = model_dir / filename_model_parameters
    model.load_state_dict(torch.load(path))
    print(f'Loaded parameters stored in {filename_model_parameters}')
    # Send model to gpu (if available)
    model.to(device)
    # Set model to evaluate mode
    model.eval()
    # Initialize ConfusionMatrix instance
    confmat = ConfusionMatrix(num_classes=3).to(device)
    # Loop over batches of tiles
    for idxs, inputs, labels in tqdm(dataloader):
        inputs = inputs.to(device)
        labels = labels.to(device)
        outputs = model(inputs)
        _, preds = torch.max(outputs, 1)
        targets = torch.argmax(labels, dim=1)
        # Initialize list for aggregated data
        agg_data = list()
        # Loop over individual tiles
        for i in range(preds.size()[0]):
            # Get tile id, prediction and target
            idx = idxs[i].numpy().item()
            pred = preds[[i], :, :]
            target = targets[[i], :, :]
            # Calc confusion matrix and reshape to list
            cm = confmat(pred, target)
            running_confmat += cm
            # Select only first two columns
            cm = cm.to('cpu').numpy()[:, :2].reshape(-1)
            cm = [int(i) for i in cm]
            # Append to aggregated data list
            agg_data.append(tuple(cm + [idx]))
            # Write predictions to disk
            dset[idx, :, :, :] = pred.to('cpu').numpy()
        # Write aggregated tile data to database
        cur.executemany(PARAMS['postgres']['insert_agg_pred_data'],
                        agg_data)
        conn.commit()
    f.close()
    print(running_confmat)


def main():
    """
    """
    set_config_params()
    conn, cur = get_connection_and_cursor(PARAMS['postgres']['db_name'])
    filename_model_parameters = phased_model_training(cur)
    write_predictions(conn, cur, filename_model_parameters)


if __name__ == "__main__":
    main()
