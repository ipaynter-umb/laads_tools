import c_laads
import datetime
import logging
from pathlib import Path
from os import environ

# Name for the dataset. Must be a string, must be locally unique.
dataset_name = "fiona"

# Set up the logging config
logging.basicConfig(filename=Path(environ['logs_dir'], f'{dataset_name}_{datetime.datetime.now():%Y%m%d%H%M%S}.log'),
                    filemode='w',
                    format=' %(levelname)s - %(asctime)s - %(message)s',
                    level=logging.DEBUG)

dataset = c_laads.LAADSDataSet(dataset_name)

dataset.print_download_report()