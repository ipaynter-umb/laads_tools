import c_laads
import datetime
import logging
from pathlib import Path
from os import environ

# Specify a name for the dataset (e.g. FloridaMangroves). Must be locally unique
dataset_name = 'roundtree'

# Set up the logging config
logging.basicConfig(filename=Path(environ['logs_dir'], f'{dataset_name}_{datetime.datetime.now():%Y%m%d%H%M%S}.log'),
                    filemode='w',
                    format=' %(levelname)s - %(asctime)s - %(message)s',
                    level=logging.DEBUG)

# Create a dataset from at least a known Archive Set and Product
# Add other constraints as desired.
# If there is no catalog of data it will obtain one from LAADS.
dataset = c_laads.LAADSDataSet(dataset_name,
                               archive_set='5000',
                               product='VNP46A2',
                               start_date=datetime.date(year=2019,
                                                        month=6,
                                                        day=1),
                               end_date=datetime.date(year=2019,
                                                      month=6,
                                                      day=10),
                               include='h11v07')

# The first time you call a dataset by name it will make a specification file in laads_tools\support.
# For future calls you will only have to provide the name.
recall_dataset = c_laads.LAADSDataSet(dataset_name)

# Instruct the dataset to download its own catalog
recall_dataset.download_catalog()
