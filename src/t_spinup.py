from dotenv import load_dotenv
from os import environ, mkdir
from os.path import exists
from pathlib import Path


def spinup():

    # Load environment variables from .env and .env.daacs file
    load_dotenv()
    load_dotenv('.env.secrets')
    # Establish other environment variables with relative paths
    environ['inputs_dir'] = str(Path(str(Path(__file__).parents[1]), 'inputs'))
    environ['outputs_dir'] = str(Path(str(Path(__file__).parents[1]), 'outputs'))
    environ['support_dir'] = str(Path(str(Path(__file__).parents[1]), 'support'))
    environ['logs_dir'] = str(Path(str(Path(__file__).parents[1]), 'logs'))
    # For each essential directory
    for essential_dir in [environ['inputs_dir'],
                          environ['outputs_dir'],
                          environ['support_dir'],
                          environ['logs_dir']]:
        # If the directory does not exist
        if not exists(Path(essential_dir)):
            # Make it
            mkdir(Path(essential_dir))


# If 'spinup' is not in the environment variables
if 'SPINUP' not in environ:
    # Call the spinup function
    spinup()
    # Add spinup to environmental variables
    environ['spinup'] = 'True'
