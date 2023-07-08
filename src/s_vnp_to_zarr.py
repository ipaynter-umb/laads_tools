import logging

import zarr
import h5py
import numpy as np
import t_spinup
import c_laads
import t_laads
import t_vnp46a
from pathlib import Path
from os import environ, walk, remove, mkdir
from os.path import exists
from time import time
from datetime import datetime, timedelta
from concurrent.futures import ProcessPoolExecutor


# Function for multiprocessing
def vnp_to_zarr_mp(work_packet):

    # Start the clock
    stime = time()

    # Split the components of the work packet
    name = work_packet[0]
    dir_path = work_packet[1]
    store_path = work_packet[2]

    # Reference the DirectoryStore
    store = zarr.DirectoryStore(str(store_path))
    # Reference the root group
    zarr_root = zarr.group(store=store, overwrite=False)

    # Get components of the filename
    date_obj, tilename = t_vnp46a.get_components_from_filename(name,
                                                               date_obj=True,
                                                               tilename=True)
    # Create a group for the tile if one does not exist
    zarr_root.require_group(tilename, overwrite=False)

    # Open the H5 file
    h5file = h5py.File(Path(dir_path, name))
    # Get the array of the nighttime lights
    ntl_array = np.array(h5file['HDFEOS']['GRIDS']['VNP_Grid_DNB']['Data Fields'][
                             'DNB_BRDF-Corrected_NTL'][300:520, 480:1160])

    logging.info(f'Opened NTL {name} in {np.around(time() - stime, decimals=2)} seconds.')
    # Checkpoint time
    ctime = time()

    # For each row in the ntl_array
    for row_ind, row in enumerate(ntl_array):
        # Create datasets for the tile (if they do not exist)
        zarr_root[tilename].require_dataset(row_ind,
                                            overwrite=False,
                                            shape=(zarr_root['Date'].shape[0], ntl_array.shape[1]),
                                            chunks=(zarr_root['Date'].shape[0], 10),
                                            dtype='uint16',
                                            fill_value=65535,
                                            write_empty_chunks=False
                                            )
        # For each ntl value in the row
        for col_ind, ntl in enumerate(row):
            # If the NTL value is not a fill
            if ntl != 65535:
                # Get the array index for the date
                arr_ind = list(zarr_root['Date']).index(date_obj.strftime('%Y%m%d'))
                zarr_root[f'{tilename}/{row_ind}'][arr_ind, col_ind] = ntl

    logging.info(f'Transferred NTL from {name} in {np.around(time() - ctime, decimals=2)} seconds.')


# Multiprocessing function
def multiprocess_vnp_to_zarr(mp_func, work, max_workers=3):

    with ProcessPoolExecutor(max_workers=max_workers) as func_exec:

        func_exec.map(mp_func, work)


# Setup vnp to zarr multiprocessing
def vnp_to_zarr_mp_setup(dir_path, output_path, zarr_name):
    # List of work
    list_of_work = []
    # Start and end dates
    start_date = None
    end_date = None
    # Count files
    file_count = 0
    # Walk the support file directory
    for root, dirs, files in walk(dir_path):
        # For each file name
        for name in files:
            # If it looks like a product file
            if 'VNP46A' in name:
                # Get components of the filename
                date_obj = t_vnp46a.get_components_from_filename(name,
                                                                 date_obj=True)
                # Update start and end dates
                if not start_date:
                    start_date = date_obj
                elif date_obj < start_date:
                    start_date = date_obj
                if not end_date:
                    end_date = date_obj
                elif date_obj > end_date:
                    end_date = date_obj
                # Increment file count
                file_count += 1
                # Add file name to work
                list_of_work.append([name, dir_path])
    stime = time()
    # Assemble Directory Store path
    store_path = Path(output_path, zarr_name)
    # Reference the DirectoryStore
    store = zarr.DirectoryStore(str(store_path))
    # Reference the root group
    zarr_root = zarr.group(store=store, overwrite=False)
    # # Number of days in timeseries
    day_count = (end_date - start_date).days + 1
    # # Create a dataset for the dates
    zarr_root.require_dataset('Date',
                              shape=day_count,
                              chunks=day_count,
                              dtype='str')
    curr_date = start_date
    curr_ind = 0

    while curr_date <= end_date:
        # Convert to string and store
        zarr_root['Date'][curr_ind] = curr_date.strftime('%Y%m%d')
        curr_date += timedelta(days=1)
        curr_ind += 1
    logging.info(f'Initial setup complete in {np.around(time() - stime, decimals=2)} seconds.')
    # Checkpoint time
    ctime = time()

    # For each piece of work
    for work in list_of_work:
        # Append the directory store path
        work.append(store_path)

    # Return the list of work
    return list_of_work


# Create zarr store from directory of VNP46A2 files
def create_zarr_from_vnp_dir(dir_path, output_path, zarr_name):
    # Start and end dates
    start_date = None
    end_date = None
    # Count files
    file_count = 0
    # Walk the support file directory
    for root, dirs, files in walk(dir_path):
        # For each file name
        for name in files:
            # If it looks like a product file
            if 'VNP46A' in name:
                # Get components of the filename
                date_obj = t_vnp46a.get_components_from_filename(name,
                                                                 date_obj=True)
                # Update start and end dates
                if not start_date:
                    start_date = date_obj
                elif date_obj < start_date:
                    start_date = date_obj
                if not end_date:
                    end_date = date_obj
                elif date_obj > end_date:
                    end_date = date_obj
                # Increment file count
                file_count += 1
    stime = time()
    # Assemble Directory Store path
    store_path = Path(output_path, f'{zarr_name}_zarr')
    # Reference the DirectoryStore
    store = zarr.DirectoryStore(str(store_path))
    # Reference the root group
    zarr_root = zarr.group(store=store, overwrite=False)
    # # Number of days in timeseries
    day_count = (end_date - start_date).days + 1
    # # Create a dataset for the dates
    zarr_root.require_dataset('Date',
                              shape=day_count,
                              chunks=day_count,
                              dtype='str')
    curr_date = start_date
    curr_ind = 0

    while curr_date <= end_date:
        # Convert to string and store
        zarr_root['Date'][curr_ind] = curr_date.strftime('%Y%m%d')
        curr_date += timedelta(days=1)
        curr_ind += 1
    logging.info(f'Initial setup complete in {np.around(time() - stime, decimals=2)} seconds.')
    # Checkpoint time
    ctime = time()
    # Walk the support file directory
    for root, dirs, files in walk(dir_path):
        # For each file name
        for name in files:
            # If it looks like a product file
            if 'VNP46A' in name:
                # Get components of the filename
                date_obj, tilename = t_vnp46a.get_components_from_filename(name,
                                                                           date_obj=True,
                                                                           tilename=True)
                # Create a group for the tile if one does not exist
                zarr_root.require_group(tilename, overwrite=False)

                # Open the H5 file
                h5file = h5py.File(Path(dir_path, name))
                # Get the array of the nighttime lights
                ntl_array = np.array(h5file['HDFEOS']['GRIDS']['VNP_Grid_DNB']['Data Fields'][
                                         'DNB_BRDF-Corrected_NTL'][300:520, 480:1160])

                logging.info(f'Opened NTL {name} in {np.around(time() - ctime, decimals=2)} seconds.')
                # Checkpoint time
                ctime = time()

                # For each row in the ntl_array
                for row_ind, row in enumerate(ntl_array):
                    # Create datasets for the tile (if they do not exist)
                    zarr_root[tilename].require_dataset(row_ind,
                                                        overwrite=False,
                                                        shape=(zarr_root['Date'].shape[0], ntl_array.shape[1]),
                                                        chunks=(zarr_root['Date'].shape[0], 10),
                                                        dtype='uint16',
                                                        fill_value=65535,
                                                        write_empty_chunks=False
                                                        )
                    # For each ntl value in the row
                    for col_ind, ntl in enumerate(row):
                        # If the NTL value is not a fill
                        if ntl != 65535:
                            # Get the array index for the date
                            arr_ind = list(zarr_root['Date']).index(date_obj.strftime('%Y%m%d'))
                            zarr_root[f'{tilename}/{row_ind}'][arr_ind, col_ind] = ntl

                logging.info(f'Transferred NTL from {name} in {np.around(time() - ctime, decimals=2)} seconds.')
                # Checkpoint time
                ctime = time()


if __name__ == '__main__':

    zarr_name = 'puerto_rico_vnp46a2'

    # Set up the logging config
    logging.basicConfig(
        filename=Path(environ['logs_dir'], f'{zarr_name}_main_{datetime.now():%Y%m%d%H%M%S}.log'),
        filemode='w',
        format=' %(levelname)s - %(asctime)s - %(message)s',
        level=logging.DEBUG)

    stime = time()

    dir_path = Path(environ['inputs_dir'], 'fiona')
    output_path = Path(environ['outputs_dir'])

    list_of_work = vnp_to_zarr_mp_setup(dir_path, output_path, zarr_name)

    # Submit list of work to multiprocessor
    multiprocess_vnp_to_zarr(vnp_to_zarr_mp, list_of_work, max_workers=30)

    logging.info(f'Finished Multiprocessing zarr in {np.around(time() - stime, decimals=2)} seconds.')





