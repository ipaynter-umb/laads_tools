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

    # Row and col mins and maxes
    row_min = 300
    row_max = 520
    col_min = 480
    col_max = 1160

    # Start the clock
    stime = time()

    # Split the components of the work packet
    name_dict = work_packet[0]
    a2_name = name_dict['VNP46A2']
    a1_name = name_dict['VNP46A1']
    a1_path = work_packet[1][0]
    a2_path = work_packet[1][1]
    store_path = work_packet[2]

    # Reference the DirectoryStore
    store = zarr.DirectoryStore(str(store_path))
    # Reference the root group
    zarr_root = zarr.group(store=store, overwrite=False)

    # Get components of the VNP46A2 filename
    date_obj, tilename = t_vnp46a.get_components_from_filename(a2_name,
                                                               date_obj=True,
                                                               tilename=True)
    # Create a group for the tile if one does not exist
    zarr_root.require_group(tilename, overwrite=False)
    zarr_root[tilename].attrs['Min Row'] = row_min
    zarr_root[tilename].attrs['Min Col'] = col_min

    # Open the H5 files
    a2_h5file = h5py.File(Path(a2_path, a2_name))
    a1_h5file = h5py.File(Path(a1_path, a1_name))
    # Get the array of the nighttime lights
    a2_ntl_array = np.array(a2_h5file['HDFEOS']['GRIDS']['VNP_Grid_DNB']['Data Fields'][
                                'DNB_BRDF-Corrected_NTL'][row_min:row_max, col_min:col_max])
    a2_qf_array = np.array(a2_h5file['HDFEOS']['GRIDS']['VNP_Grid_DNB']['Data Fields'][
                                'Mandatory_Quality_Flag'][row_min:row_max, col_min:col_max])
    a2_cloudqf_array = np.array(a2_h5file['HDFEOS']['GRIDS']['VNP_Grid_DNB']['Data Fields'][
                                'QF_Cloud_Mask'][row_min:row_max, col_min:col_max])
    a1_zenangle_array = np.array(a1_h5file['HDFEOS']['GRIDS']['VNP_Grid_DNB']['Data Fields'][
                               'Sensor_Zenith'][row_min:row_max, col_min:col_max])
    a1_azangle_array = np.array(a1_h5file['HDFEOS']['GRIDS']['VNP_Grid_DNB']['Data Fields'][
                                     'Sensor_Azimuth'][row_min:row_max, col_min:col_max])

    logging.info(f'Opened NTL {a2_name} in {np.around(time() - stime, decimals=2)} seconds.')
    # Checkpoint time
    ctime = time()

    # For each row in the ntl_array
    for row_ind, row in enumerate(a2_ntl_array):
        # Create a group for the row (if it does not exist)
        zarr_root[tilename].require_group(row_ind,
                                          overwrite=False)
        # Create datasets for the row (if they do not exist)
        zarr_root[tilename][row_ind].require_dataset('DNB_BRDF-Corrected_NTL',
                                                     overwrite=False,
                                                     shape=(zarr_root['Date'].shape[0], a2_ntl_array.shape[1]),
                                                     chunks=(zarr_root['Date'].shape[0], 10),
                                                     dtype='uint16',
                                                     fill_value=65535,
                                                     write_empty_chunks=False
                                                     )
        zarr_root[tilename][row_ind].require_dataset('QF_Cloud_Mask',
                                                     overwrite=False,
                                                     shape=(zarr_root['Date'].shape[0], a2_ntl_array.shape[1]),
                                                     chunks=(zarr_root['Date'].shape[0], 10),
                                                     dtype='uint16',
                                                     fill_value=65535,
                                                     write_empty_chunks=False
                                                     )
        zarr_root[tilename][row_ind].require_dataset('Mandatory_Quality_Flag',
                                                     overwrite=False,
                                                     shape=(zarr_root['Date'].shape[0], a2_ntl_array.shape[1]),
                                                     chunks=(zarr_root['Date'].shape[0], 10),
                                                     dtype='uint8',
                                                     fill_value=255,
                                                     write_empty_chunks=False
                                                     )
        zarr_root[tilename][row_ind].require_dataset('Sensor_Zenith',
                                                     overwrite=False,
                                                     shape=(zarr_root['Date'].shape[0], a2_ntl_array.shape[1]),
                                                     chunks=(zarr_root['Date'].shape[0], 10),
                                                     dtype='int16',
                                                     fill_value=-32768,
                                                     write_empty_chunks=False
                                                     )
        zarr_root[tilename][row_ind].require_dataset('Sensor_Azimuth',
                                                     overwrite=False,
                                                     shape=(zarr_root['Date'].shape[0], a2_ntl_array.shape[1]),
                                                     chunks=(zarr_root['Date'].shape[0], 10),
                                                     dtype='int16',
                                                     fill_value=-32768,
                                                     write_empty_chunks=False
                                                     )
        # For each ntl value in the row
        for col_ind, ntl in enumerate(row):
            # If the NTL value is not a fill
            if ntl != 65535:
                # Get the array index for the date
                arr_ind = list(zarr_root['Date']).index(date_obj.strftime('%Y%m%d'))
                zarr_root[f'{tilename}/{row_ind}/DNB_BRDF-Corrected_NTL'][arr_ind, col_ind] = ntl
                zarr_root[f'{tilename}/{row_ind}/QF_Cloud_Mask'][arr_ind, col_ind] = a2_cloudqf_array[row_ind, col_ind]
                zarr_root[f'{tilename}/{row_ind}/Mandatory_Quality_Flag'][arr_ind, col_ind] = a2_qf_array[
                    row_ind, col_ind]
                zarr_root[f'{tilename}/{row_ind}/Sensor_Zenith'][arr_ind, col_ind] = a1_zenangle_array[
                    row_ind, col_ind]
                zarr_root[f'{tilename}/{row_ind}/Sensor_Azimuth'][arr_ind, col_ind] = a1_azangle_array[
                    row_ind, col_ind]
        #logging.info(f'Finished row {row_ind} for {a2_name} in {np.around(time() - stime, decimals=2)} seconds.')

    logging.info(f'Transferred NTL from {a2_name} in {np.around(time() - ctime, decimals=2)} seconds.')


# Multiprocessing function
def multiprocess_vnp_to_zarr(mp_func, work, max_workers=3):

    with ProcessPoolExecutor(max_workers=max_workers) as func_exec:

        func_exec.map(mp_func, work)


# Setup vnp to zarr multiprocessing
def vnp_to_zarr_mp_setup(a1_path, a2_path, output_path, zarr_name):
    # List of work
    list_of_work = []
    # Dict of files
    dict_of_files = {}
    # Start and end dates
    start_date = None
    end_date = None
    # Count files
    file_count = 0
    # Walk the support file directory
    for root, dirs, files in walk(a2_path):
        # For each file name
        for name in files:
            # If it looks like a VNP46A2 product file
            if 'VNP46A2' in name:
                # Get components of the filename
                date_obj = t_vnp46a.get_components_from_filename(name,
                                                                 date_obj=True)
                # If the date is not in the dictionary yet
                if date_obj not in dict_of_files.keys():
                    dict_of_files[date_obj] = {}
                dict_of_files[date_obj]['VNP46A2'] = name
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
    # Walk the support file directory
    for root, dirs, files in walk(a1_path):
        # For each file name
        for name in files:
            # If it looks like a VNP46A1 product file
            if 'VNP46A1' in name:
                # Get components of the filename
                date_obj = t_vnp46a.get_components_from_filename(name,
                                                                 date_obj=True)
                # If the date is in the dictionary
                if date_obj in dict_of_files.keys():
                    dict_of_files[date_obj]['VNP46A1'] = name
    # Walk the dictionary of files
    for dict_date in dict_of_files.keys():
        if len(dict_of_files[dict_date]) == 2:
            # Add file names to work
            list_of_work.append([dict_of_files[dict_date], [a1_path, a2_path]])
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


if __name__ == '__main__':

    zarr_name = 'puerto_rico_vnp46'

    # Set up the logging config
    logging.basicConfig(
        filename=Path(environ['logs_dir'], f'{zarr_name}_main_{datetime.now():%Y%m%d%H%M%S}.log'),
        filemode='w',
        format=' %(levelname)s - %(asctime)s - %(message)s',
        level=logging.DEBUG)

    stime = time()

    a1_inputs_path = Path(environ['inputs_dir'], 'fionaA1')
    a2_inputs_path = Path(environ['inputs_dir'], 'fiona')
    output_path = Path(environ['outputs_dir'])

    list_of_work = vnp_to_zarr_mp_setup(a1_inputs_path, a2_inputs_path, output_path, zarr_name)

    # Submit list of work to multiprocessor
    multiprocess_vnp_to_zarr(vnp_to_zarr_mp, list_of_work, max_workers=30)

    logging.info(f'Finished Multiprocessing zarr in {np.around(time() - stime, decimals=2)} seconds.')





