import zarr
import h5py
import numpy as np
from concurrent.futures import ProcessPoolExecutor, as_completed
from itertools import islice
from dotenv import load_dotenv
from pathlib import Path
from os import environ, walk

# Load environmental variables
load_dotenv()


def chunk_task_list_two(task_list, chunk_size=10):
    chunk = []
    while task_list:
        chunk.append(task_list.pop())
        if len(chunk) == chunk_size:
            yield chunk
            chunk = []


def chunk_task_list(task_list, chunk=10):
    it = iter(task_list)
    while True:
        piece = list(islice(it, chunk))
        if piece:
            yield piece
        else:
            return


def create_zarr_root(storage_path):

    store = zarr.DirectoryStore(storage_path)
    root = zarr.group(store=store, overwrite=True)
    root.create_group("Base Data")
    root.create_group("Processed Data")
    root.create_group("Visualization Data")

    return root


# Transfer VNP data from a tile to a zarr array
def transfer_vnp_to_zarr(zarr_arr, base_path, tile_file_name):

    # Open the file
    h5file = h5py.File(Path(base_path, tile_file_name))
    # Split the file name
    split_name = tile_file_name.split('_')
    # Assign the split sections
    tile = split_name[1]
    # Get the tile h/v
    tile_h = int(tile[1:3])
    tile_v = int(tile[4:])
    # Get the global row/col indices for the top-left corner of the tile
    tile_row_ul = tile_v * 2400
    tile_col_ul = tile_h * 2400
    # Make numpy arrays of NTL data, Quality flags, and snow flags
    ntl_arr = np.array(h5file['HDFEOS']['GRIDS']['VIIRS_Grid_DNB_2d']['Data Fields'][
                            f'{data_type}_Composite_Snow_Free'])
    qa_arr = np.array(h5file['HDFEOS']['GRIDS']['VIIRS_Grid_DNB_2d']['Data Fields'][
                            f'{data_type}_Composite_Snow_Free_Quality'])
    # Make a filtered array
    filtered_array = np.where(ntl_arr != 65535 and qa_arr != 255 and qa_arr != 2, ntl_arr, np.nan)
    # Write the filtered array to the zarr file
    zarr_arr[tile_row_ul : tile_row_ul + 2399][tile_col_ul : tile_col_ul + 2399] = filtered_array
    # Print message
    print(f"Finished writing {tile_file_name} to zarr array.")


# Add VNP data to a zarr store as a base for future emissions comparisons
def add_vnp_base(zarr_obj,
                 vnp_product,
                 vnp_year,
                 vnp_doy=None):

    # If not Day of Year was not specified
    if vnp_doy is None:
        # If it's VNP46A3
        if vnp_product == "VNP46A3":
            # Print a warning
            print(f"Warning: VNP46A3 data submitted, but no DoY specified. Proceeding with 001 as DoY.")
            # Set DoY
            vnp_doy = "001"
        # Otherwise, if it's VNP46A4
        elif vnp_product == "VNP46A4":
            # Set DoY
            vnp_doy = "001"

    # If the zarr file does not have a group for the base data product yet
    if vnp_product not in zarr_obj['Base Data'].keys():
        # Add a group
        zarr_obj['Base Data'].create_group(vnp_product)

    # If the zarr file does not have a group for the year yet
    if vnp_year not in zarr_obj[f'Base Data/{vnp_product}'].keys():
        # Add a group
        zarr_obj['Base Data'][vnp_product][vnp_year] = zarr.group()

    # If the zarr file does not have a group for the doy yet
    if vnp_doy not in zarr_obj['Base Data'][vnp_product][vnp_year].keys():
        # Add a group
        zarr_obj['Base Data'][vnp_product][vnp_year][vnp_doy] = zarr.group()

    # Create a dataset to receive the file
    zarr_obj['Base Data'][vnp_product][vnp_year][vnp_doy].create_dataset("AllAngle_Composite_Snow_Free",
                                                                          shape=(43200, 86400),
                                                                          chunks=(1200, 1200),
                                                                          dtype="uint16")

    # Reference the dataset
    zarr_arr = zarr_obj['/Base Data'][vnp_product][vnp_year][vnp_doy]["AllAngle_Composite_Snow_Free"]

    # List for VNP files
    vnp_file_list = []

    # Base path for the VNP input data
    base_path = Path(environ["inputs_path"], "VNP", vnp_product)

    # Walk the VNP files
    for root, dirs, files in walk(base_path):
        # For each file
        for file in files:
            # Split the file name
            split_name = file.split('_')
            # Assign the split sections
            product = split_name[0]
            tile = split_name[1]
            year = split_name[2]
            year = year.split('.')[0]
            # If it's VNP46A4
            if data_product == "VNP46A4":
                # Set doy to 1
                doy = "001"
            # Otherwise
            else:
                # Split it from the file name
                doy = split_name[3]
                doy = doy.split('.')[0]
            # If the file is from the right year, doy, and the tile is in the worldpop data
            if vnp_year == year and vnp_doy == doy and tile in wp_tiles:
                # Add the file to the list
                vnp_file_list.append(file)

    # Start a Process Executor
    with ProcessPoolExecutor(max_workers=4) as executor:
        for vnp_chunk in chunk_task_list(vnp_file_list):
            future_events = [executor.submit(transfer_vnp_to_zarr,
                                             zarr_arr,
                                             base_path,
                                             vnp_file) for vnp_file in vnp_chunk]


def specify_datasets(zarr_obj, timeseries_days, pixel_hs, pixel_vs):

    zarr_obj.create_dataset("Pixel V",
                            data=pixel_vs,
                            shape=(1, len(pixel_vs)),
                            dtype="float64")