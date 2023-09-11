import t_spinup
import zarr
import c_vnp_zarr
import matplotlib as mpl
import numpy as np
from matplotlib import pyplot as plt
from pathlib import Path
from os import environ

# Set matplotlib defaults for fonts
mpl.rc('font', family='Times New Roman')
mpl.rc('axes', labelsize=14)
mpl.rc('xtick', labelsize=12)
mpl.rc('ytick', labelsize=12)

arr = np.array()


# Assemble Directory Store path
dir_path = Path(environ['outputs_dir'], 'puerto_rico_vnp46')
# Get Zarr object
curr_zarr = c_vnp_zarr.VNPZarr(dir_path)

for tile in curr_zarr.tiles.items():

    #tile[1].plot_pixel_timeseries(120, 200)
    tile[1].plot_pixel_angle_effects(120, 200)



