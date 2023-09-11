import t_spinup
import zarr
import datetime
import numpy as np
import re
import t_vnp_zarr


class VNPZarr:

    def __init__(self, path):

        self.path = path
        self.root = zarr.group(store=zarr.DirectoryStore(str(self.path)), overwrite=False)
        self.dates = np.empty(self.root[f'Date'].shape, dtype=object)
        self.tiles = {}

        self.spinup()

    def spinup(self):

        for root_key in self.root.keys():
            if root_key == 'Date':
                for date_ind, datestr in enumerate(self.root[f'Date']):
                    self.dates[date_ind] = datetime.datetime.strptime(datestr, '%Y%m%d')
            else:
                new_tile = VNPTile(self, root_key)
                self.tiles[new_tile.name] = new_tile


class VNPTile:

    def __init__(self, parent, name):

        self.zarr = parent
        self.name = name
        self.min_row = None
        self.min_col = None

        self.spinup()

    def spinup(self):

        if 'Min Row' in self.zarr.root[self.name].attrs.keys():
            self.min_row = self.zarr.root[self.name].attrs['Min Row']
        if 'Min Col' in self.zarr.root[self.name].attrs.keys():
            self.min_col = self.zarr.root[self.name].attrs['Min Col']

    def get_local_row_col(self, row=None, col=None):

        if row:
            row -= self.min_row
        if col:
            col -= self.min_col

        return row, col

    def get_pixel_azimuth_angles(self, row, col, local_row_col=True):

        if not local_row_col:
            row, col = self.get_local_row_col(row, col)

        return self.zarr.root[f'{self.name}/{row}/Sensor_Azimuth'][:, col]

    def get_pixel_zenith_angles(self, row, col, local_row_col=True):

        if not local_row_col:
            row, col = self.get_local_row_col(row, col)

        return self.zarr.root[f'{self.name}/{row}/Sensor_Zenith'][:, col]

    def get_pixel_ntls(self, row, col, local_row_col=True):

        if not local_row_col:
            row, col = self.get_local_row_col(row, col)

        return self.zarr.root[f'{self.name}/{row}/DNB_BRDF-Corrected_NTL'][:, col]

    def get_timeseries_dates(self):

        return self.zarr.dates

    def plot_pixel_timeseries(self, row, col, local_row_col=True):

        if not local_row_col:
            row, col = self.get_local_row_col(row, col)

        t_vnp_zarr.visualize_pr_pixel_hurricanes(self.get_timeseries_dates(), self.get_pixel_ntls(row, col))

    def plot_pixel_angle_effects(self, row, col, local_row_col=True):

        if not local_row_col:
            row, col = self.get_local_row_col(row, col)

        t_vnp_zarr.visualize_pr_pixel_angles(
            self.get_pixel_ntls(row, col),
            self.get_pixel_azimuth_angles(row, col),
            self.get_pixel_zenith_angles(row, col)
        )