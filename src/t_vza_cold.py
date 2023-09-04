'''
A spatial buffer was applied to remove these edge pixels.
Confident/probable cloud
Cirrus cloud
Snow/ice observations
were firstly removed according to the QA flags of
the standard NASA’s Black Marble product. The cloud/snow edge pixels
removal was tested by dilating cloud/snow pixels (at 8-connected directions)
from 0 to 11 pixels to find the optimal moving window size
based on our calibration samples. The 5 × 5 pixel moving
window was selected as the optimal buffer size for masking potential
cloud/snow-influenced pixels.
'''

from pathlib import Path
from time import time
import numpy as np
import h5py


def get_vnp_qf_dict():

    return {
        'Fill Value': 65535,
        'Day': [
            slice(0, 1),
            {
                '0': True,
                '1': False
            }
        ],
        'Background': [
            slice(1, 4),
            {
                '000': 'Land & Desert',
                '001': 'Land no Desert',
                '010': 'Inland Water',
                '011': 'Sea Water',
                '101': 'Coastal'
             }
        ],
        'Cloud Mask Quality': [
            slice(4, 6),
            {
                '00': 'Poor',
                '01': 'Low',
                '10': 'Medium',
                '11': 'High'
             }
        ],
        'Cloud Detection Results & Confidence Indicator': [
            slice(6, 8),
            {
                '00': 'Confident Clear',
                '01': 'Probably Clear',
                '10': 'Probably Cloudy',
                '11': 'Confident Cloudy'
             }
        ],
        'Shadow': [
            slice(8, 9),
            {
                '0': False,
                '1': True
            }
        ],
        'Cirrus': [
            slice(9, 10),
            {
                '0': False,
                '1': True
            }
        ],
        'Snow or Ice': [
            slice(10, 11),
            {
                '0': False,
                '1': True
            }
        ]
    }


def get_vza_cold_qf_dict():

    return {
        'Day': True,
        'Cloud Detection Results & Confidence Indicator':
            [
                'Probably Cloudy',
                'Confident Cloudy'
            ],
        'Cirrus': True,
        'Snow or Ice': True
    }


def get_mask_hash_table(qf_dict, mask_dict):

    fill_value = None
    if 'Fill Value' in qf_dict:
        fill_value = qf_dict['Fill Value']
        qf_dict.pop('Fill Value')

    mask_list = []

    for value in np.arange(0, 2100):

        unpacked = bin(value)[2:]

        while len(unpacked) < 16:
            unpacked = '0' + unpacked

        reversed = ''

        unpacked_list = list(unpacked)
        while unpacked_list:
            reversed += unpacked_list.pop()

        match = True

        for qf_key in qf_dict.keys():
            if reversed[qf_dict[qf_key][0]] not in qf_dict[qf_key][1].keys():
                match = False
                break
            if qf_key in mask_dict.keys():
                if isinstance(mask_dict[qf_key], list):
                    if qf_dict[qf_key][1][reversed[qf_dict[qf_key][0]]] not in mask_dict[qf_key]:
                        match = False
                        break
                elif qf_dict[qf_key][1][reversed[qf_dict[qf_key][0]]] != mask_dict[qf_key]:
                        match = False
                        break

        if match:
            mask_list.append(value)

    return mask_list


def get_uint16_bits():

    return '0000000000000000'


def unpack_qf(qf, qf_dict, fill_value=None):

    if qf == fill_value:
        return None

    unpacked = bin(qf)[2:]

    while len(unpacked) < 16:
        unpacked = '0' + unpacked

    reversed = ''

    unpacked_list = list(unpacked)
    while unpacked_list:
        reversed += unpacked_list.pop()

    results_dict = {}

    for qf_key in qf_dict.keys():
        qf_slice = qf_dict[qf_key][0]
        # <> 100 is an invalid code in the place it appeared.
        if reversed[qf_slice] == '100':
            continue
            #print(reversed, qf_key, qf_slice, qf_dict[qf_key])

        qf_value = qf_dict[qf_key][1][reversed[qf_slice]]
        results_dict[qf_key] = qf_value

    return results_dict


def qf_mask_by_list(qf_array, qf_spec_dict, mask_dict, buffer=None, mask_fills=False):

    fill_value = None
    if 'Fill Value' in qf_spec_dict:
        fill_value = qf_spec_dict['Fill Value']
        qf_spec_dict.pop('Fill Value')

    mask_pixels = {}

    mask_list = get_mask_hash_table(qf_spec_dict, mask_dict)

    mark_arr = np.zeros(qf_array.shape)

    for row_ind, row in enumerate(qf_array):
        for col_ind, value in enumerate(row):

            if value == fill_value:
                if mask_fills:
                    if row_ind not in mask_pixels.keys():
                        mask_pixels[row_ind] = {}
                    mask_pixels[row_ind][col_ind] = None
            else:
                if value in mask_list:
                    if row_ind not in mask_pixels.keys():
                        mask_pixels[row_ind] = {}
                    mask_pixels[row_ind][col_ind] = None
                    if buffer:
                        for buff_row in np.arange(row_ind - buffer, row_ind + buffer):
                            for buff_col in np.arange(col_ind - buffer, row_ind + buffer):
                                if 0 <= buff_row < qf_array.shape[0]:
                                    if 0 <= buff_col < qf_array.shape[1]:
                                        if buff_row not in mask_pixels.keys():
                                            mask_pixels[buff_row] = {}
                                        mask_pixels[buff_row][buff_col] = None

    return mask_pixels


def compare_dicts(ref_dict, subject_dict, any_match=False):

    matches = 0

    for ref_key in ref_dict.keys():
        if ref_key in subject_dict.keys():
            if not isinstance(ref_dict[ref_key], list):
                if not isinstance(subject_dict[ref_key], list):
                    if subject_dict[ref_key] == ref_dict[ref_key]:
                        if any_match:
                            return True
                        matches += 1
                else:
                    for sub_val in subject_dict[ref_key]:
                        if sub_val == ref_dict[ref_key]:
                            if any_match:
                                return True
                            matches += 1
            else:
                for ref_val in ref_dict[ref_key]:
                    if not isinstance(subject_dict[ref_key], list):
                        if subject_dict[ref_key] == ref_val:
                            if any_match:
                                return True
                            matches += 1
                    else:
                        for sub_val in subject_dict[ref_key]:
                            if sub_val == ref_val:
                                if any_match:
                                    return True
                                matches += 1

    return matches
