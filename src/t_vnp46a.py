import t_spinup
import t_misc
from time import time
from datetime import datetime

def get_components_from_filename(filename,
                                 product=False,
                                 year=False,
                                 doy=False,
                                 date_obj=False,
                                 tilename=False,
                                 tile_h=False,
                                 tile_v=False,
                                 collection=False):

    splitname = filename.split('.')

    output = []

    if product:
        output.append(splitname[0])
    if year:
        output.append(splitname[1][1:5])
    if doy:
        output.append(splitname[1][5:])
    if date_obj:
        output.append(t_misc.get_dateobj_from_yeardoy(int(splitname[1][1:5]), int(splitname[1][5:])))
    if tilename:
        output.append(splitname[2])
    if tile_h:
        output.append(splitname[2][1:3])
    if tile_v:
        output.append(splitname[2][4:])
    if collection:
        output.append(splitname[3])
    if len(output) == 1:
        return output[0]
    if len(output) > 0:
        return output


def get_components_from_filename_benchmark(filename):

    splitname = filename.split('.')

    return splitname[0], splitname[1][1:5], splitname[1][5:], splitname[2], splitname[2][1:3], splitname[2][4:], splitname[3]


if __name__ == '__main__':

    filename = 'VNP46A2.A2018118.h11v07.001.2020337102243.h5'

    stime = time()

    product, year, doy, tilename, tile_h, tile_v, collection = get_components_from_filename_benchmark(filename)

    print(time() - stime)

    print(product, year, doy, tilename, tile_h, tile_v, collection)

    stime = time()

    product, year, doy, tilename, tile_h, tile_v, collection = get_components_from_filename(filename,
                                                                                            product=True,
                                                                                            year=True,
                                                                                            doy=True,
                                                                                            tilename=True,
                                                                                            tile_h=True,
                                                                                            tile_v=True,
                                                                                            collection=True)

    print(time() - stime)

    print(product, year, doy, tilename, tile_h, tile_v, collection)