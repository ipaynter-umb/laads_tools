import datetime
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from os.path import exists
from os import mkdir


# Listify a target (return an empty list if None, encapsulate in list if single value)
def listify(target):
    # If the input is None
    if not target:
        # Return an empty list
        return []
    # Otherwise, if input is not a list
    elif not isinstance(target, list):
        # Return input encapsulated in list
        return [target]
    # Otherwise (it was a list)
    else:
        # Return input unchanged
        return target


# Multithread a function for a list of work
def multithread(mt_func, list_of_work, as_completed_func=None, as_completed_yield=False, max_workers=4):
    # Instantiate thread pool
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit the work using a list structure to capture the results
        futures = [executor.submit(mt_func, work) for work in list_of_work]
        # For each completed Future object
        for future in as_completed(futures):
            # If something to do as completed
            if as_completed_func:
                # Do the work
                as_completed_func(future.result())
            # If yielding the results as completed
            if as_completed_yield:
                # Yield the future
                yield future


# Get a DOY from a datetime object (specify zero pad digits in zero_pad kwarg)
def get_doy_from_date(date, zero_pad_digits=None):
    # Get the day of year
    doy = (date - datetime.date(year=date.year, month=1, day=1)).days + 1
    # If there is a zero pad requested
    if zero_pad_digits:
        # Zero pad the number
        doy = zero_pad_number(doy, zero_pad_digits)
    # Return the doy
    return doy


def get_dateobj_from_yeardoy(year, doy):
    # Convert year and doy to ints
    year = int(year)
    doy = int(doy)
    # Return the datetime object
    return datetime.date(year=year, month=1, day=1) + datetime.timedelta(days=doy - 1)


def zero_pad_number(input_number, digits=3):
    # Make sure the number has been converted to a string
    input_number = str(input_number)
    # While the length of the string is less than the required digits
    while len(input_number) < digits:
        # Prepend a 0 to the string
        input_number = '0' + input_number
    # Return the string
    return input_number


# Ensure a path exists
def ensure_file_path_dirs_exist(file_path):
    # Empty check path
    check_path = Path()
    # For each part in the file path (apart from the file name)
    for part in file_path.parts[0:-1]:
        # Form the path
        check_path = Path(str(check_path), part)
        # If this section of the path does not exists
        if not exists(check_path):
            # Log the info
            logging.info(f'Creating {str(check_path)} for write path {str(file_path)}')
            # Try to make the directory
            try:
                mkdir(check_path)
            # If this does not work
            except:
                # Log error
                logging.error(f'Could not create directory {check_path} for {file_path}.')
                # Return False
                return False
    # Return True
    return True


# Write bytes object to storage
def write_bytes(bytes_object, write_path):
    # If the write path already exists
    if exists(write_path):
        # Log a warning
        logging.warning(f'Already a file at {write_path}. Overwriting.')
    # Ensure the path, but if it returns False (failed)
    if not ensure_file_path_dirs_exist(write_path):
        # Log an error
        logging.error(f'Could not ensure write path for {write_path}.')
        # Return False (failed)
        return False
    # Log the info
    logging.info(f'Writing object to {write_path}.')

    # Try to write the request content
    try:
        with open(write_path, 'wb') as f:
            f.write(bytes_object)
    except:
        # Log error
        logging.error(f'Could not write object to {write_path}.')
        # Return False (failed)
        return False
    else:
        # Log the info
        logging.info(f'Successfully wrote object to {write_path}.')
        # Return True (succeeded)
        return True