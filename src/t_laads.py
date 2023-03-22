import requests
import logging
import t_spinup
import t_requests
import t_misc
from os import environ


# Get a request session object from LAADS via token-based authorization
def get_laads_session():
    # Header command utilizing security token
    auth_token = {'Authorization': f'Bearer {environ["laads_token"]}'}
    # Create session
    s = requests.session()
    # Update header with authorization
    s.headers.update(auth_token)
    # Return the session object
    return s


# Get a json from LAADS
def get_laads_json(json_url, session=get_laads_session()):
    # Ensure the json_url ends in .json
    if json_url.split('.')[-1] != 'json':
        json_url += '.json'
    # Ask nicely for the json, validating with a conversion to json
    r = t_requests.ask_nicely(session, json_url, validation_func=t_requests.validate_request_json)
    # Return the parse of the get attempt
    return parse_laads_get(r, json_url)


# Get a HDF5 file from LAADS
def get_laads_hdf5(h5_request, session=get_laads_session(), hash_to_check=None):
    # If a tuple of h5 url and a hash was supplied
    if isinstance(h5_request, tuple):
        # Break up the components
        h5_url = h5_request[0]
        hash_to_check = h5_request[1]
    else:
        h5_url = h5_request
    # Ensure the h5_url ends in .h5
    if h5_url.split('.')[-1] != 'h5':
        h5_url += '.h5'
    # Hash function
    hash_func = None
    # If a hash was provided
    if hash_to_check:
        # Reference the hash function
        hash_func = t_requests.validate_request_md5
    # Ask nicely for the HDF5 file, checking the hash
    r = t_requests.ask_nicely(session,
                              h5_url,
                              hash_func=hash_func,
                              hash_to_check=hash_to_check,
                              validation_func=t_requests.validate_request_hdf5)
    # Return a tuple of the URL and a parse of the get attempt
    return (h5_url, parse_laads_get(r, h5_url))


# Get a HDF4 file from LAADS
def get_laads_hdf4(h4_request, session=get_laads_session(), hash_to_check=None):
    # If a tuple of h5 url and a hash was supplied
    if isinstance(h4_request, tuple):
        # Break up the components
        h4_url = h4_request[0]
        hash_to_check = h4_request[1]
    else:
        h4_url = h4_request
    # Ensure the h4_url ends in .hdf
    if h4_url.split('.')[-1] != 'hdf':
        h4_url += '.hdf'
    # Hash function
    hash_func = None
    # If a hash was provided
    if hash_to_check:
        # Reference the hash function
        hash_func = t_requests.validate_request_md5
    # Ask nicely for the HDF5 file, checking the hash
    r = t_requests.ask_nicely(session,
                              h4_url,
                              hash_func=hash_func,
                              hash_to_check=hash_to_check)
    # Return a tuple of the URL and a parse of the get attempt
    return (h4_url, parse_laads_get(r, h4_url))


# Parse the result of getting a file from LAADS
def parse_laads_get(r, url):
    # If we got a response
    if r:
        # Return it
        return r
    # Otherwise (unsuccessful)
    else:
        # Log the error
        logging.error(f'Attempt to get {url} failed.')
        # Return None
        return None


# Convert the LAADS format "downloadsLink" to a usable URL
def convert_link_to_url(downloads_link):

    return str(downloads_link).replace('\\', '')


# Get a datetime date object from a LAADS format filename
def get_date_from_filename(filename):
    # Get the year and DOY
    year, doy = get_year_doy_from_filename(filename)
    # Return a date object
    return t_misc.get_dateobj_from_yeardoy(year, doy)


# Get year and DOY from filename
def get_year_doy_from_filename(filename):
    # Split out the date string
    date_string = filename.split('.')[1].replace('A', '')
    # Get the year and DOY
    year = date_string[0:4]
    doy = date_string[4:]
    # Return the year and DOY
    return year, doy

