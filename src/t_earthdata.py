import requests
from os import environ


# Get a request session object from EarthData via token-based authorization
def get_earthdata_session():
    # Header command utilizing security token
    auth_token = {'Authorization': f'Bearer {environ["earthdata_token"]}'}
    # Create session
    s = requests.session()
    # Update header with authorization
    s.headers.update(auth_token)
    # Return the session object
    return s


# Get a request session object from LAADS via token-based authorization
def get_laads_session():
    # Header command utilizing security token
    auth_token = {'Authorization': f'Bearer {environ["laads_token"].split(".")[0]}'}
    # Create session
    s = requests.session()
    # Update header with authorization
    s.headers.update(auth_token)
    # Return the session object
    return s


def get_s3_session():

    cookie = {'Cookie': 'accessToken=' + environ['earthdata_token'] + '; Path=/; Expires=Wed, 04 Oct 2023 15:24:51 GMT; HttpOnly; Secure',
              'Cookie': 'accessToken=' + environ['earthdata_token'] + '; Path=/; Expires=Wed, 04 Oct 2023 15:24:51 GMT; HttpOnly; Secure'}
    s = requests.Session()
    s.headers.update(cookie)

    return s