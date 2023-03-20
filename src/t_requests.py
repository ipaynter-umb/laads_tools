import logging
import h5py
import t_misc
import hashlib
from io import BytesIO
from time import sleep
from requests.exceptions import JSONDecodeError


# Ask nicely for a particular URL from a requests module session object
def ask_nicely(session,
               url,
               session_func=None,
               validation_func=None,
               hash_func=None,
               hash_to_check=None,
               back_off_base=0,
               back_off_inc=1,
               attempts_per_session=3,
               max_attempts=10):
    # Attempts count
    attempts = 0
    # Attempts left in session
    session_attempts = attempts_per_session
    # While attempts continue
    while True:
        # Increment attempts
        attempts += 1
        # If this is not the first attempt
        if attempts > 1:
            # Add to the back-off timer
            back_off_base += back_off_inc
            # Wait quietly and politely
            sleep(back_off_base)
        # If attempt max has been reached
        if attempts == max_attempts + 1:
            # Log max attempts
            logging.error(f'Request for {url} reached maximum attempts ({max_attempts}).')
            # Break the loop
            break
        # Decrement remaining attempts for session
        session_attempts -= 1
        # If session attempts has reached 0
        if session_attempts == 0:
            # If a session generation function has been supplied
            if session_func:
                # Log info
                logging.info(
                    f'Request for {url} reached maximum attempts for session ({attempts_per_session}). '
                    f'Getting new session from {session_func}.')
                # Refresh the session
                session = session_func()
                # Reset the attempts
                session_attempts = attempts_per_session
            # Otherwise (no session function supplied)
            else:
                # Log information
                logging.info(
                    f'Request for {url} reached maximum attempts for session ({attempts_per_session}). '
                    f'No function to get new session (session_func) was supplied.')
                # Break the loop
                break
        # Make a request
        r = session.get(url, allow_redirects=False)
        # If the request does not have a success code (code 200)
        if r.status_code != 200:
            # Log a non-200 status code
            logging.info(f'Request for {url}, attempt {attempts} returned code: {r.status_code}.')
            # Move to next attempt
            continue
        # If there is a hash function
        if hash_func:
            # If a hash was provided to check against
            if hash_to_check:
                # Check the hash with the validation function
                r = hash_func(r, hash_to_check)
                # If the response failed the hash match
                if not r:
                    # Log a hash match failure
                    logging.info(f'Request for {url} did not match hash.')
                    # Go to the next attempt
                    continue
            # Otherwise (no hash provided)
            else:
                # Log a warning
                logging.info(f'Hash validation function provided for {url}, but no reference hash provided.')
        # If there is a validation function
        if validation_func:
            # Listify the validation function
            validation_func = t_misc.listify(validation_func)
            # Validation trigger
            valid_file = True
            # For each validation function
            for v_func in validation_func:
                # Get the response wanted from the validation function
                r = v_func(r)
                # If the response failed the validation function
                if not r:
                    # Flip the valid switch
                    valid_file = False
                    # Log a validation failure
                    logging.info(f'Request for {url} failed validation by {v_func}.')
            # If file was not valid
            if not valid_file:
                # Go to the next attempt
                continue
        # Return the response
        return r
    # Log max attempts
    logging.error(f'Request for {url} failed completely.')
    # Return None
    return None


# Validate a request contents as json
def validate_request_json(r):
    # Try to parse the response into json
    try:
        r = r.json()
    # If not successful
    except JSONDecodeError:
        # Log the occurrence
        logging.debug(f'Response content was not a valid JSON.')
        # Return None
        return None
    # Otherwise (JSON decoded)
    else:
        # Return the response converted to JSON
        return r


# Validate a request contents as hdf5
def validate_request_hdf5(r):
    # Try to parse the response into HDF5
    try:
        test_convert = h5py.File(BytesIO(r.content), 'r')
    # If not successful
    except OSError:
        # Log the occurrence
        logging.debug(f'Response content was not a valid HDF5.')
        # Return None
        return None
    # Otherwise (HDF5 encoded)
    else:
        # Return the response
        return r


# Validate MD5 hash for a file
def validate_request_md5(r, ref_hash):
    # Hash the file
    file_hash = hashlib.md5(r.content).hexdigest()
    # If the hashes don't match
    if file_hash != ref_hash:
        # Log the hash mistmatch
        logging.info(f'Response content did not match the reference hash.')
        # Return None
        return None
    # Otherwise (hashes matched)
    else:
        # Return the response
        return r


# Write (bytes) content of request to storage
def write_request_content(request, write_path):
    # Send the content to the writer
    result = t_misc.write_bytes(request.content, write_path)
    # If successful
    if result:
        # Return True
        return True
    # Otherwise
    else:
        # Return False
        return False
