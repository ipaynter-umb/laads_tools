import logging
import json
import datetime
import t_spinup
import t_misc
import t_laads
import t_requests
from os import environ, walk, mkdir
from os.path import exists
from pathlib import Path
from time import time
from dotenv import load_dotenv
from numpy import around
from shutil import rmtree
from concurrent.futures import as_completed


# Class LAADS data set (to load when you need it)
class LAADSDataSet:

    def __init__(self,
                 name,
                 archive_set=None,
                 product=None,
                 start_date=None,
                 end_date=None,
                 include=None,
                 exclude=None):

        self.name = name
        self.archive_set = archive_set
        self.product = product
        self.start_date = start_date
        self.end_date = end_date
        # Portions of file names to include or exclude
        self.include = t_misc.listify(include)
        self.exclude = t_misc.listify(exclude)

        # Dictionaries for indexing the files
        self.by_date = {}
        self.by_filename = {}
        self.by_year_doy = {}

        # Spin up the object
        self.spinup()

    # Spinup procedure for the object
    def spinup(self):

        # LOAD SPECIFICATION FILE, OR CREATE ONE
        # If there is a dataset file with the name
        if self.find_dataset_spec():
            for attribute, attr_name in zip([self.archive_set, self.product, self.start_date, self.end_date],
                                            ['archive_set', 'product', 'start_date', 'end_date']):
                # Check the specification for an existing value and compare to provided value
                self.check_spec(attribute, attr_name)
        # Otherwise (no dataset spec)
        else:
            # If there is no archive set or product
            if not self.archive_set or not self.product:
                # Log an error
                logging.error(f"No preexisting specification found for LAADsDataSet {self.name}. Provide at least "
                              f"archive_set and product on instantiation.")
                # Return
                return
            # Instantiate start and end date variables
            start_date = None
            end_date = None
            # Convert dates to strings
            if self.start_date:
                start_date = self.start_date.strftime('%m/%d/%Y')
            if self.end_date:
                end_date = self.end_date.strftime('%m/%d/%Y')
            # Form output dictionary
            output_dict = {'Name': self.name,
                           'Archive Set': self.archive_set,
                           'Product': self.product,
                           'Start Date': start_date,
                           'End Date': end_date,
                           'Include': self.include,
                           'Exclude': self.exclude}
            # Save the specification
            with open(Path(environ["support_dir"], f"{self.name}_dataset_spec.json"), mode='w') as of:
                json.dump(output_dict, of, indent=4)

        # CATALOGS: LOAD, UPDATE, OR CREATE NEW ONE
        # Get the date of the latest catalog (or None if there is none)
        catalog_date = self.find_catalog_file()
        # If remake of catalog file is requested (remake from scratch) or there is no catalog
        if not catalog_date:
            # Make a new catalog
            self.get_catalog()
        # Otherwise (existing catalog)
        else:
            # Ingest catalog file
            self.ingest_catalog_file(catalog_date)

    # Check the specification for a given value
    def check_spec(self, attribute, attr_name):
        # If an attribute value was provided
        if attribute:
            # If the loaded archive set differed from the specified
            if getattr(self, attr_name) != attribute:
                # Log a warning
                logging.warning(f"For LAADsDataSet {self.name}, {attr_name} {attribute} was specified,"
                                f" but {attr_name} {attribute} was found in a preexisting"
                                f" dataset specification and was used.")

    # Look for a dataset specification file
    def find_dataset_spec(self):
        # Walk the support file directory
        for root, dirs, files in walk(environ["support_dir"]):
            # For each file name
            for name in files:
                # If the dataset's name is in the file name
                if f"{self.name}_dataset_spec.json" in name:
                    # Open the file
                    with open(Path(root, name), 'r') as f:
                        # Load dictionary
                        dataset_dict = json.load(f)
                    # Transfer the values
                    self.archive_set = dataset_dict['Archive Set']
                    self.product = dataset_dict['Product']
                    # If there is a start date
                    if dataset_dict['Start Date']:
                        self.start_date = datetime.datetime.strptime(dataset_dict['Start Date'], '%m/%d/%Y').date()
                    # If there is an end date
                    if dataset_dict['End Date']:
                        self.end_date = datetime.datetime.strptime(dataset_dict['End Date'], '%m/%d/%Y').date()
                    self.include = dataset_dict['Include']
                    self.exclude = dataset_dict['Exclude']
                    # Return True
                    return True
        # Return False (didn't find a file)
        return False

    # Find the latest date for a support file containing a given string
    def get_latest_support_file_date(self, file_str):
        # Latest date variable
        latest_date = None
        # Walk the support file directory
        for root, dirs, files in walk(environ["support_dir"]):
            # For each file name
            for name in files:
                # If the dataset's name is in the file name
                if file_str in name:
                    # Split the name
                    split_name = name.split('_')
                    # Make a datetime date object from the name
                    datetime_str = split_name[-2] + split_name[-1].split('.')[0]
                    file_datetime = datetime.datetime.strptime(datetime_str, '%m%d%Y%H%M%S')
                    # If there is no latest date yet
                    if not latest_date:
                        # Set the file's date
                        latest_date = file_datetime
                    # Otherwise, if the file's date is later
                    elif file_datetime > latest_date:
                        # Set the file's date as latest
                        latest_date = file_datetime
        # Return latest date
        return latest_date

    # Find the date of latest dataset catalog file
    def find_catalog_file(self):
        return self.get_latest_support_file_date(f"{self.name}_catalog_")

    # Ingest a catalog file
    def ingest_catalog_file(self, catalog_datetime):
        # If the date provided is a datetime.date object
        if isinstance(catalog_datetime, datetime.datetime):
            # Convert to string
            catalog_datetime = catalog_datetime.strftime('%m%d%Y_%H%M%S')
        # Assemble path
        catalog_path = Path(environ['support_dir'], f'{self.name}_catalog_{catalog_datetime}.json')
        # Open the file
        with open(catalog_path, mode='r') as f:
            # Convert to json
            incoming_catalog = json.load(f)
        # For each key (File name) in the dictionary
        for filename in incoming_catalog.keys():
            # Instantiate an object
            file_obj = LAADSFile(filename,
                                 t_laads.get_date_from_filename(filename),
                                 incoming_catalog[filename])
            # Store under the default indexing dictionaries
            self.by_filename[filename] = file_obj
            # Add sublist for date object key
            if file_obj.date not in self.by_date.keys():
                self.by_date[file_obj.date] = []
            self.by_date[file_obj.date].append(file_obj)
            # Get year and doy keys
            year_key = str(file_obj.date.year)
            doy_key = t_misc.get_doy_from_date(file_obj.date,
                                               zero_pad_digits=3)
            # Store by year and doy
            if year_key not in self.by_year_doy.keys():
                self.by_year_doy[year_key] = {}
            if doy_key not in self.by_year_doy[year_key].keys():
                self.by_year_doy[year_key][doy_key] = []
            self.by_year_doy[year_key][doy_key].append(file_obj)

    # Find the latest download file
    def find_download_file(self):
        return self.get_latest_support_file_date(f"{self.name}_download_")

    # Get a brand new catalog based on a LAADSDataSet object
    def get_catalog(self):

        # Start time
        stime = time()

        # Log start of catalog
        logging.info(f"Starting retrieval of catalog for {self.product}"
                     f" from archive set {self.archive_set}.")

        # URL for the archive set + product
        product_url = environ['laads_alldata_url'] + f'{self.archive_set}/{self.product}'

        # Get a json of the years
        years_json = t_laads.get_laads_json(product_url)

        # If we did not get a years json
        if not years_json:
            # Stop here
            return

        # List for the years urls
        years_urls = []
        # For each year in the json
        for year in years_json['content']:
            # Reference year name
            year_name = year['name']
            # Check it is a valid year
            try:
                curr_date = datetime.date(year=(int(year_name)),
                                          day=1,
                                          month=1)
            except:
                logging.warning(f'Year {year_name} in product {self.product}'
                                f' in Archive Set {self.archive_set} is not a valid year.')
                # Skip the year
                continue
            # If the year is before the start date year
            if self.start_date:
                if int(year['name']) < self.start_date.year:
                    # Skip it
                    continue
            # If the year is after the end date year
            if self.end_date:
                if int(year['name']) > self.end_date.year:
                    # Skip it
                    continue
            # Get the URL and append to list
            years_urls.append(t_laads.convert_link_to_url(year['downloadsLink']))
        # List for the doys urls
        doys_urls = []

        # Multithread crawl through the years
        futures = t_misc.multithread(t_laads.get_laads_json, years_urls, as_completed_yield=True)

        # For each future
        for future in futures:
            # For each doy in the result
            for doy in future.result()['content']:
                # Get the URL
                doy_url = t_laads.convert_link_to_url(doy['downloadsLink'])
                # Get the year
                year = doy_url.split('/')[-2]
                # If the dataset has a specified start date
                if self.start_date:
                    # If the doy is before the start date
                    if t_misc.get_dateobj_from_yeardoy(int(year), int(doy['name'])) < self.start_date:
                        # Skip the DOY
                        continue
                # If the dataset has a specified end date
                if self.end_date:
                    # If the doy is after the end date
                    if t_misc.get_dateobj_from_yeardoy(int(year), int(doy['name'])) > self.end_date:
                        # Skip the DOY
                        continue
                # Add the URL to the list
                doys_urls.append(doy_url)
        # Log information
        logging.info(f"Total of {len(doys_urls)} days entering multithreaded crawling.")

        # Multithread crawl through the days
        futures = t_misc.multithread(t_laads.get_laads_json, doys_urls, as_completed_yield=True)

        # Dictionary for files
        file_dict = {}

        # For each future
        for future in futures:
            # For each file in the result
            for file in future.result()['content']:
                # If there are inclusions
                if self.include:
                    # Pass inclusions switch
                    pass_includes = True
                    # For each inclusion
                    for include in self.include:
                        # If the inclusion is not in the file name
                        if include not in file['name']:
                            # Flip the switch
                            pass_includes = False
                    # If failed any includes
                    if not pass_includes:
                        # Skip the file
                        continue
                # If there are exclusions
                if self.exclude:
                    # Pass exclusions switch
                    pass_excludes = True
                    # For each exclusion
                    for exclude in self.exclude:
                        # If the exclusion is in the file name
                        if exclude in file['name']:
                            # Flip the switch
                            pass_excludes = False
                    # If failed any excludes
                    if not pass_excludes:
                        # Skip the file
                        continue
                # Add to file dictionary
                file_dict[file['name']] = file['md5sum']

        logging.info(f"File catalog for {self.product} in Archive Set {self.archive_set}"
                     f" retrieved in {around(time() - stime, decimals=2)} seconds. Writing output...")

        # End datetime string
        end = datetime.datetime.now().strftime("%m%d%Y_%H%M%S")

        # Assemble the path to the catalog file
        output_path = Path(
            environ["support_dir"], f"{self.name}_catalog_" + end + ".json")

        # When we're finished, save the dictionary
        with open(output_path, 'w') as of:
            json.dump(file_dict, of, indent=4)

        logging.info(f"Output saved to {output_path}.")

    # Get a URL from a filename
    def get_url_from_filename(self, filename):
        # Get the year and DOY from filename
        year, doy = t_laads.get_year_doy_from_filename(filename)
        # Return full URL for file
        return environ['laads_alldata_url'] + \
               self.archive_set + \
               f'/{self.product}' + \
               f'/{year}' + \
               f'/{doy}' + \
               f'/{filename}'

    # Download the whole catalog
    def download_catalog(self, from_scratch=False):
        # Directory to download to
        download_dir = Path(environ['inputs_dir'], self.name)
        # If there is a directory to store the files
        if exists(download_dir):
            # If starting from scratch
            if from_scratch:
                # Remove the directory
                rmtree(download_dir)
                # Remake the directory
                mkdir(download_dir)
        # Otherwise (no directory)
        else:
            # Make the directory
            mkdir(download_dir)
        # Get the download record to date
        download_dict = self.get_download_record()
        # List of work for multithreading
        list_of_work = []
        # For each filename
        for filename in self.by_filename.keys():
            # If the file is already in the download dictionary
            if filename in download_dict.keys():
                # If the status is anything other than True
                if not download_dict[filename]:
                    # Add tuple of URL and hash to list of work
                    list_of_work.append((self.get_url_from_filename(filename), self.by_filename[filename].hash))
            # Otherwise (not in download dictionary)
            else:
                # Add URL and hash to list of work
                list_of_work.append((self.get_url_from_filename(filename), self.by_filename[filename].hash))
        # Log information about the list of work
        logging.info(f'Sending {len(list_of_work)} files to multithreaded download.')
        # Mark start time
        stime = time()
        # Get datetime string
        now_str = datetime.datetime.now().strftime("%m%d%Y_%H%M%S")

        # Open a download log in write mode
        of = open(Path(environ['support_dir'],
                           f'{self.name}_download_{now_str}.txt'),
                  mode='w')

        # Get the file suffix for the first file
        file_suffix = list(self.by_filename.keys())[0].split('.')[-1]
        # Set multithread function to h5
        mt_func = t_laads.get_laads_hdf5
        # If this is a hdf4 catalog
        if file_suffix == 'hdf':
            # Swap the multithread function for HDF4 retrieval
            mt_func = t_laads.get_laads_hdf4
        # Multithread
        futures = t_misc.multithread(mt_func, list_of_work, as_completed_yield=True)
        # For each future as it is completed
        for future in as_completed(futures):
            # Split out the file and filename
            file = future.result()[1].content
            filename = future.result()[0].split('/')[-1]
            # If we did not get the file
            if not file:
                # Write a line to the download log
                of.write(f'{filename} False\n')
            # Otherwise
            else:
                # Get the write path for the file
                write_path = Path(environ['inputs_dir'], self.name, filename)
                # Write the file locally
                write_result = t_misc.write_bytes(file, write_path)
                # If the result was successful
                if write_result:
                    # Write a line to the download log
                    of.write(f'{filename} True\n')
                # Otherwise
                else:
                    # Write a line to the download log
                    of.write(f'{filename} False\n')
        # Close the log file
        of.close()
        # Report on the overall time taken
        logging.info(f"All downloads finished in {around(time() - stime, decimals=2)} seconds.")

    def get_download_record(self):
        # Download dictionary
        download_dict = {}
        # Walk the support file directory
        for root, dirs, files in walk(environ["support_dir"]):
            # For each file name
            for name in files:
                # If the dataset's name and download is in the file name
                if f'{self.name}_download_' in name:
                    # Open the file
                    f = open(Path(environ['support_dir'], name), mode='r')
                    # For each line
                    for line in f:
                        # Split the line on the space
                        split_line = line.strip().split(' ')
                        # Set the status
                        status = False
                        # Convert the status
                        if split_line[1] == 'True':
                            status = True
                        # If the filename is not in the download dict
                        if split_line[0] not in download_dict.keys():
                            # Add it, with the status
                            download_dict[split_line[0]] = status
                        # Otherwise, if the status is True
                        elif status:
                            # Swap the status to True
                            download_dict[split_line[0]] = status
        # Return the download record
        return download_dict


class LAADSFile:

    def __init__(self, file_name, date, hash):

        self.name = file_name
        self.date = date
        self.hash = hash

        # If the date is a string
        if isinstance(self.date, str):
            # Convert to a datetime date object
            self.date = datetime.datetime.strptime(self.date, '%m%d%Y').date()