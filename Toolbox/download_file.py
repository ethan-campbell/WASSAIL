# -*- coding: utf-8 -*-

from numpy import *
import os
import platform
import shutil
import requests
import subprocess
import time
from ftplib import FTP
from datetime import datetime
import urllib3
import getpass


def single_file(url_dir,filename,save_to,ftp_root=False,overwrite=False,verbose=True,auth=None,cert=True,
                nasa_auth_session=None):
    """ Downloads and saves a file from a given URL.

    Notes:
        - For HTTP downloads, if '404 file not found' error returned, function will return without
          downloading anything.
        - For FTP downloads, if given filename doesn't exist in directory, function will return without
          downloading anything.
    
    Args:
        url_dir: URL up to the filename, including ending slash
            NOTE: for ftp servers, include URL after the root, without starting slash
        filename: filename, including suffix
        save_to: directory path, including trailing slash but not including the filename
        ftp_root: root URL of ftp server without preamble (ftp://) or ending slash, or 'False' if using HTTP
        overwrite: False (default) to leave existing files in place; True to overwrite
            (note: doesn't explicitly/separately delete existing file before downloading new file)
        cert: True (default) to validate SSL certificate; False to ignore validity of SSL certificate
        nasa_auth_session: if NASA Earthdata authentication required, pass 'sessions' instance from df.nasa_auth()
    
    """
    starting_dir = os.getcwd()

    if cert is False:
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    try:
        if starting_dir is not save_to:
            os.chdir(save_to)
        if filename in os.listdir():
            if not overwrite:
                if verbose: print('>>> File ' + filename + ' already exists. Leaving current version.')
                return
            else:
                if verbose: print('>>> File ' + filename + ' already exists. Overwriting with new version.')

        if not ftp_root:
            full_url = url_dir + filename

            def get_func(url, stream=True, auth_key=None, verify=cert):
                try:
                    if nasa_auth_session is None:
                        return requests.get(url, stream=stream, auth=auth_key, verify=cert)
                    else:
                        return nasa_auth_session.get(url,stream=stream,auth=auth_key,verify=cert)
                except requests.exceptions.ConnectionError as error_tag:
                    print('Error connecting:', error_tag)
                    # note: this solution is super hacky and bad practice,
                    #       see https://stackoverflow.com/questions/16511337/correct-way-to-try-except-using-python-requests-module
                    time.sleep(1)
                    return get_func(url, stream=stream, auth_key=auth_key, verify=cert)

            response = get_func(full_url, stream=True, auth_key=auth, verify=cert)

            if response.status_code == 404:
                if verbose: print('>>> File ' + filename + ' returned 404 error during download.')
                return
            with open(filename,'wb') as out_file:
                shutil.copyfileobj(response.raw, out_file)
            del response
        else:
            ftp = FTP(ftp_root)
            ftp.login();
            ftp.cwd(url_dir);
            contents = ftp.nlst();
            if filename in contents:
                local_file = open(filename,'wb')
                ftp.retrbinary('RETR ' + filename, local_file.write);
                local_file.close()
            ftp.quit();
    finally:
        os.chdir(starting_dir)


def nasa_auth(stored_auth=True,which_nasa='n5eil02u'):
    """ Authenticate username and password with NASA/NSIDC server to download data. Asks user for input.

    Arguments:
        stored_auth: True (default) to use NASA Earthdata credentials stored in ~/.bash_profile
                     (accessible by Python scripts, not Jupyter notebooks)
                     or False to prompt user for login info
        which_nasa: 'n5eil02u' (default) for data sets accessible at URLs starting with 'https://n5eil02u.ecs.nsidc.org/'
                    or 'daacdata' for data sets accessible at URLs starting at 'https://daacdata.apps.nsidc.org/'

    Returns:
        session: Python requests library authenticated session instance, to be used for download:
                 e.g., session.get(URL...)

    To store login credentials:
        1. Edit ~/.bash_profile, e.g., using: nano ~/.bash_profile
        2. Append the following lines: export NASA_USERNAME=<my_username>
                                       export NASA_PASSWORD=<my_password>
        3. Save the file and exit. To make changes take effect immediately, run: source ~/.bash_profile

    """
    if stored_auth:
        username = os.getenv('NASA_USERNAME','localhost')
        password = os.getenv('NASA_PASSWORD','localhost')
    else:
        username = input('Please enter NASA Earthdata login username: ')
        password = getpass.getpass('Please enter NASA Earthdata login password: ')

    if which_nasa == 'n5eil02u':
        capability_url = 'https://n5eil02u.ecs.nsidc.org/egi/'
    elif which_nasa == 'daacdata':
        capability_url = 'https://daacdata.apps.nsidc.org'
    else:
        print('ERROR from df.nasa_auth(): please provide a correct URL prefix')
        return None
    session = requests.session()
    s = session.get(capability_url)
    response = session.get(s.url,auth=(username,password))
    if response.status_code != 200:
        print(f'ERROR from df.nasa_auth(): authentication attempt gave status code of {response.status_code}')
    return session


def convert_to_hdf5(script_dir, filename, old_data_dir, new_data_dir, overwrite=False, delete_original=False):
    """ Converts a single file from HDF4 to HDF5 format using a command line utility from internet.

    Converts given .hdf file in old_data_dir. Stores new HDF5 file as a copy in new_data_dir.
    If given HDF4 file does not exist, function will return without doing anything.

    Args:
        script_dir: directory path of h4toh5 executable (command line) script
        old_data_dir: directory path of original HDF4 data file
        new_data_dir: directory path for new, converted HDF5 data file
        NOTE: all directory paths should contain trailing slash ('/')

    """

    starting_dir = os.getcwd()
    os.chdir(new_data_dir)
    new_data_files = os.listdir()
    os.chdir(old_data_dir)
    old_data_files = os.listdir()
    os.chdir(script_dir)

    if   platform.system() == 'Darwin': exec_filename = 'h4toh5_macos_catalina'   # MacOS 10.15 Catalina
    elif platform.system() == 'Linux':  exec_filename = 'h4toh5_linux_centos7'    # Linux CentOS7 (x86_64)
    else: raise OSError("df.convert_to_hdf5() failed because h4toh5 file converter"
                        "is not available for this computer's architecture.")

    new_filename = filename.split('.')[0] + '.h5'
    if filename in old_data_files:
        if (new_filename not in new_data_files) or (new_filename in new_data_files and overwrite is True):
            command = './' + exec_filename + ' "' + old_data_dir + filename + '"'
            subprocess.call(command,shell=True)
            if old_data_dir is not new_data_dir:
                os.rename(old_data_dir + new_filename, new_data_dir + new_filename)
        if delete_original:
            os.remove(old_data_dir + filename)
    os.chdir(starting_dir)


def how_far(index, all_vals, interval):
    """ Prints percent-completion notices while iterating through a list.

    Args:
        index: current index within all_vals
        all_vals: a list
        interval: e.g. 0.1 = print notices at 10%-completion intervals
    """
    percents_comp = floor(linspace(0, 100, int(1 / interval + 1)))
    percents_comp_indices = floor(linspace(0, len(all_vals), int(1 / interval + 1)))
    if index in percents_comp_indices:
        percent_comp = percents_comp[where(percents_comp_indices == index)[0][0]]
        print('>>> ' + str(datetime.now()) + ' - action is ' + str(percent_comp) + '% complete')
