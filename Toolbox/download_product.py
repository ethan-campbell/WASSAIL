# -*- coding: utf-8 -*-

from numpy import *
from datetime import datetime, timedelta
from dateutil.relativedelta import *
import os
import pandas as pd
import xarray as xr
import cdsapi
import netCDF4

import time_tools as tt
import load_product as ldp
import download_file as df


def amsr(which_amsr,start_date,end_date,save_to,get_images=True,overwrite=False,convert=False,
         conversion_script_dir=None):
    """ Downloads AMSR-E or AMSR2 sea ice concentration product.

    Converts data from HDF4 to HDF5 format by calling df.convert_to_hdf5() if 'convert'
    is True, then deletes original HDF4 file.

    AMSR-2:
        AMSR2 6.25 km daily sea ice concentration product is ARTIST Sea Ice (ASI)
        algorithm, v5.4, from 89 GHz channel, a preliminary data product that uses the
        AMSR-E calibrations. Consider switching to JAXA GCOM-W1 AMSR2 sea ice
        product when "research" calibrated version becomes available, or NSIDC
        DAAC validated versions (supposedly in late 2016).

        Example file path: http://www.iup.uni-bremen.de:8084/amsr2data/asi_daygrid_swath/s6250/2015/aug/Antarctic/asi-AMSR2-s6250-20150801-v5.4.hdf
                           https://seaice.uni-bremen.de/data/amsr2/asi_daygrid_swath/s6250/2021/jun/Antarctic/asi-AMSR2-s6250-20210601-v5.4.hdf

        Note that 3.125 km gridded ARTIST AMSR2 is available from the following
        link, but the lower 6.25 km resolution is used here for consistency with
        AMSR-E products: ftp://ftp-projects.zmaw.de/seaice/AMSR2/

    AMSR-E:
        AMSR-E 6.25 km daily sea ice concentration product is ARTIST Sea Ice (ASI)
        algorithm, v5.4, from 89 GHz channel.

        Example file path: https://seaice.uni-bremen.de/data/amsre/asi_daygrid_swath/s6250/2011/oct/Antarctic/asi-s6250-20111004-v5.4.hdf

        Another option for AMSR-E is the 12.5 km v3 NSIDC product available here:
        http://nsidc.org/data/AE_SI12

        It seems that the 6.25 km ASI product is also available at the following link,
        but no 3.125 km product is available: ftp://ftp-projects.zmaw.de/seaice/AMSR-E_ASI_IceConc/

    SSMIS product from University of Bremen on 6.25 km grid to bridge gap between AMSR-E and AMSR2:
        SSMIS interim: https://seaice.uni-bremen.de/data/ssmis/asi_daygrid_swath/s6250/
    
    Required data acknowledgement: Spreen et al. (2008), doi:10.1029/2005JC003384
    Optional data acknowledgement (for AMSR2): Beitsch et al. (2014), doi:10.3390/rs6053841
    
    Args:
        which_amsr: if 1, download AMSR-E; if 2, download AMSR2
        start_date and end_date: (Y,M,D), with start/end inclusive
        save_to: directory path
        get_pdfs: download image files
    Returns:
        None
    Raises:
        No handled exceptions
    
    """
    if which_amsr == 2:
        url_part1 = 'https://seaice.uni-bremen.de/data/amsr2/asi_daygrid_swath/s6250/'
        url_part2 = '/Antarctic/'
        filename_part1 = 'asi-AMSR2-s6250-'
        filename_part2 = '-v5.4.hdf'
        new_filename_part2 = '-v5.4.h5'
    elif which_amsr == 1:
        url_part1 = 'https://seaice.uni-bremen.de/data/amsre/asi_daygrid_swath/s6250/'
        url_part2 = '/Antarctic/'
        filename_part1 = 'asi-s6250-'
        filename_part2 = '-v5.4.hdf'
        new_filename_part2 = '-v5.4.h5'
    filename_part2_image1 = '-v5.4_nic.png'
    filename_part2_image2 = '-v5.4_visual.png'
    months = ['jan','feb','mar','apr','may','jun','jul','aug','sep','oct','nov','dec']

    starting_dir = os.getcwd()
    os.chdir(save_to)
    existing_files = os.listdir()
    os.chdir(starting_dir)

    all_dates = tt.dates_in_range(start_date, end_date)
    for index, d in enumerate(all_dates):
        url_dir = url_part1 + str(d[0]) + '/' + months[d[1]-1] + url_part2
        filename = filename_part1 + '{0[0]}{0[1]:02d}{0[2]:02d}'.format(d) + filename_part2
        new_filename = filename_part1 + '{0[0]}{0[1]:02d}{0[2]:02d}'.format(d) + new_filename_part2
        if (new_filename not in existing_files) or (new_filename in existing_files and overwrite is True):
            df.single_file(url_dir, filename, save_to, overwrite)
        if convert:
            df.convert_to_hdf5(conversion_script_dir,filename,save_to,save_to,overwrite=overwrite,delete_original=True)
        if get_images:
            image1name = filename_part1 + '{0[0]}{0[1]:02d}{0[2]:02d}'.format(d) + filename_part2_image1
            image2name = filename_part1 + '{0[0]}{0[1]:02d}{0[2]:02d}'.format(d) + filename_part2_image2
            df.single_file(url_dir, image1name, save_to, overwrite)
            df.single_file(url_dir, image2name, save_to, overwrite)
        df.how_far(index,all_dates,0.01)


def cdr_nrt_v3(start_date,end_date,save_to,overwrite=False):
    """ Downloads NOAA/NSIDC 25 km preliminary Near Real-Time (NRT) v3 Climate Data Record (CDR) passive microwave
        sea ice concentration product.

    NSIDC's v3 (r0) daily SSMIS product on 25 km grid in netCDF-4 (HDF5) format. Product derived from 3 channels. Data
    files are based on purely automated application and merging of the NASA Team (NT) and Bootstrap (BT) algorithms.

    Data page (defaults to latest version): https://nsidc.org/data/g10016

    Documentation: https://nsidc.org/sites/default/files/documents/user-guide/g10016-v003-userguide.pdf

    Example file path:
        https://noaadata.apps.nsidc.org/NOAA/G10016_V3/CDR/south/daily/2025/sic_pss25_20250101_F17_icdr_v03r00.nc

    Expert guidance on the related CDR record:
        https://climatedataguide.ucar.edu/climate-data/sea-ice-concentration-noaansidc-climate-data-record

    Required citation:
        Meier, W. N., Fetterer, F., Windnagel, A. K., Stewart, J. S. & Stafford, T. (2024). Near-Real-Time NOAA/NSIDC Climate 
        Data Record of Passive Microwave Sea Ice Concentration. (G10016, Version 3). [Data Set]. Boulder, Colorado USA. 
        National Snow and Ice Data Center. https://doi.org/10.7265/j0z0-4h87. [Indicate subset used]. [Date Accessed].

    """
    url_prefix = 'https://noaadata.apps.nsidc.org/NOAA/G10016_V3/CDR/south/daily/'
    filename_prefix = 'sic_pss25_'
    filename_suffix = '_icdr_v03r00.nc'

    sat_abbrevs = ['F17']
    sat_start_dates = [(2025,1,1)]
    sat_end_dates = [tt.now()]

    all_dates = tt.dates_in_range(start_date,end_date)
    for index, d in enumerate(all_dates):
        if not tt.is_time_in_range(sat_start_dates[0],sat_end_dates[-1],d):
            raise ValueError('Given date range exceeds hard-coded satellite date ranges.')
        for sat in range(0,len(sat_abbrevs)):
            if tt.is_time_in_range(sat_start_dates[sat],sat_end_dates[sat],d):
                sat_abbrev = sat_abbrevs[sat]
        filename = filename_prefix + '{0[0]}{0[1]:02d}{0[2]:02d}'.format(d) + '_' + sat_abbrev + filename_suffix

        starting_dir = os.getcwd()
        try:
            if starting_dir is not save_to:
                os.chdir(save_to)
            df.single_file(url_prefix + '{0[0]}/'.format(d),filename,save_to,overwrite=overwrite)
        finally:
            os.chdir(starting_dir)
        df.how_far(index,all_dates,0.1)
        
        
def cdr_v5(start_date,end_date,save_to,overwrite=False):
    """ Downloads NOAA/NSIDC 25 km CDR v5 (Climate Data Record) passive microwave sea ice concentration product.

    NSIDC's v5 (r0) daily SMMR + SSM/I + SSMIS product on 25 km grid in netCDF-4 (HDF5) format. Product derived from
    3 channels. Daily data files contain the following, based on purely automated application and merging of the
    NASA Team (NT) and Bootstrap (BT) algorithms:
    - Nimbus-7 SMMR from 1978-10-25 onwards (interpolated to daily resolution from original every-other-day resolution)
    - DMSP SSM/I and SSMIS from 1987-07-10 onwards
    - AMSR2 for recent years ("prototype" fields only)

    Dates of missing (no data file) or partially missing (data file exists) data for Antarctic (from CDR v4 version):
    - 1982/08/04 - 1982/08/09 (corrupt/partially missing)
    - 1984/08/12 - 1984/08/24 (missing)
    - 1984/08/25 - 1984/08/26 (corrupt/partially missing)
    - 1985/08/04 (corrupt/partially missing)
    - 1985/08/05 - 1985/08/09 (missing)
    - 1985/08/10 - 1985/08/11 (corrupt/partially missing)
    - 1986/03/30 - 1986/04/08 (corrupt/partially missing)
    - 1986/12/04 - 1986/12/10 (missing)
    - 1986/12/11 - 1986/12/12 (corrupt/partially missing)
    - 1987/12/03 – 1988/01/13 (missing)
    - 1990/12/26 – 1990/12/27 (corrupt/partially missing)
    - 2008/03/24 – 2008/03/25 (missing) [not missing in CDR v5 version]
    - 2008/03/26 (corrupt/partially missing) [not missing in CDR v5 version]

    Data page (defaults to latest version): https://nsidc.org/data/g02202/

    Documentation: https://nsidc.org/sites/default/files/documents/user-guide/g02202-v005-userguide.pdf

    Example file path:
        https://noaadata.apps.nsidc.org/NOAA/G02202_V5/south/daily/1978/sic_pss25_19781025_n07_v05r00.nc

    Expert guidance on these records:
        https://climatedataguide.ucar.edu/climate-data/sea-ice-concentration-noaansidc-climate-data-record

    Required citation:
        Meier, W. N., Fetterer, F., Windnagel, A. K., Stewart, J. S. & Stafford, T. (2024). NOAA/NSIDC Climate Data 
        Record of Passive Microwave Sea Ice Concentration. (G02202, Version 5). [Data Set]. Boulder, Colorado USA. 
        National Snow and Ice Data Center. https://doi.org/10.7265/rjzb-pf78. [Date Accessed].

    """
    url_prefix = 'https://noaadata.apps.nsidc.org/NOAA/G02202_V5/south/daily/'
    filename_prefix = 'sic_pss25_'
    filename_suffix = '_v05r00.nc'

    sat_abbrevs = ['n07','F08','F11','F13','F17']
    sat_start_dates = [(1978,10,25),(1987,7,10),(1991,12,3),(1995,10,1),(2008,1,1)]
    sat_end_dates = [(1987,7,9),(1991,12,2),(1995,9,30),(2007,12,31),(2024,12,31)]

    all_dates = [(dt.year, dt.month, dt.day) for dt in pd.date_range(datetime(*start_date),datetime(*end_date))]

    starting_dir = os.getcwd()
    if starting_dir is not save_to:
        os.chdir(save_to)

    for index, d in enumerate(all_dates):
        print('>>> working on {0}'.format(str(d)))
        if not datetime(*sat_start_dates[0]) <= datetime(*d) <= datetime(*sat_end_dates[-1]):
            raise ValueError('Given date range exceeds hard-coded satellite date ranges.')
        for sat in range(0,len(sat_abbrevs)):
            if datetime(*sat_start_dates[sat]) <= datetime(*d) <= datetime(*sat_end_dates[sat]):
                sat_abbrev = sat_abbrevs[sat]
        filename = filename_prefix + '{0[0]}{0[1]:02d}{0[2]:02d}'.format(d) + '_' + sat_abbrev + filename_suffix

        df.single_file(url_prefix + '{0[0]}/'.format(d),filename,save_to,overwrite=overwrite)
        df.how_far(index,all_dates,0.1)

    os.chdir(starting_dir)        


def nsidc_amsr_snow_depth(datetime_range,save_to,amsr_12km_gridfile,amsr_12km_areafile,conversion_script_dir,
                          stored_auth=True,overwrite=False,verbose=True):
    """ Downloads NSIDC Level-3 AMSR-E and AMSR2 12.5 km Southern Hemisphere snow depth on sea ice data.

    Daily data represent 5-day running means, based on the current day and the previous 4 days.

    Description assembled from NSIDC resources: The AMSR-E snow-depth-on-sea-ice algorithm was developed using
    DMSP SSMI data (Markus and Cavalieri 1998). Snow depth on sea ice is calculated using the spectral gradient ratio
    of the 18.7 GHz and 37 GHz vertical polarization channels. The correlation of regional in situ snow depth
    distributions and satellite-derived snow depth distributions was found to be 0.81. The upper limit for snow depth
    retrievals is (approximately?) 50 cm, which is a result of the limited penetration depth at 18.7 and 36.5 GHz.
    The algorithm is applicable to dry snow conditions only. Multiyear ice has a signature similar to snow cover on
    first-year ice; in the Antarctic, the algorithm retrieves snow depth everywhere, but in the Arctic, the algorithm
    only retrieves snow depth in the seasonal sea ice zones where multiyear ice concentration is less than about
    20 percent. In the Arctic only: because of the higher sensitivity of snow depth retrievals to SIC < 20%, the
    algorithm limits snow depth retrievals to SIC between 20-100%. Because of the uncertainties in grain size and
    density variations as well as sporadic weather effects, AMSR daily snow depth products are five-day running average;
    snow depth over sea ice is reported as a 5-day running average, which is based on the current day and the previous
    four days.

    Args:
        datetime_range: [start_datetime,end_datetime] - list or tuple of Datetime range to download (ends inclusive)
        save_to: filepath for directory to save data
        amsr_12km_gridfile: lon/lat grid filepath (file likely named 'LongitudeLatitudeGrid-s12500-Antarctic.h5')
        amsr_12km_areafile: pixel area filepath (file likely named 'pss12area_v3.dat')
        conversion_script_dir: filepath of .hdf to .h5 conversion executable
        stored_auth: True (default) to use NASA Earthdata credentials stored in ~/.bash_profile
                     (accessible from Python scripts, but not Jupyter notebooks)
                     or False to prompt user for login info (see df.nasa_auth() for more details)
        overwrite: False (default) to leave existing data files in place; True to overwrite
        verbose: True (default) to print files downloaded; False to stay silent

    Script has no return argument, but exports daily xarray-ready NetCDF files and deletes original raw data files.

    Product information and required citations:
        AMSR-E: https://nsidc.org/data/AE_SI12/versions/3
        AMSR2: https://nsidc.org/data/AU_SI12/versions/1

    """
    # load AMSR-E/2 12 km polar stereographic grid and pixel area files
    amsr_12km_grid = ldp.load_amsr_grid(amsr_12km_gridfile,amsr_12km_areafile,load_12_not_6=True)

    # authenticate with NASA Earthdata; wait for user input
    nasa_auth_session = df.nasa_auth(stored_auth=stored_auth)

    # iterate over daily data in range
    for dt in pd.date_range(*datetime_range):
        if verbose: print('>>> dlp.nsidc_amsr_snow_depth() is working on {0}{1:02}{2:02}'.format(dt.year,dt.month,dt.day))

        new_filename = 'nsidc_amsr_sh_snow_depth_{0}{1:02}{2:02}.nc'.format(dt.year,dt.month,dt.day)
        if os.path.isfile(save_to + new_filename) and (overwrite is False):
            if verbose: print('>>> Processed file already exists. Leaving in place...')
            continue

        # does this date correspond to AMSR-E or AMSR2?
        if dt.year <= 2011:
            which_amsr = 'AMSR-E'
        elif dt.year >= 2012:
            which_amsr = 'AMSR2'

        # construct data file URL
        if which_amsr == 'AMSR-E':
            url_dir = 'https://n5eil01u.ecs.nsidc.org/AMSA/AE_SI12.003/' + \
                      '{0}.{1:02}.{2:02}/'.format(dt.year,dt.month,dt.day)
            filename = 'AMSR_E_L3_SeaIce12km_V15_{0}{1:02}{2:02}.hdf'.format(dt.year,dt.month,dt.day)
        elif which_amsr == 'AMSR2':
            url_dir = 'https://n5eil01u.ecs.nsidc.org/AMSA/AU_SI12.001/' + \
                      '{0}.{1:02}.{2:02}/'.format(dt.year,dt.month,dt.day)
            filename = 'AMSR_U2_L3_SeaIce12km_B04_{0}{1:02}{2:02}.he5'.format(dt.year,dt.month,dt.day)

        # check if file exists; if not, continue
        response = nasa_auth_session.head(url_dir + filename)
        if response.status_code != 200:
            print('>>> dlp.nsidc_amsr_snow_depth() did not find a valid data file for this date')
            continue

        # download full (~125 MB for AMSR2; ~60 MB for AMSR-E) data file with fields not needed
        df.single_file(url_dir,filename,save_to,overwrite=overwrite,verbose=True,
                       nasa_auth_session=nasa_auth_session)

        # lengthy data description to save
        desc = '5-day running average based on current day and previous 4 days; ' \
               'NaN values may be missing data, land, open water, multiyear ice (Arctic only), ' \
               'snow melt, or periods with variability in snow depth'

        # load file, extract snow data
        if which_amsr == 'AMSR-E':
            product_desc = 'NSIDC AMSR-E/Aqua Daily L3 12.5 km Brightness Temperature, Sea Ice ' \
                           'Concentration, & Snow Depth Polar Grids, Version 3'
            # convert from .HDF to .h5 format; update filename
            df.convert_to_hdf5(conversion_script_dir,filename,save_to,save_to,overwrite=overwrite,delete_original=True)
            filename = filename[:-4] + '.h5'
            data_xr = xr.open_dataset(save_to + filename,engine='h5netcdf',
                                      group='/SpPolarGrid12km/Data Fields')
        elif which_amsr == 'AMSR2':
            product_desc = 'NSIDC AMSR-E/AMSR2 Unified L3 Daily 12.5 km Brightness Temperatures, ' \
                           'Sea Ice Concentration, Motion & Snow Depth Polar Grids, Version 1'
            data_xr = xr.open_dataset(save_to + filename,engine='h5netcdf',
                                      group='/HDFEOS/GRIDS/SpPolarGrid12km/Data Fields')

        # add data and coordinates to new xarray DataArray
        data_xr = data_xr['SI_12km_SH_SNOWDEPTH_5DAY']
        data = expand_dims(flipud(data_xr).astype(float),axis=2)
        data[logical_or.reduce((data == -1,data == 110,data == 120,data == 130,
                                data == 140,data == 150,data == 160))] = nan
        new_data_xr = xr.DataArray(data=data,dims=['x','y','time'],
                                   coords=dict(lon=(['x','y'],amsr_12km_grid['lons']),
                                               lat=(['x','y'],amsr_12km_grid['lats']),
                                               time=([dt.to_pydatetime()])),
                                   attrs=dict(name='snow depth',
                                              long_name='5-day snow depth on sea ice',
                                              units='cm',
                                              notes=desc,
                                              product=product_desc),
                                   name='snow_depth')

        # merge in metadata from original file (AMSR2 only; AMSR-E metadata is a mess)
        if which_amsr == 'AMSR2':
            new_data_xr.attrs.update(xr.open_dataset(save_to + filename,engine='h5netcdf').attrs)

        # save formatted data and delete original file
        if os.path.isfile(save_to + new_filename): os.remove(save_to + new_filename)
        new_data_xr.to_netcdf(save_to + new_filename)
        os.remove(save_to + filename)

    if verbose: print('>>> dlp.nsidc_amsr_snow_depth() has finished this series of downloads!')


def pathfinder(save_to,overwrite=False,verbose=True,stored_auth=True,start_year=1978,end_year=2023):
    """ Downloads NSIDC Polar Pathfinder daily 25 km sea ice motion product (version 4.1, new as of April 2019).

    Arguments:
        overwrite: False (default) to leave existing data files in place; True to overwrite
        verbose: True (default) to print files downloaded; False to stay silent
        stored_auth: True (default) to use NASA Earthdata credentials stored in ~/.bash_profile
             (accessible from Python scripts, but not Jupyter notebooks)
             or False to prompt user for login info (see df.nasa_auth() for more details)

    Provided in netCDF format on an EASE grid at a resolution of 25 km, although input data resolution varies and may
    be coarser than 25 km. For the Antarctic, the only input data are the following:
        - SMMR (October 25, 1978 - July 8, 1987) [available every other day]
        - SSM/I (July 9, 1987 - December 31, 2006) [daily]
        - SSMIS (January 1, 2007 - present) [daily]
        - AVHRR (July 24, 1981 - December 31, 2000) [4 satellite passes used per day when available]

    Documentation: https://nsidc.org/data/NSIDC-0116/versions/4

    Example file path: https://daacdata.apps.nsidc.org/pub/DATASETS/nsidc0116_icemotion_vectors_v4/south/daily/ ...
                       icemotion_daily_sh_25km_20180101_20181231_v4.1.nc

    Acknowledgement/citation:
        Tschudi, M., W. N. Meier, J. S. Stewart, C. Fowler, and J. Maslanik. 2019. Polar Pathfinder Daily 25 km
        EASE-Grid Sea Ice Motion Vectors, Version 4. Boulder, Colorado USA. NASA National Snow and Ice Data Center
        Distributed Active Archive Center. doi: https://doi.org/10.5067/INAWUWO7QH7B.

    """
    # authenticate with NASA Earthdata; wait for user input
    nasa_auth_session = df.nasa_auth(stored_auth=stored_auth,which_nasa='daacdata')

    # data files
    url_root = 'https://daacdata.apps.nsidc.org/pub/DATASETS/nsidc0116_icemotion_vectors_v4/south/daily/'
    filename_prefix = 'icemotion_daily_sh_25km_'
    filename_suffix = '_v4.1.nc'

    all_filenames = []
    years = arange(start_year,end_year+1)
    for year in years:
        if year == 1978:
            url_dates = '19781101_19781231'
        else:
            url_dates = '{0}0101_{0}1231'.format(year)
        all_filenames.append(filename_prefix + url_dates + filename_suffix)

    for index, filename in enumerate(all_filenames):
        df.single_file(url_root,filename,save_to,overwrite=overwrite,verbose=verbose,
                       nasa_auth_session=nasa_auth_session)
        df.how_far(index,all_filenames,0.1)
    if verbose: print('>>> Download has finished!')


def pathfinder_ql(save_to,verbose=True,stored_auth=False):
    """ Downloads NSIDC Polar Pathfinder 'Quicklook' (recent/preliminary) weekly 25 km sea ice motion product.

    Arguments:
        verbose: True (default) to print files downloaded; False to stay silent
        stored_auth: True (default) to use NASA Earthdata credentials stored in ~/.bash_profile
             (accessible from Python scripts, but not Jupyter notebooks)
             or False to prompt user for login info (see df.nasa_auth() for more details)

    Provided in netCDF format on an EASE grid at a resolution of 25 km.

    Documentation: https://nsidc.org/data/nsidc-0748/versions/1

    Example file path: https://daacdata.apps.nsidc.org/pub/DATASETS/nsidc0748_ql_icemotion/ ...
                       icemotion_weekly_sh_25km_20230101_20240303_ql.nc

    Acknowledgement/citation:
        Tschudi, M., W. N. Meier, and J. S. Stewart. 2019. Quicklook Arctic Weekly EASE-Grid Sea Ice
        Motion Vectors, Version 1. [Indicate subset used]. Boulder, Colorado USA. NASA National Snow and
        Ice Data Center Distributed Active Archive Center. https://doi.org/10.5067/O0XI8PPYEZJ6. [Date Accessed].

    """
    # authenticate with NASA Earthdata; wait for user input
    nasa_auth_session = df.nasa_auth(stored_auth=stored_auth,which_nasa='daacdata')

    # data files
    url_root = 'https://daacdata.apps.nsidc.org/pub/DATASETS/nsidc0748_ql_icemotion/'
    filename_prefix = 'icemotion_weekly_sh_25km_'
    filename_suffix = '_ql.nc'

    # identify single filename
    page_text = nasa_auth_session.get(url_root).text.split('"')
    matches = [excerpt for excerpt in page_text if (excerpt.startswith(filename_prefix) and \
                                                    excerpt.endswith(filename_suffix))]
    filename = matches[0]

    # download
    df.single_file(url_root,filename,save_to,overwrite=True,verbose=verbose,
                   nasa_auth_session=nasa_auth_session)
    if verbose: print('>>> Download has finished!')


def era5(area=[-50,-180,-80,180],years=[str(yr) for yr in range(2003,2024+1)],variables=[None],download_dir=None,
         batch=False,legacy_netcdf=False):
    """ Submits CDS API request to retrieve ERA5 reanalysis fields (0.25° x 0.25° grid) as netCDF file.
    
    NOTE: if downloading variables not on the example list below, then make sure to update their GRIB1/GRIB2 variable name
          and 'long_name' attribute correspondence in ldp.load_era5(). To do this, find their GRIB1 names on the ERA5 data
          documentation (https://confluence.ecmwf.int/display/CKB/ERA5%3A+data+documentation) and click the link to find
          the GRIB2 names in the ECMWF parameter database.

    Arguments:
        'area': [North, West, South, East] in °N or °E (-180 to 180)
        'years': list of years given as strings
        'variables': list of variables given as strings; the following are examples:
            '10m_u_component_of_wind'
            '10m_v_component_of_wind'
            '2m_dewpoint_temperature'
            '2m_temperature'
            'sea_surface_temperature' [*]
            'skin_temperature' [*]
            'mean_eastward_turbulent_surface_stress' [*]
            'mean_northward_turbulent_surface_stress' [*]
            'mean_evaporation_rate'
            'mean_total_precipitation_rate'
            'mean_snowfall_rate'
            'surface_pressure'
            'mean_sea_level_pressure' [*]
            'sea_ice_cover' [NOTE: now called 'sea_ice_area_fraction' with short name 'ci', not 'siconc', but old names may still work]
            'surface_latent_heat_flux' [*]
            'surface_sensible_heat_flux' [*]
            'surface_net_solar_radiation' [*]
            'surface_net_thermal_radiation' [*]
        'download_dir': None, to queue request and download from web browser
                        or directory path (including trailing backslash) to download directly into (for small requests)
        'batch': False (default) to display status of request sending / queued / running sequence
                 True to submit request, then disconnect from ECMWF to allow additional submissions (e.g., in a loop)
                 (note: if True, <download_dir> will default to behavior of None)
        'legacy_netcdf': False (default) to ask ECMWF to process files using current GRIB-to-netCDF4 converter
                         True to ask ECMWF to process files using legacy GRIB-to-netCDF3 converter; see ldp.load_era5() for more details

    Documentation:
        - general ERA5 documentation: https://confluence.ecmwf.int/display/CKB/ERA5+data+documentation
        - how to download: https://confluence.ecmwf.int/display/CKB/How+to+download+ERA5
        - parameter database: https://apps.ecmwf.int/codes/grib/param-db

    Cite using:
        Hersbach et al. (2020). The ERA5 global reanalysis, Quarterly Journal of the Royal Meteorological Society, TBD.
            doi:10.1002/qj.3803. https://onlinelibrary.wiley.com/doi/abs/10.1002/qj.3803
        Copernicus Climate Change Service (C3S) (2017). ERA5: Fifth generation of ECMWF atmospheric reanalyses of the
            global climate. Copernicus Climate Change Service Climate Data Store (CDS), date of access.
            https://cds.climate.copernicus.eu/cdsapp#!/home

    Important notes:
        - local installation of CDS API key is required! see details: https://cds.climate.copernicus.eu/how-to-api
        - for most (large) requests, after "Request is queued" appears, cancel using Ctrl-C and download from here:
          https://cds.climate.copernicus.eu/requests?tab=all
        - note that it seems that including years containing both validated ERA5 and preliminary ERA5T data within
          multiyear downloads is problematic, causing processing failures, so split up downloads, e.g. 2012-2018 then 2019
        - on MacOS, consider using a stand-alone download manager app, such as Download Shuttle, instead of a web
          browser download interface
        - on Linux terminal, download files from ECMWF CDS in background using wget -bqc <URL from ECMWF portal>
        - limit for requests used to be 120,000 "items" (where 1 hourly spatial field of 1 variable = 1 item), but may have decreased recently
        - known biases include high instantaneous surface stress; workaround is to use accumulated surface stress
          fields, as described here: https://confluence.ecmwf.int/display/CKB/ERA5+instantaneous+surface+stress+and+friction+velocity+over+the+oceans

    """
    time_str = str(datetime.now().date()) + '-' + str(datetime.now().time()).replace(':','-').replace('.','-')
    if download_dir is None: filepath = os.getcwd() + '/era5_download_{}.nc'.format(time_str)
    else:                    filepath = download_dir + 'era5_download_{}.nc'.format(time_str)

    if batch: filepath = None

    if not batch:
        c = cdsapi.Client()
    if batch:
        c = cdsapi.Client(wait_until_complete=False,delete=False,forget=True)
        
    if legacy_netcdf:
        netcdf_format = 'netcdf_legacy'
    else:
        netcdf_format = 'netcdf'
        
    c.retrieve('reanalysis-era5-single-levels', {
            'product_type':['reanalysis'],
            'format':netcdf_format,
            'download_format':'unarchived',
            'variable': variables,
            'year': years,
            'month': [
                '01','02','03',
                '04','05','06',
                '07','08','09',
                '10','11','12'],
            'day': [
                '01','02','03',
                '04','05','06',
                '07','08','09',
                '10','11','12',
                '13','14','15',
                '16','17','18',
                '19','20','21',
                '22','23','24',
                '25','26','27',
                '28','29','30',
                '31'],
            'time': [
                '00:00','01:00','02:00',
                '03:00','04:00','05:00',
                '06:00','07:00','08:00',
                '09:00','10:00','11:00',
                '12:00','13:00','14:00',
                '15:00','16:00','17:00',
                '18:00','19:00','20:00',
                '21:00','22:00','23:00'],
            'area': area,
    }, filepath)
