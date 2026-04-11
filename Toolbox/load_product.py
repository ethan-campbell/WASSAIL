# -*- coding: utf-8 -*-

import os
import shutil
import warnings
import h5py
from datetime import datetime, timedelta
from numpy import *
import pandas as pd
import xarray as xr
import dask

import geo_tools as gt
import time_tools as tt


def sea_ice_data_prep(nimbus5_dir,cdr_dir,cdr_nrt_dir,amsre_dir,amsr2_dir,
                      amsr_gridfile,amsr_areafile,nsidc_ps25_grid_dir):
    """ Returns meta-information on sea ice data to be fed to other accessor functions.

    Intended to be called before performing any sea ice analysis, e.g.:
        [sea_ice_grids,sea_ice_data_avail,sea_ice_all_dates] = ldp.sea_ice_data_prep(<ARGS ABOVE>)
        sea_ice_data_avail['nimbus5'][(1973,2,3)] = [<FILEPATH>,True]
        sea_ice_grids['nimbus5']['areas'] = <2D array by lat/lon>

    Returns:
        - grids: dictionary {'nimbus5','cdr','amsre','amsr2'}, where each entry is a dictionary of grid
          information {'lats','lons','areas'}, for which each entry is a 2D array
        - data_avail: dictionary {'nimbus5','cdr','amsre','amsr2'}, where each entry is a dictionary of
          date tuple keys (YYYY,MM,DD) returning [filepath, exists]
        - all_dates: simple list of date tuples from first satellite data to today

    """
    grids = {}
    grids['amsre'] = load_amsr_grid(amsr_gridfile, amsr_areafile)
    grids['amsr2'] = grids['amsre']
    grids['amsre_25km'] = load_amsr_grid(amsr_gridfile, amsr_areafile, regrid_to_25km=True)
    grids['amsr2_25km'] = grids['amsre_25km']
    grids['cdr'] = load_nsidc_ps_25km_grid(nsidc_ps25_grid_dir)
    grids['nimbus5'] = grids['cdr']

    all_dates = tt.dates_in_range((1972,12,12),tt.now())
    data_avail = {'nimbus5':{}, 'cdr':{}, 'amsre':{}, 'amsr2':{}}
    for index, d in enumerate(all_dates):
        data_avail['nimbus5'][d] = sea_ice_filename('nimbus5', d, nimbus5_dir, cdr_dir, cdr_nrt_dir, amsre_dir, amsr2_dir)
        data_avail['cdr'][d] = sea_ice_filename('cdr', d, nimbus5_dir, cdr_dir, cdr_nrt_dir, amsre_dir, amsr2_dir)
        data_avail['amsre'][d] = sea_ice_filename('amsre', d, nimbus5_dir, cdr_dir, cdr_nrt_dir, amsre_dir, amsr2_dir)
        data_avail['amsr2'][d] = sea_ice_filename('amsr2', d, nimbus5_dir, cdr_dir, cdr_nrt_dir, amsre_dir, amsr2_dir)

    return [grids,data_avail,all_dates]


def load_amsr(filepath,regrid_to_25km=False):
    """ Opens AMSR-E or AMSR2 data file and returns sea ice concentration (SIC).

    Arguments:
        filepath: data file location, including directory and filename
        regrid_to_25km: if True, regrid SIC onto NSIDC 25-km-square grid by averaging SIC of the 16 nearest
                                 6.25-km-square pixels to each location on the 25-km-square grid

    Returns:
        NumPy array of shape (1328, 1264), with concentrations from 0 to 100 (or NaN)

    """
    assert os.path.isfile(filepath), 'AMSR data file cannot be found at {0}.'.format(filepath)
    with h5py.File(filepath,'r') as data:
        ic_orig = data['ASI Ice Concentration'][:]
    if regrid_to_25km is not True:
        return ic_orig
    else:
        old_h = shape(ic_orig)[0]
        old_w = shape(ic_orig)[1]
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", category=RuntimeWarning)
            ic_regridded = nanmean(nanmean(ic_orig.reshape([old_h,old_w//4,4]),2).T.reshape(old_w//4,old_h//4,4),2).T
        return ic_regridded


def load_cdr(filepath,date,switch_to_nrt=(2024,12,31)):
    """ Opens NSIDC CDR or NRT CDR passive microwave sea ice concentration (SIC) data file and returns SIC.

    Arguments:
        filepath: data file location, including directory and filename
        date: date tuple (YYYY,MM,DD)
        switch_to_nrt: date tuple (YYYY,MM,DD) for last day of NOAA/NSIDC CDR data, after which this routine will
                       switch to NOAA/NSIDC Near Real-Time (NRT) CDR

    Returns:
        NumPy array of shape (332,316), with concentrations from 0 to 100 (or NaN, for land/coasts/missing data)

    Notes:
        - In the original file, missing data is encoded as 255, while land/coast/lake/pole hole masks are encoded as
          254, 253, 252, and 251, respectively.
        - This routine changes all of the above flags to NaN.

    """
    data_field = 'cdr_seaice_conc'     # variable name in CDR and NRT CDR

    assert os.path.isfile(filepath), 'SIC data file cannot be found at {0}.'.format(filepath)
    with h5py.File(filepath,'r') as data:
        ice_conc = data[data_field][0].astype(float32)
    ice_conc[logical_or.reduce((ice_conc == 251,ice_conc == 252,ice_conc == 253,ice_conc == 254, ice_conc == 255))] = nan
    return ice_conc


def load_pathfinder(data_dir,ql_version=False):
    """ Opens all NSIDC Polar Pathfinder daily 25 km sea ice motion data (concatenates files over time axis, yielding
        data from, e.g. 1978-present).

    Arguments:
        data_dir: filepath to data directory containing all Polar Pathfinder netCDF files, with trailing backslash
        ql_version: False (default) to compile and load daily Polar Pathfinder files
                    True to load single Polar Pathfinder Quicklook recent/preliminary weekly data file

    Returns: xarray Dataset including variables:
        'u' and 'v': eastward and northward components of ice motion (units: cm/s)
            note: these are derived from original EASE grid-oriented u and v using rotation matrix
        'u_grid' and 'v_grid': original EASE grid-oriented u and v (units: cm/s)
        'icemotion_error_estimate': estimated vector error (standard deviation; units: cm/s) from interpolation
            note: normal values are around 0-10 cm/s
            note: negative errors indicate vector is very near coast, and values of 100 have been added when closest
                  input vector to interpolated value is greater than 1250 km; thus, could screen for questionable
                  values by masking u and v values with icemotion_error_estimate > 100 or < 0
        'latitude' and 'longitude'

    For more info, see also download_product routine. Documentation here:
        https://nsidc.org/data/NSIDC-0116/versions/4

    Information about EASE Grid:
        https://nsidc.org/data/ease

    """
    if not ql_version:
        # load Polar Pathfinder daily data
        all_data = xr.open_mfdataset(data_dir + '*.nc',concat_dim='time',combine='nested')

        # drop unnecessary time dimension from lat and lon, noting that:
        #   all(drift_data.isel(time=100).longitude.values == drift_data.isel(time=1000).longitude.values) is True
        all_data['latitude'] = all_data['latitude'].isel(time=0).drop_vars('time')
        all_data['longitude'] = all_data['longitude'].isel(time=0).drop_vars('time')
    else:
        # load Polar Pathfinder Quicklook weekly data
        all_data = xr.open_mfdataset(data_dir + 'icemotion_weekly_sh_25km_*.nc')

    # drop unnecessary variable
    all_data = all_data.drop_vars('crs')

    # re-orient motion vectors in east/north U/V frame using SH rotation matrix, described here:
    #   https://nsidc.org/support/how/how-convert-horizontal-and-vertical-components-east-and-north
    all_data['u_grid'] = all_data['u']
    all_data['v_grid'] = all_data['v']
    cos_lon = cos(all_data['longitude']*2*pi/360)
    sin_lon = sin(all_data['longitude']*2*pi/360)
    all_data['u'] = all_data['u_grid'] * cos_lon - all_data['v_grid'] * sin_lon
    all_data['v'] = all_data['u_grid'] * sin_lon + all_data['v_grid'] * cos_lon

    # process error estimates: convert into cm/s, per example given under "Calculate Daily Error Values" here:
    #   https://nsidc.org/data/NSIDC-0116/versions/4
    if not ql_version:
        all_data['icemotion_error_estimate'] = all_data['icemotion_error_estimate'] / 10

    # convert time index to useable form
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        all_data['time'] = xr.CFTimeIndex(all_data['time'].values).to_datetimeindex()

    return all_data


def load_era5(data_dir,process_and_export=False,datetime_range=None,lat_range=None,lon_range=None,
              time_chunk=None,lat_chunk=None,lon_chunk=None,rechunk=False,use_grib2_names=False):
    """ Opens ERA5 reanalysis data files downloaded in netCDF format.

    Args:
        data_dir: directory of all data files (all files ending in '.nc' will be loaded)
        process_and_export: False (default) to simply load all files, assuming they've been processed already
                            True to do two processing tasks:
                                (1) assess all available files and merge validated ERA5 data with preliminary
                                    ERA5T data when necessary, then export the resulting file and move the original
                                (2) assess all available files and split files with more than one variable into
                                    separate exported files, then move the original to the "To delete" directory
            note: no Dataset will be returned if True
        datetime_range: None or [Datetime0,Datetime1] or [Datestring0,Datestring1] to subset fields
            note: to slice with open right end, e.g., use [Datetime0,None]
            note: selection is generous, so ['2016-1-1','2016-1-1'] will include all hours on January 1, 2016
            note: example of Datestring: '2016-1-1-h12' or '2016-01-01 12:00'
        lat_range: None or [lat_N,lat_S] to subset fields (!!! - descending order - !!!)
        lon_range: None or [lon_W,lon_E] to subset fields
        time_chunk, lat_chunk, lon_chunk: specify chunk sizes (otherwise use lat_chunk=20, lon_chunk=100, and
                                          compute time_chunk such that chunks are 1-5 MB each)
        rechunk: re-chunk with specified or computed chunk sizes (useful if time indices vary across files)
        use_grib2_names: False to use GRIB1 naming conventions, renaming all variables (and their 'long_name' attribute)
                             whose names were changed in ECMWF's migration from GRIB1 to GRIB2
                         True to use GRIB2 naming conventions, similarly renaming those changed from GRIB1

    Returns:
        all_data: xarray Dataset with coordinates (time,lats,lons); examples of accessing/slicing follow:
            all_data.loc[dict(time='2016-1-1')]                            to extract without slicing
            all_data.sel(lats=slice(-60,-70))                              to slice all variables
            all_data['skt'].values                                         to convert to NumPy array in memory
            all_data['skt'][0,:,:]  or  all_data.isel(time=0)              to slice data using indices (t=0)
            all_data['skt'].loc['2016-1-1':'2016-2-1',-60:-70,0:10]        to slice data using values (not recommended)
            all_data['skt']['latitude']                                    to get view of 1-D coordinate
            all_data['skt']['time']                                        NumPy Datetime coordinate
            all_data['skt']['doy']                                         fractional day-of-year coordinate
            pd.to_datetime(all_data['skt']['time'].values)                 useable Datetime version of the above
            all_data['skt'].attrs['units']
            all_data['skt'].attrs['long_name']

    Note: as shown above, 'doy' (fractional day-of-year) is included as a secondary coordinate with dimension 'time'.
    
    Note: after ECMWF's migration to a new GRIB to netCDF4 converter in late 2024, xarray I/O with the resulting netCDF4 files is extremely slow
          compared to the previous netCDF3 files. This seems to be related to poor chunking decisions in their converter settings (see threads
          linked below). This slow I/O be confirmed by timing simple .sel().values operations for a subset of data (see below). The only solution if
          staying with the new netCDF4 files would likely be using ncks or a similar utility to pre-process all ERA5 files with a more optimized
          chunking scheme before loading in xarray. An alternative could be requesting the original GRIB files from ECMWF, which xarray can load.
          In the interim, ECMWF has provided an option to request the legacy netCDF3 files, which is an option in dlp.era5(). This option may
          eventually be removed by ECMWF (and the netCDF3 files are "not supported" in any case) so this issue should be revisited at a later date.
          Likely related to this, time_chunk values computed in this routine for the new netCDF4 files were approx. 800, while they should be ~300
          for optimal performance.
          
          TESTING ROUTINE: the following timing test for any date of data should give an elapsed time of 0.2-0.4 seconds:
          >>> timer_start = time.time()
          >>> era5['mtpr'].sel(time='2025-03-28T00').sel(lons=(-50,50),lats=(-70,-65)).mean().values
          >>> timer_end = time.time()
          >>> print('elapsed time: {0:.1f} s'.format(timer_end - timer_start))
          
          - ECMWF info page: https://confluence.ecmwf.int/display/CKB/GRIB+to+netCDF+conversion+on+new+CDS+and+ADS+systems#GRIBtonetCDFconversiononnewCDSandADSsystems-ERA5
          - See comment by Tristan Schuler on this thread: https://forum.ecmwf.int/t/changes-to-grib-to-netcdf-converter-on-cds-beta-ads-beta/4322/35?page=2
          - See comment by Kenneth Bowman on this thread: https://forum.ecmwf.int/t/forthcoming-update-to-the-format-of-netcdf-files-produced-by-the-conversion-of-grib-data-on-the-cds/7772/4
          - And see this thread: https://code.mpimet.mpg.de/boards/2/topics/16082

    The following derived quantities are calculated here, to be evaluated lazily using Dask:
        'q2m': 2-m specific humidity from 'msl' and 'd2m'
        'si10': 10-m wind speed from 'u10' and 'v10'

    """
    name_list = array([('mer','avg_ie','Mean evaporation rate','Time-mean moisture flux'),
                       ('mtpr','avg_tprate','Mean total precipitation rate','Time-mean total precipitation rate'),
                       ('msr','avg_tsrwe','Mean snowfall rate','Time-mean total snowfall rate water equivalent'),
                       ('metss','avg_iews','Mean eastward turbulent surface stress','Time-mean eastward turbulent surface stress'),
                       ('mntss','avg_inss','Mean northward turbulent surface stress','Time-mean northward turbulent surface stress'),
                       ('slhf','slhf','Surface latent heat flux','Time-integrated surface latent heat net flux'),
                       ('sshf','sshf','Surface sensible heat flux','Time-integrated surface sensible heat net flux'),
                       ('ssr','ssr','Surface net solar radiation','Surface net short-wave (solar) radiation'),
                       ('str','str','Surface net thermal radiation','Surface net long-wave (thermal) radiation')])
    grib_rename = pd.DataFrame(data=name_list,columns=['GRIB1_var','GRIB2_var','GRIB1_long','GRIB2_long'])
    if use_grib2_names: grib_rename = grib_rename.set_index('GRIB1_var'); grib_str = 'GRIB2'  # prepare to change any GRIB1 names
    else:               grib_rename = grib_rename.set_index('GRIB2_var'); grib_str = 'GRIB1'  # prepare to change any GRIB2 names

    # list all files in directory
    all_filenames = os.listdir(data_dir)
    netcdf_filenames = []
    for filename in all_filenames:
        if '.nc' in filename: netcdf_filenames.append(filename)    # ignore subdirectories and '.DS_Store' file
    all_filenames = netcdf_filenames

    if time_chunk is None and lat_chunk is None and lon_chunk is None:
        # compute chunk size for Dask; aiming for each chunk to be between 1-5 MB
        # total chunk size computed here as (0.0025 GB) * len(data.time) / filesize, using first file in directory
        first_filename = all_filenames[0]
        first_file_size = os.path.getsize(data_dir + first_filename) / 1E9     # in GB
        first_file_data = xr.open_dataset(data_dir + first_filename,chunks={})
        if 'valid_time' in first_file_data.variables: first_file_data = first_file_data.rename({'valid_time':'time'})
        lat_chunk = 20
        N_lat_chunks = len(first_file_data.latitude) / lat_chunk
        lon_chunk = 100
        N_lon_chunks = len(first_file_data.longitude) / lon_chunk
        time_chunk = int(N_lat_chunks * N_lon_chunks * 0.0025 * len(first_file_data.time) / first_file_size)
        if time_chunk < 10 or time_chunk > 2500: 'Caution from ldp.load_era5(): revisit chunk length calculation'
        first_file_data.close()

    if process_and_export:
        # create reverse look-up table of variable abbreviations and long names (only necessary because files with both
        #   validated ERA5 and preliminary ERA5T data and a single variable [?] are missing the variable abbreviation)
        var_name_lookup = {}
        for filename in all_filenames:
            data = xr.open_dataset(data_dir + filename,chunks={})
            assert ('p0001' in data.variables) == ('p0005' in data.variables), \
                'Error from ldp.load_era5(): variables not recognized as either from validated ERA5 or preliminary ' \
                'ERA5T in filename {0}'.format(filename)
            if 'p0001' not in data.variables and 'p0005' not in data.variables:
                for var_abbrev in data.data_vars:
                    var_name_lookup[data[var_abbrev].long_name] = data[var_abbrev].name
            data.close()

        # process files containing both validated ERA5 and preliminary ERA5T data (merge the two and export)
        for f_idx, filename in enumerate(all_filenames):
            print('ldp.load_era5() is checking if file {0} of {1} needs to be merged'.format(f_idx+1,len(all_filenames)))
            data = xr.open_dataset(data_dir + filename,
                                   chunks={'valid_time':time_chunk,'time':time_chunk,'latitude':lat_chunk,'longitude':lon_chunk})
            if 'valid_time' in data.variables:
                data = data.rename({'valid_time':'time'})
            if 'expver' in data.variables:
                if data['expver'].dims[0] == 'expver':   # legacy format from GRIB-to-netCDF converter for files with both ERA5 and ERA5T data
                    data = data.sel(expver=1).combine_first(data.sel(expver=5))   # best way
                    # data = data.reduce(nansum,'expver')                         # more risky way
                    data.to_netcdf(data_dir + filename.rstrip('.nc') + '_ERA5_ERA5T_legacy_v2_merged.nc')
                    data.close()
                    if 'To delete' not in os.listdir(data_dir): os.mkdir(data_dir + 'To delete/')
                    _ = shutil.move(data_dir + filename,data_dir + 'To delete/' + filename)
                elif data['expver'].dims[0] == 'time':   # current format from GRIB-to-netCDF converter for files with both ERA5 and ERA5T data
                    data.close()
                else:
                    print('ERROR: ERA5 file is in an unrecognized format – check how merged ERA5/ERA5T data files are currently being handled by ECMWF')
                    data.close()
            elif 'p0001' in data.variables and 'p0005' in data.variables:   # likely only applies to super-legacy format for ERA5 files (two generations ago)
                era5_validated = data['p0001'].dropna('time',how='all')
                era5_validated.name = var_name_lookup[era5_validated.long_name]
                era5t_prelim = data['p0005'].dropna('time',how='all')
                era5t_prelim.name = var_name_lookup[era5t_prelim.long_name]
                dataarray = xr.concat([era5_validated,era5t_prelim],dim='time')
                dataarray \
                    = dataarray.chunk({'time':time_chunk,'latitude':lat_chunk,'longitude':lon_chunk})  # just in case
                # check for weird case where arrays at some times are filled with the add_offset value instead of NaNs
                if len(unique(dataarray.time)) != len(dataarray.time):
                    unique_times,unique_idx = unique(dataarray.time,return_index=True)
                    dataarray = dataarray.isel(time=unique_idx)  # ignore second occurrences of each non-unique time
                data = dataarray.to_dataset()
                data.to_netcdf(data_dir + filename.rstrip('.nc') + '_ERA5_ERA5T_legacy_v1_merged.nc')
                data.close()
                if 'To delete' not in os.listdir(data_dir): os.mkdir(data_dir + 'To delete/')
                _ = shutil.move(data_dir + filename,data_dir + 'To delete/' + filename)
            else:
                data.close()

        # split up files containing multiple variables (without this, open_mfdataset() will hang and cannot finish)
        all_filenames = os.listdir(data_dir)
        for filename in all_filenames:
            if '.nc' in filename:
                data = xr.open_dataset(data_dir + filename,
                                       chunks={'time':time_chunk,'latitude':lat_chunk,'longitude':lon_chunk})
                if len(data.data_vars) > 1:
                    for var_name in data.data_vars:
                        print('ldp.load_era5() is splitting variable {0} from file {1}'.format(var_name,filename))
                        data_subset = data[var_name].to_dataset()
                        data_subset.to_netcdf(data_dir + filename.rstrip('.nc') + '_split_{0}.nc'.format(var_name))
                    data.close()
                    if 'To delete' not in os.listdir(data_dir): os.mkdir(data_dir + 'To delete/')
                    _ = shutil.move(data_dir + filename,data_dir + 'To delete/' + filename)
        
        print('>>> ldp.era5() is finished!')
        return

    # load all files in directory
    # catch PerformanceWarning about "Increasing number of chunks by factor of X"; re-chunking makes this irrelevant
    with warnings.catch_warnings():
        warnings.simplefilter('ignore',dask.array.PerformanceWarning)
        def era5_preprocess(ds):
            if 'valid_time' in ds.coords:
                ds = ds.rename({'valid_time':'time'})
            if 'number' in ds.coords:
                ds = ds.drop('number')
            if 'expver' in ds.variables:
                if ds['expver'].dims[0] == 'time' or ds['expver'].dims[0] == 'valid_time':
                    ds = ds.drop('expver')
            for grib_name in grib_rename.index:
                if grib_name in ds.variables:
                    ds = ds.rename({grib_name:grib_rename.loc[grib_name][grib_str + '_var']})
                    ds[grib_rename.loc[grib_name][grib_str + '_var']].attrs['long_name'] = grib_rename.loc[grib_name][grib_str + '_long']
            # ds = ds.chunk({'time':time_chunk,'latitude':lat_chunk,'longitude':lon_chunk})
            return ds
        all_data = xr.open_mfdataset(data_dir + '*.nc',combine='by_coords',preprocess=era5_preprocess,
                                     chunks={'valid_time':time_chunk,'time':time_chunk,'latitude':lat_chunk,'longitude':lon_chunk})
        
    # re-chunk (sometimes necessary when combining if some time indices have different lengths)
    if rechunk:
        all_data = all_data.chunk({'time':time_chunk,'latitude':lat_chunk,'longitude':lon_chunk})

    # rename for convenience
    if 'longitude' in all_data and 'latitude' in all_data:
        all_data = all_data.rename({'latitude':'lats','longitude':'lons'})

    # slice dimensions
    if datetime_range is not None:
        all_data = all_data.sel(time=slice(datetime_range[0],datetime_range[1]))
    if lat_range is not None:
        all_data = all_data.sel(lats=slice(lat_range[0],lat_range[1]))
    if lon_range is not None:
        all_data = all_data.sel(lons=slice(lon_range[0],lon_range[1]))

    for var_abbrev in all_data.data_vars:
        # rename some variable long names and units for convenience, e.g. during plotting
        all_data[var_abbrev].attrs['long_name'] = all_data[var_abbrev].long_name.replace('metre','m')
        all_data[var_abbrev].attrs['units'] \
            = all_data[var_abbrev].units.replace('**-3','^{-3}').replace('**-2','^{-2}').replace('**-1','^{-1}') \
                .replace('**2','^{2}').replace('**3','^{3}')

        # revise units for convenience and/or deaccumulate
        # note: this evaluates lazily using Dask, so expect processing hangs upon computation (instead of load)
        if var_abbrev == 'e' or var_abbrev == 'mer' or var_abbrev == 'avg_ie': all_data[var_abbrev] *= -1
        if all_data[var_abbrev].attrs['units'] == 'Pa':
            orig_name = all_data[var_abbrev].attrs['long_name']
            all_data[var_abbrev] /= 100.0
            all_data[var_abbrev].attrs = {'units':'hPa','long_name':orig_name}
        elif all_data[var_abbrev].attrs['units'] == 'K' and var_abbrev != 'd2m':
            orig_name = all_data[var_abbrev].attrs['long_name']
            all_data[var_abbrev] -= 273.15
            all_data[var_abbrev].attrs = {'units':'°C','long_name':orig_name}
        elif all_data[var_abbrev].attrs['units'] == 'J m^{-2}':   # deaccumulate
            orig_name = all_data[var_abbrev].attrs['long_name']
            all_data[var_abbrev] /= (60.0 * 60.0)
            all_data[var_abbrev].attrs = {'units':'W m^{-2}','long_name':orig_name}

    # add day-of-year as a secondary coordinate with dimension 'time'
    if 'doy' not in all_data.coords:
        datetime_index = pd.to_datetime(all_data['time'].values)
        doy_index = datetime_index.dayofyear + datetime_index.hour / 24. + datetime_index.minute / 60.
        all_data.coords['doy'] = ('time',doy_index)

    # calculate 10-m wind speed from u, v
    # note: this evaluates lazily using Dask, so expect processing hangs upon computation (instead of load)
    if 'si10' not in all_data and 'u10' in all_data and 'v10' in all_data:
        all_data['si10'] = (all_data['u10']**2 + all_data['v10']**2)**0.5
        all_data['si10'].attrs['units'] = 'm s^{-1}'
        all_data['si10'].attrs['long_name'] = '10 m wind speed'

    # calculate 2-m specific humidity from surface pressure and dewpoint temperature, if available
    # note: this evaluates lazily using Dask, so expect processing hangs upon computation (instead of load)
    # uses Equations 7.4 and 7.5 on p. 92 of ECMWF IFS Documentation, Ch. 7:
    #   https://www.ecmwf.int/sites/default/files/elibrary/2015/9211-part-iv-physical-processes.pdf
    if 'q2m' not in all_data and 'd2m' in all_data and 'msl' in all_data:
        # constants for Teten's formula for saturation water vapor pressure over water [not ice] (Eq. 7.5)
        # origin: Buck (1981)
        a1 = 611.21 # Pa
        a3 = 17.502 # unitless
        a4 = 32.19  # K
        T_0 = 273.16 # K

        # saturation water vapor pressure; units: Pa
        e_sat_at_Td = a1 * exp(a3 * (all_data['d2m'] - T_0) / (all_data['d2m'] - a4))

        # saturation specific humidity at dewpoint temperature (Eq. 7.4)
        # note conversion of surface pressure from hPa back to Pa
        R_dry_over_R_vap = 0.621981  # gas constant for dry air over gas constant for water vapor, p. 110
        q_sat_at_Td = R_dry_over_R_vap * e_sat_at_Td / (100*all_data['msl'] - (e_sat_at_Td*(1.0 - R_dry_over_R_vap)))

        all_data['q2m'] = q_sat_at_Td
        all_data['q2m'].attrs['units'] = 'kg kg^{-1}'
        all_data['q2m'].attrs['long_name'] = 'Specific humidity at 2 m'

    return all_data


def load_snow_buoys(data_dir):
    """ Load and process data from AWI snow buoys drifting on Antarctic sea ice.

    Arguments:
        data_dir: filepath to buoy data directory

    Returns:
        buoy_data: dict with buoy IDs as keys, and xarray DataSet for each buoy's data with coord 'time' and variables:
                   - 'latitude (deg)'
                   - 'longitude (deg)'
                   - 'distance_to_initial_snow_ice_interface_1 (m)' (and '..._2', '..._3', and '..._4')
                   - 'distance_to_initial_snow_interface' (averaged values)
                   - 'barometric_pressure (hPa)'
                   - 'temperature_air (degC)'
                   - 'temperature_body (degC)'
                   - 'GPS_time_since_last_fix (min)'
        buoy_names: list of buoy IDs

    More information:
    - "The central element consists of four ultrasonic sensors, each of which measures its distance from the snow’s
      surface."
    - Buoys record lat/lon, distance to the initial snow/ice interface at approximately hourly resolution, and
      meteorological data.
    - This routine excludes 6 snow buoys deployed on fast ice in Atka Bay, Queen Maud Land, which remained relatively stationary:
         • Buoys 2013S1, 2013S2, 2014S24, 2017S49, 2017S54, 2021S89
    - The following buoys deployed on fast ice in Atka Bay are included due to their longer time series:
         • Buoy 2018S56, which obtained an approx. 1.5-year time series following its breakout from landfast ice
         • Buoy 2019S88, which obtained an approx. 0.5-year time series following its breakout from landfast ice
         • Buoy 2020S55, which obtained an approx. 1-year time series following its breakout from landfast ice
         • Buoy 2022S110, which obtained an approx. 0.5-year time series following its breakout from landfast ice
         • Buoy 2023S111, which obtained an approx. 0.5-year time series following its breakout from landfast ice
         • Buoy 2024S120, which obtained an approx. 0.5-year time series following its breakout from landfast ice
    - This routine includes buoys 2025S135, 2025S141, and 2025S144, despite their deployments being listed as "Unknown".
    - This routine excludes buoys 2025S142 and 2025S143 due to short time series (perhaps due to early failure).
    - This routine quality controls data from recent (active) buoys 2025S135, 2025S141, and 2025S144 by removing data spikes
      (defined from visual inspection as any data points ≥ 0.9 m).
    - This routine creates a new data column (<distance_to_initial_snow_interface>) with the calculated average of
      each buoy's four ultrasonic sensors, including for timestamps at which one or more sensors did not report data.
    - This routine also linearly interpolates over data gaps of up to 1 day in the averaged sensor time series.

    Data provenance:
        "Antarctic buoys (Dataset ZIP)" from https://data.meereisportal.de/relaunch/buoy.php?lang=en

    Cite as:
        Nicolaus, M.; Hoppmann, M.; Arndt, S.; Hendricks, S.; Katlein, C.; König-Langlo, G.; Nicolaus, A.; Rossmann, L.;
        Schiller, M.; Schwegmann, S.; Langevin, D.; Bartsch, A. (2017): Snow height and air temperature on sea ice from
        Snow Buoy measurements. Alfred Wegener Institute, Helmholtz Center for Polar and Marine Research, Bremerhaven,
        doi:10.1594/PANGAEA.875638.
    """
    buoy_list = pd.read_csv(data_dir + 'antarctic_buoy_list.csv')
    
    # exclude Atka Bay buoys deployed on fast ice, plus buoys with very short time series (from visual inspection)
    buoys_to_exclude = [row[1]['name'] for row in buoy_list.iterrows() if
                        ('ATKA' in str(row[1]['station']) or 'AFIN' in str(row[1]['station']))]
    buoys_to_exclude.extend(['2025S142','2025S143'])
    
    # explicitly include these buoys, despite their deployment on Atka Bay fast ice
    buoys_to_include = ['2018S56','2019S88','2020S55','2022S110','2023S111','2024S120']
    
    buoy_filenames = os.listdir(data_dir)
    buoy_filenames.sort()
    if 'antarctic_buoy_list.csv' in buoy_filenames: buoy_filenames.remove('antarctic_buoy_list.csv')
    buoy_data = dict();
    buoy_names = []
    for fn in buoy_filenames:
        buoy_id = fn.split('_')[0]
        if fn[4] == 'S' and '_proc.csv' in fn and ((buoy_id not in buoys_to_exclude) or (buoy_id in buoys_to_include)):
            buoy_data[buoy_id] = pd.read_csv(data_dir + fn,parse_dates=['time'],
                                             index_col='time').to_xarray().assign_attrs({'buoy_ID':buoy_id})
            
            # apply quality control to recent (active) snow buoys with data gaps and spikes
            if buoy_id in ['2025S135','2025S141','2025S144']:
                for sensor_num in arange(1,5):
                    buoy_data[buoy_id]['distance_to_initial_snow_ice_interface_{0} (m)'.format(sensor_num)] = \
                        xr.where(buoy_data[buoy_id]['distance_to_initial_snow_ice_interface_{0} (m)'.format(sensor_num)] >= 0.9,nan,
                                 buoy_data[buoy_id]['distance_to_initial_snow_ice_interface_{0} (m)'.format(sensor_num)])
            
            buoy_names.append(buoy_id)
            buoy_data[buoy_id]['distance_to_initial_snow_interface'] = \
                buoy_data[buoy_id][
                    ['distance_to_initial_snow_ice_interface_{0} (m)'.format(sensor_num) for sensor_num in arange(1,5)]] \
                    .to_array(dim='sensor').mean('sensor').interpolate_na(dim='time',method='linear',
                                                                          max_gap=timedelta(days=1))
    return buoy_data, buoy_names


def sea_ice_filename(sat_name,date,nimbus5_dir,cdr_dir,cdr_nrt_dir,amsre_dir,amsr2_dir,verbose=False):
    """ Returns full path (directory + filename) of a sea ice data file and checks for existence.

    Arguments:
        sat_name: 'amsr' (meaning AMSR2 or AMSR-E), 'amsr2', 'amsre', 'cdr_either' (meaning NSIDC CDR or NRT CDR),
                  'cdr', 'cdr_nrt', or 'nimbus5'
        date: tuple (YYYY,MM,DD)
        data directories: full paths to data folders

    Returns [filepath, exists], where:
        filepath: string of directory + filename
        exists: True or False (does the file exist?)
    """
    sat_abbrevs = ['n05','n07','F08','F11','F13','F17','F17_nrt',
                   'ame','am2']
    sat_start_dates = [(1972,12,12),(1978,10,25),(1987,7,10),(1991,12,3),(1995,10,1),(2008,1,1),(2025,1,1),
                       (2002,6,1),(2012,7,4)]
    sat_end_dates = [(1977,5,11),(1987,7,9),(1991,12,2),(1995,9,30),(2007,12,31),(2024,12,31),tt.now(),
                     (2011,10,4),tt.now()]   # note: okay if end date, e.g. tt.now(), is beyond actual end date

    # if specified satellite name ambiguous, figure out exactly which record to use
    if sat_name == 'amsr':
        if tt.is_time_in_range(sat_start_dates[sat_abbrevs.index('ame')], sat_end_dates[sat_abbrevs.index('ame')], date):
            sat_name = 'amsre'
        elif tt.is_time_in_range(sat_start_dates[sat_abbrevs.index('am2')], sat_end_dates[sat_abbrevs.index('am2')], date):
            sat_name = 'amsr2'
        else:
            if verbose:
                raise ValueError('Satellite name given as AMSR but given date not within AMSR-E or AMSR2 date ranges.')
            else:
                pass
    elif sat_name == 'cdr_either':
        if tt.is_time_in_range(sat_start_dates[sat_abbrevs.index('n07')], sat_end_dates[sat_abbrevs.index('F17')], date):
            sat_name = 'cdr'
        elif tt.is_time_in_range(sat_start_dates[sat_abbrevs.index('F17_nrt')], sat_end_dates[sat_abbrevs.index('F17_nrt')], date):
            sat_name = 'cdr_nrt'
        else:
            sat_name = 'cdr_ERROR'   # throwaway, since it will return exists = False regardless

    # construct filepath and and check for existence
    if sat_name == 'nimbus5':
        filename_part1 = 'ESMR-'
        filename_part2 = '.tse.00.h5'
        date_365 = tt.convert_date_to_365(date)
        filepath = nimbus5_dir + filename_part1 + '{0[0]}{1:03d}'.format(date, date_365) + filename_part2
        if not tt.is_time_in_range(sat_start_dates[sat_abbrevs.index('n05')], sat_end_dates[sat_abbrevs.index('n05')], date):
            exists = False
        else:
            exists = os.path.isfile(filepath)
    elif sat_name == 'cdr':
        filename_prefix = 'sic_pss25_'
        filename_suffix = '_v05r00.nc'
        sat_abbrev = 'NAN' # default value to create a meaningless filename for dates outside CDR range
        for sat in range(sat_abbrevs.index('n07'),sat_abbrevs.index('F17') + 1):
            if tt.is_time_in_range(sat_start_dates[sat], sat_end_dates[sat], date):
                sat_abbrev = sat_abbrevs[sat]
        filepath = cdr_dir + filename_prefix + '{0[0]}{0[1]:02d}{0[2]:02d}'.format(date) + '_' + sat_abbrev + filename_suffix
        if not tt.is_time_in_range(sat_start_dates[sat_abbrevs.index('n07')], sat_end_dates[sat_abbrevs.index('F17')], date):
            exists = False
        else:
            exists = os.path.isfile(filepath)
    elif sat_name == 'cdr_nrt':
        filename_prefix = 'sic_pss25_'
        filename_suffix = '_icdr_v03r00.nc'
        sat_abbrev = 'NAN' # default value to create a meaningless filename for dates outside CDR range
        for sat in range(sat_abbrevs.index('F17_nrt'),sat_abbrevs.index('F17_nrt') + 1):
            if tt.is_time_in_range(sat_start_dates[sat], sat_end_dates[sat], date):
                sat_abbrev = sat_abbrevs[sat][:-4]   # to strip away '_nrt' suffix on abbreviation
        filepath = cdr_nrt_dir + filename_prefix + '{0[0]}{0[1]:02d}{0[2]:02d}'.format(date) + '_' + sat_abbrev + filename_suffix
        if not tt.is_time_in_range(sat_start_dates[sat_abbrevs.index('F17_nrt')], sat_end_dates[sat_abbrevs.index('F17_nrt')], date):
            exists = False
        else:
            exists = os.path.isfile(filepath)
    elif sat_name == 'amsre':
        filename_part1 = 'asi-s6250-'
        filename_part2 = '-v5.4.h5'
        filepath = amsre_dir + filename_part1 + '{0[0]}{0[1]:02d}{0[2]:02d}'.format(date) + filename_part2
        if not tt.is_time_in_range(sat_start_dates[sat_abbrevs.index('ame')], sat_end_dates[sat_abbrevs.index('ame')], date):
            exists = False
        else:
            exists = os.path.isfile(filepath)
    elif sat_name == 'amsr2':
        filename_part1 = 'asi-AMSR2-s6250-'
        filename_part2 = '-v5.4.h5'
        filepath = amsr2_dir + filename_part1 + '{0[0]}{0[1]:02d}{0[2]:02d}'.format(date) + filename_part2
        if not tt.is_time_in_range(sat_start_dates[sat_abbrevs.index('am2')], sat_end_dates[sat_abbrevs.index('am2')], date):
            exists = False
        else:
            exists = os.path.isfile(filepath)
    else:
        if verbose:
            raise ValueError('Given satellite name does match those hard-coded in function.')
        else:
            return [None, False]

    return [filepath, exists]


def load_amsr_grid(grid_file,area_file,load_12_not_6=False,regrid_to_25km=False):
    """ Processes and returns AMSR-E/AMSR2 polar stereographic lat/lon grid and pixel areas (in km^2).

    Arguments:
        grid_file: filepath of lon/lat grid file converted to .h5 format
                   (e.g., 'LongitudeLatitudeGrid-s6250-Antarctic.h5')
        area_file: filepath of pixel area data file in .dat format
                   (e.g., 'pss06area_v3.dat')
        load_12_not_6: if False, load 6.25 km grid; if True, load 12.5 km grid
        regrid_to_25km: if True, return mean lat/lon of blocks of 16 grid cells from 6.25 km grid ONLY
                        (NOTE: this will not function properly for 12.5 km grid)
    
    Note: pixel area files were obtained from NSIDC polar stereographic tool website:
        http://nsidc.org/data/polar-stereo/tools_geo_pixel.html.

    """
    with h5py.File(grid_file,'r') as grid:
        grid_dict = {'lats':grid['Latitudes'][()], 'lons':grid['Longitudes'][()]}
    grid_dict['lons'] = gt.convert_360_lon_to_180(grid_dict['lons'])
    areas_flat = fromfile(area_file,dtype=int32) / 1000
    if load_12_not_6:   # assume 12.5 km grid file
        areas = reshape(areas_flat,(664,632))
    else:               # assume 6.25 km grid file
        areas = reshape(areas_flat,(1328,1264))
    areas = flipud(areas)
    grid_dict['areas'] = areas
    if regrid_to_25km is not True:
        return grid_dict
    else:
        old_h = shape(grid_dict['areas'])[0]
        old_w = shape(grid_dict['areas'])[1]
        grid_dict['lons'] = grid_dict['lons'].reshape([old_h,old_w//4,4]).mean(2).T.reshape(old_w//4,old_h//4,4).mean(2).T
        grid_dict['lats'] = grid_dict['lats'].reshape([old_h,old_w//4,4]).mean(2).T.reshape(old_w//4,old_h//4,4).mean(2).T
        grid_dict['areas'] = grid_dict['areas'].reshape([old_h,old_w//4,4]).sum(2).T.reshape(old_w//4,old_h//4,4).sum(2).T
        return grid_dict


def load_nsidc_ps_25km_grid(grid_dir):
    """ Processes and returns NSIDC 25 km polar stereographic lat/lon grid and pixel areas (in km^2).

    Applicable to Nimbus-5 and CDR (Nimbus-7/SMMR/DMSP) datasets.

    Further information here: http://nsidc.org/data/polar-stereo/tools_geo_pixel.html

    Lat/lon and area files downloaded from:
    ftp://sidads.colorado.edu/pub/DATASETS/brightness-temperatures/polar-stereo/tools/geo-coord/grid/

    """
    area_file = grid_dir + 'pss25area_v3.dat'
    lat_file = grid_dir + 'pss25lats_v3.dat'
    lon_file = grid_dir + 'pss25lons_v3.dat'

    grid_dict = {}
    lats_flat = fromfile(lat_file, dtype=int32) / 100000
    grid_dict['lats'] = reshape(lats_flat,(332,316))
    lons_flat = fromfile(lon_file, dtype=int32) / 100000
    grid_dict['lons'] = reshape(lons_flat,(332,316))
    areas_flat = fromfile(area_file, dtype=int32) / 1000
    grid_dict['areas'] = reshape(areas_flat,(332,316))
    return grid_dict
