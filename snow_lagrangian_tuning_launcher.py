import os
import subprocess
import sys
import time
from datetime import datetime
from numpy import *
import pandas as pd
import xarray as xr
from scipy import stats

# parameters
processed_model_output_dir_params = sys.argv[1]
this_code_dir = sys.argv[2]
env_addition = sys.argv[3]
n_test_workers = 54               # capacity based on number of server CPUs (i.e., on server with 64 CPUs, 54 workers would leave 10 cores unused)
                                  # (see below: adds 2 more workers to each rung [total = 56] for 'default' parameter runs)
best_performers = 5               # consider "best performers" as the 5 parameter sets with lowest RMSE (whose median values are used in final parameter selection)
update_params_tuning = True       # keep True except for testing
overwrite_params = False          # keep False except for testing
                                  # only change to True if starting from rung 0 using an existing exported parameter set (check control flow first)

# initial tuning rung
rung = 0

# load comparison data set
parcels_input = xr.open_dataset(processed_model_output_dir_params + 'parcels_input.nc').load()
parcels_input['parcel_id'] = parcels_input['parcel_id'].astype(str)
parcels_input_daily = parcels_input.resample(time='1D').mean()  # create daily average version of input data

# load data file with assignments for split between calibration vs. validation buoy data
buoy_assignments = pd.read_csv(processed_model_output_dir_params + 'buoy_split_assignments.csv',index_col=0)
buoy_names_training_set = buoy_assignments.index[buoy_assignments['for_training']].values
buoy_names_validation_set = buoy_assignments.index[~buoy_assignments['for_training']].values

# functions for randomization of test parameters
def truncnorm(center,scale,draws):
    # Calculates samples from a one-sided truncated normal distribution bounded by [0.0, Infinity]
    # 
    # Arguments: center = center value (similiar to, but not necessarily, the mean)
    #            scale = scale parameter (similar to, but not the same as, the standard deviation)
    #            draws = number of random values to draw
    if center == 0.0 and scale == 0.0:
        return tile(0.0,draws)
    else:
        return stats.truncnorm.rvs((0.0-center)/scale,inf,loc=center,scale=scale,size=draws)
    
def standard_norm(center,scale,draws):
    # Calculates samples from a standard normal continuous distribution
    #
    # Arguments: center = mean
    #            scale = standard deviation
    #            draws = number of random values to draw
    return stats.norm.rvs(center,scale,draws)

# assess performance of model parameters using bias when compared to snow buoy observations
# - NOTE: performance is assessed separately based on 'cal' data (subset of snow buoys assigned to model calibration) and 'val' data (buoys assigned to validation)
def assess_perf(rung):
    perf = pd.read_csv(processed_model_output_dir_params + 'rung{0}/snow_model_params_rung{0}.csv'.format(rung))
    files = os.listdir(processed_model_output_dir_params + 'rung{0}/'.format(rung))
    worker_counter = 0
    for w_idx, worker_num in enumerate(arange(len(perf))):
        if not any(['snow_model_output_rung{0}_worker{1}_'.format(rung,worker_num) in file for file in files]):
            continue
        worker_counter += 1
        parcels_worker = xr.open_mfdataset(processed_model_output_dir_params + 'rung{0}/snow_model_output_rung{0}_worker{1}_'.format(rung,worker_num) + '*.nc').load()
        parcels_worker['parcel_id'] = parcels_worker['parcel_id'].astype(str)
        parcel_nums = parcels_worker['parcel'].values
        parcel_ids = parcels_worker['parcel_id'].values

        # shift input buoy data set to align starting snow depths with those identified in model run
        # - also split shifted versions of <parcels_input> into: (1) calibration data set, and (2) validation data set 
        # - also omit any buoys that didn't result in a corresponding model run (e.g., those outside of time range of available ERA5 data)
        if worker_counter == 1:
            if 'parcels_input_daily_shifted' in locals():
                parcels_input_daily_shifted.close(); del parcels_input_daily_shifted
            parcels_input_daily_shifted = parcels_input_daily.load().copy()
            parcels_input_daily_shifted['snow_accum'] = parcels_input_daily_shifted['snow_accum'].copy()
            for parcel_idx, parcel_num in enumerate(parcel_nums):
                model_parcel = parcels_worker.sel(parcel=parcel_num)['snow_depth'].dropna(dim='time')
                model_initial_snow_depth = model_parcel.isel(time=0).values.item()
                input_initial_snow_depth = 100 * parcels_input_daily_shifted.sel(parcel_id=parcel_ids[parcel_idx])['snow_accum'].dropna(dim='time').isel(time=0).values.item()
                parcels_input_daily_shifted['snow_accum'].loc[dict(parcel_id=parcel_ids[parcel_idx])] = \
                    (100 * parcels_input_daily_shifted['snow_accum'].sel(parcel_id=parcel_ids[parcel_idx])) + \
                    (model_initial_snow_depth - input_initial_snow_depth)
            parcels_input_daily_shifted = parcels_input_daily_shifted.sel(parcel_id=parcel_ids)
            parcels_input_daily_shifted_cal = parcels_input_daily_shifted.sel(parcel_id=[buoy_id for buoy_id in buoy_names_training_set if buoy_id in parcel_ids])
            parcels_input_daily_shifted_val = parcels_input_daily_shifted.sel(parcel_id=[buoy_id for buoy_id in buoy_names_validation_set if buoy_id in parcel_ids])

        # evaluate model performance using difference metrics
        diff_cal = (parcels_worker['snow_depth'].swap_dims({'parcel':'parcel_id'}) - parcels_input_daily_shifted_cal['snow_accum'])
        diff_val = (parcels_worker['snow_depth'].swap_dims({'parcel':'parcel_id'}) - parcels_input_daily_shifted_val['snow_accum'])
        perf.loc[worker_num,'mean_error'] = diff_cal.mean().item()      # based on 'cal' buoys (not included in variable name to maintain legacy code)
        perf.loc[worker_num,'mean_error_val'] = diff_val.mean().item()  # based on 'val' buoys
        perf.loc[worker_num,'rmse'] = ((diff_cal**2).mean()**0.5).item()
        perf.loc[worker_num,'rmse_val'] = ((diff_val**2).mean()**0.5).item()

        # evaluate model performance using metrics based on rate of change (with 3-day centered rolling mean applied to buoy snow accumulation measurements to mitigate spikes)
        diff_diffs_cal = parcels_worker['snow_depth'].swap_dims({'parcel':'parcel_id'}).diff(dim='time') - parcels_input_daily_shifted_cal['snow_accum'].rolling(time=3,center=True).mean().diff(dim='time')
        diff_diffs_val = parcels_worker['snow_depth'].swap_dims({'parcel':'parcel_id'}).diff(dim='time') - parcels_input_daily_shifted_val['snow_accum'].rolling(time=3,center=True).mean().diff(dim='time')
        perf.loc[worker_num,'mean_error_diff'] = diff_diffs_cal.mean().item()
        perf.loc[worker_num,'mean_error_diff_val'] = diff_diffs_val.mean().item()
        perf.loc[worker_num,'rmse_diff'] = ((diff_diffs_cal**2).mean()**0.5).item()
        perf.loc[worker_num,'rmse_diff_val'] = ((diff_diffs_val**2).mean()**0.5).item()
    return perf


# iterate through each rung of tuning procedure until stopping criterion is reached
tuning_complete = False
while tuning_complete == False:
    
    # load existing parameter tracker file
    param_output_filename = 'snow_model_params_tuning.csv'
    params_tuning = pd.read_csv(processed_model_output_dir_params + param_output_filename)

    # set number of workers including 'default' (baseline) parameter sets
    n_workers = n_test_workers + 2    # 56 workers

    # assess the rung whose runs have completed
    if rung > 0:
        print('Now evaluating rung {0}...'.format(rung-1))
        perf = assess_perf(rung-1)

        # save initial (rung = 0) output to enable plotting of metrics for two 'default' baseline parameter set
        if rung == 1:
            perf.to_csv(processed_model_output_dir_params + 'rung0/snow_model_params_perf_rung0.csv')

        # exclude last two 'default' model simulations
        perf_for_halving = perf.iloc[:-2]   # exclude last two workers, which are 'default' model simulations

        # save performance metrics based on CALIBRATION set of buoys for: (1) all workers [mean ± sigma], (2) min RMSE worker, (3) baseline parameter set worker
        params_tuning.loc[rung-1,'bias_average'] = perf_for_halving['mean_error'].mean()
        params_tuning.loc[rung-1,'bias_sigma'] = perf_for_halving['mean_error'].std()
        params_tuning.loc[rung-1,'rmse_average'] = perf_for_halving['rmse'].mean()
        params_tuning.loc[rung-1,'rmse_sigma'] = perf_for_halving['rmse'].std()
        params_tuning.loc[rung-1,'bias_diff_average'] = perf_for_halving['mean_error_diff'].mean()
        params_tuning.loc[rung-1,'bias_diff_sigma'] = perf_for_halving['mean_error_diff'].std()
        params_tuning.loc[rung-1,'rmse_diff_average'] = perf_for_halving['rmse_diff'].mean()
        params_tuning.loc[rung-1,'rmse_diff_sigma'] = perf_for_halving['rmse_diff'].std()
        params_tuning.loc[rung-1,'rmse_min'] = perf_for_halving['rmse'].min()        # minimum RMSE value out of all workers
        if rung == 1: baseline_worker_idx = -1
        else:         baseline_worker_idx = -2
        params_tuning.loc[rung-1,'bias_baseline'] = perf.iloc[baseline_worker_idx]['mean_error']      # the next four are for the baseline parameter set
        params_tuning.loc[rung-1,'rmse_baseline'] = perf.iloc[baseline_worker_idx]['rmse']
        params_tuning.loc[rung-1,'bias_diff_baseline'] = perf.iloc[baseline_worker_idx]['mean_error_diff']
        params_tuning.loc[rung-1,'rmse_diff_baseline'] = perf.iloc[baseline_worker_idx]['rmse_diff']
        if rung >= 2:
            params_tuning.loc[rung-1,'bias_best_performers'] = perf.iloc[-1]['mean_error']  # the next four are for the parameter set generated from the previous "best performers"
            params_tuning.loc[rung-1,'rmse_best_performers'] = perf.iloc[-1]['rmse']
            params_tuning.loc[rung-1,'bias_diff_best_performers'] = perf.iloc[-1]['mean_error_diff']
            params_tuning.loc[rung-1,'rmse_diff_best_performers'] = perf.iloc[-1]['rmse_diff']
        
        # save performance metrics based on VALIDATION set of buoys for: (1) all workers [mean ± sigma], (2) min RMSE worker, (3) baseline parameter set worker
        params_tuning.loc[rung-1,'bias_average_val'] = perf_for_halving['mean_error_val'].mean()
        params_tuning.loc[rung-1,'bias_sigma_val'] = perf_for_halving['mean_error_val'].std()
        params_tuning.loc[rung-1,'rmse_average_val'] = perf_for_halving['rmse_val'].mean()
        params_tuning.loc[rung-1,'rmse_sigma_val'] = perf_for_halving['rmse_val'].std()
        params_tuning.loc[rung-1,'bias_diff_average_val'] = perf_for_halving['mean_error_diff_val'].mean()
        params_tuning.loc[rung-1,'bias_diff_sigma_val'] = perf_for_halving['mean_error_diff_val'].std()
        params_tuning.loc[rung-1,'rmse_diff_average_val'] = perf_for_halving['rmse_diff_val'].mean()
        params_tuning.loc[rung-1,'rmse_diff_sigma_val'] = perf_for_halving['rmse_diff_val'].std()
        params_tuning.loc[rung-1,'rmse_min_val'] = perf_for_halving['rmse_val'].min()        # minimum RMSE value out of all workers
        if rung == 1: baseline_worker_idx = -1
        else:         baseline_worker_idx = -2
        params_tuning.loc[rung-1,'bias_baseline_val'] = perf.iloc[baseline_worker_idx]['mean_error_val']      # the next four are for the baseline parameter set
        params_tuning.loc[rung-1,'rmse_baseline_val'] = perf.iloc[baseline_worker_idx]['rmse_val']
        params_tuning.loc[rung-1,'bias_diff_baseline_val'] = perf.iloc[baseline_worker_idx]['mean_error_diff_val']
        params_tuning.loc[rung-1,'rmse_diff_baseline_val'] = perf.iloc[baseline_worker_idx]['rmse_diff_val']
        if rung >= 2:
            params_tuning.loc[rung-1,'bias_best_performers_val'] = perf.iloc[-1]['mean_error_val']  # the next four are for the parameter set generated from the previous "best performers"
            params_tuning.loc[rung-1,'rmse_best_performers_val'] = perf.iloc[-1]['rmse_val']
            params_tuning.loc[rung-1,'bias_diff_best_performers_val'] = perf.iloc[-1]['mean_error_diff_val']
            params_tuning.loc[rung-1,'rmse_diff_best_performers_val'] = perf.iloc[-1]['rmse_diff_val']

        # determine parameter set for next rung
        perf_top_half = perf_for_halving.sort_values('rmse')[:int(len(perf_for_halving)/2)]
        perf_best_performers = perf_for_halving.sort_values('rmse')[:best_performers]
        params_tuning.loc[rung,'lockup_factor'] = perf_top_half['lockup_factor'].median()
        params_tuning.loc[rung,'lockup_factor_sigma'] = perf_top_half['lockup_factor'].std()
        params_tuning.loc[rung,'lockup_factor_best_performers'] = perf_best_performers['lockup_factor'].median()
        params_tuning.loc[rung,'msr_factor'] = perf_top_half['msr_factor'].median()
        params_tuning.loc[rung,'msr_factor_sigma'] = perf_top_half['msr_factor'].std()
        params_tuning.loc[rung,'msr_factor_best_performers'] = perf_best_performers['msr_factor'].median()
        params_tuning.loc[rung,'compaction_factor'] = perf_top_half['compaction_factor'].median()
        params_tuning.loc[rung,'compaction_factor_sigma'] = perf_top_half['compaction_factor'].std()
        params_tuning.loc[rung,'compaction_factor_best_performers'] = perf_best_performers['compaction_factor'].median()
        params_tuning.loc[rung,'rain_factor'] = perf_top_half['rain_factor'].median()
        params_tuning.loc[rung,'rain_factor_sigma'] = perf_top_half['rain_factor'].std()
        params_tuning.loc[rung,'rain_factor_best_performers'] = perf_best_performers['rain_factor'].median()
        params_tuning.loc[rung,'melt_factor'] = perf_top_half['melt_factor'].median()
        params_tuning.loc[rung,'melt_factor_sigma'] = perf_top_half['melt_factor'].std()
        params_tuning.loc[rung,'melt_factor_best_performers'] = perf_best_performers['melt_factor'].median()
        params_tuning.loc[rung,'melt_threshold_factor'] = perf_top_half['melt_threshold_factor'].median()
        params_tuning.loc[rung,'melt_threshold_factor_sigma'] = perf_top_half['melt_threshold_factor'].std()
        params_tuning.loc[rung,'melt_threshold_factor_best_performers'] = perf_best_performers['melt_threshold_factor'].median()
        params_tuning.loc[rung,'Q_sub_factor'] = perf_top_half['Q_sub_factor'].median()
        params_tuning.loc[rung,'Q_sub_factor_sigma'] = perf_top_half['Q_sub_factor'].std()
        params_tuning.loc[rung,'Q_sub_factor_best_performers'] = perf_best_performers['Q_sub_factor'].median()
        params_tuning.loc[rung,'Q_ocean_factor'] = perf_top_half['Q_ocean_factor'].median()
        params_tuning.loc[rung,'Q_ocean_factor_sigma'] = perf_top_half['Q_ocean_factor'].std()
        params_tuning.loc[rung,'Q_ocean_factor_best_performers'] = perf_best_performers['Q_ocean_factor'].median()
        params_tuning.loc[rung,'Q_surf_factor'] = perf_top_half['Q_surf_factor'].median()
        params_tuning.loc[rung,'Q_surf_factor_sigma'] = perf_top_half['Q_surf_factor'].std()
        params_tuning.loc[rung,'Q_surf_factor_best_performers'] = perf_best_performers['Q_surf_factor'].median()

        # clean up added column names
        for col_name in params_tuning.keys():
            if 'Unnamed' in col_name:
                del params_tuning[col_name]

        # update existing file with new parameter set and previous parameter evaluation
        if update_params_tuning:
            params_tuning.to_csv(processed_model_output_dir_params + param_output_filename)
            
    if rung > 1:
        # check if stopping criterion for tuning procedure has been reached
        # -> continue generating rungs as long as the ensemble average RMSE and/or the baseline parameter set RMSE
        #    is improving (decreasing) by at least 0.1 cm
        delta_rmse_average = params_tuning.loc[rung-1,'rmse_average'] - params_tuning.loc[rung-2,'rmse_average']
        delta_rmse_baseline = params_tuning.loc[rung-1,'rmse_baseline'] - params_tuning.loc[rung-2,'rmse_baseline']
        print('Improvement in ensemble average RMSE: {0:.2f} cm'.format(-1 * delta_rmse_average))
        print('Improvement in baseline parameter set RMSE: {0:.2f} cm'.format(-1 * delta_rmse_baseline))
        if delta_rmse_average <= -0.1 or delta_rmse_baseline <= -0.1:
            tuning_complete = False
            print('>>> one or both RMSE metrics are still improving by 0.1 cm; tuning procedure will continue')
        elif rung < 11:
            tuning_complete = False
            print('>>> neither RMSE metric is improving by 0.1 cm, but tuning procedure will continue until at least rung 11')
        else:
            tuning_complete = True   # neither RMSE metric is improving; stop tuning procedure
            print('>>> neither RMSE metric is improving by 0.1 cm; tuning procedure will now terminate')
            break
    
    # generate parameter sets, including for initial rung (rung = 0)
    # - NOTE: 'melt_threshold_factor' draws from a standard normal distribution, rather than a truncated normal distribution,
    #         because it is not a scaling parameter, but rather a temperature threshold with a prior distribution centered at 0.0
    #         for which negative values should be considered
    params = pd.DataFrame(index=arange(n_workers),columns=['use_lockup','lockup_factor','msr_factor','compaction_factor',
                                                           'rain_factor','melt_factor','melt_threshold_factor',
                                                           'Q_sub_factor','Q_ocean_factor','Q_surf_factor'])
    params['use_lockup'] = False
    params['lockup_factor'] = truncnorm(params_tuning.loc[rung,'lockup_factor'],params_tuning.loc[rung,'lockup_factor_sigma'],n_workers)
    params['msr_factor'] = truncnorm(params_tuning.loc[rung,'msr_factor'],params_tuning.loc[rung,'msr_factor_sigma'],n_workers)
    params['compaction_factor'] = truncnorm(params_tuning.loc[rung,'compaction_factor'],params_tuning.loc[rung,'compaction_factor_sigma'],n_workers)
    params['rain_factor'] = truncnorm(params_tuning.loc[rung,'rain_factor'],params_tuning.loc[rung,'rain_factor_sigma'],n_workers)
    params['melt_factor'] = truncnorm(params_tuning.loc[rung,'melt_factor'],params_tuning.loc[rung,'melt_factor_sigma'],n_workers)
    params['melt_threshold_factor'] = standard_norm(params_tuning.loc[rung,'melt_threshold_factor'],
                                                    params_tuning.loc[rung,'melt_threshold_factor_sigma'],n_workers)
    params['Q_sub_factor'] = truncnorm(params_tuning.loc[rung,'Q_sub_factor'],params_tuning.loc[rung,'Q_sub_factor_sigma'],n_workers)
    params['Q_ocean_factor'] = truncnorm(params_tuning.loc[rung,'Q_ocean_factor'],params_tuning.loc[rung,'Q_ocean_factor_sigma'],n_workers)
    params['Q_surf_factor'] = truncnorm(params_tuning.loc[rung,'Q_surf_factor'],params_tuning.loc[rung,'Q_surf_factor_sigma'],n_workers)
    
    # for initial rung (rung = 0), create two workers at end with special 'default' parameter sets
    # and for subsequent rungs, create two workers at end with a 'default' (baseline) parameter set and a parameter set generated from previous best performing sets
    # - note: if changing this in the future, also need to change code above to appropriately exclude these 'default' sets from evaluation
    if rung == 0:
        params.loc[54,'use_lockup'] = False  # worker 54: snow accumulation only, zero loss processes and no compaction
        params.loc[54,'lockup_factor'] = 0.0
        params.loc[54,'msr_factor'] = 1.0
        params.loc[54,'compaction_factor'] = -1.0        # note: setting this to -1.0 turns the compaction parameterization off
        params.loc[54,'rain_factor'] = 0.0
        params.loc[54,'melt_factor'] = 0.0
        params.loc[54,'melt_threshold_factor'] = 0.0     # not relevant when <melt_factor> set to 0.0
        params.loc[54,'Q_sub_factor'] = 0.0
        params.loc[54,'Q_ocean_factor'] = 0.0
        params.loc[54,'Q_surf_factor'] = 0.0
        params.loc[55,'use_lockup'] = False  # worker 55: snow accumulation and loss with prior scalings (original model parameterizations)
        params.loc[55,'lockup_factor'] = 0.0
        params.loc[55,'msr_factor'] = 1.0
        params.loc[55,'compaction_factor'] = 1.0
        params.loc[55,'rain_factor'] = 1.0
        params.loc[55,'melt_factor'] = 1.5
        params.loc[55,'melt_threshold_factor'] = 0.0
        params.loc[55,'Q_sub_factor'] = 1.0
        params.loc[55,'Q_ocean_factor'] = 1.0
        params.loc[55,'Q_surf_factor'] = 1.0
    else:
        params.loc[54,'use_lockup'] = False     # worker 54: baseline values
        params.loc[54,'lockup_factor'] = params_tuning.loc[rung,'lockup_factor']
        params.loc[54,'msr_factor'] = params_tuning.loc[rung,'msr_factor']
        params.loc[54,'compaction_factor'] = params_tuning.loc[rung,'compaction_factor']
        params.loc[54,'rain_factor'] = params_tuning.loc[rung,'rain_factor']
        params.loc[54,'melt_factor'] = params_tuning.loc[rung,'melt_factor']
        params.loc[54,'melt_threshold_factor'] = params_tuning.loc[rung,'melt_threshold_factor']
        params.loc[54,'Q_sub_factor'] = params_tuning.loc[rung,'Q_sub_factor']
        params.loc[54,'Q_ocean_factor'] = params_tuning.loc[rung,'Q_ocean_factor']
        params.loc[54,'Q_surf_factor'] = params_tuning.loc[rung,'Q_surf_factor']
        params.loc[55,'use_lockup'] = False     # worker 55: values generated from previous best performing parameter sets
        params.loc[55,'lockup_factor'] = params_tuning.loc[rung,'lockup_factor_best_performers']
        params.loc[55,'msr_factor'] = params_tuning.loc[rung,'msr_factor_best_performers']
        params.loc[55,'compaction_factor'] = params_tuning.loc[rung,'compaction_factor_best_performers']
        params.loc[55,'rain_factor'] = params_tuning.loc[rung,'rain_factor_best_performers']
        params.loc[55,'melt_factor'] = params_tuning.loc[rung,'melt_factor_best_performers']
        params.loc[55,'melt_threshold_factor'] = params_tuning.loc[rung,'melt_threshold_factor_best_performers']
        params.loc[55,'Q_sub_factor'] = params_tuning.loc[rung,'Q_sub_factor_best_performers']
        params.loc[55,'Q_ocean_factor'] = params_tuning.loc[rung,'Q_ocean_factor_best_performers']
        params.loc[55,'Q_surf_factor'] = params_tuning.loc[rung,'Q_surf_factor_best_performers']        

    # export initial parameter set to CSV file
    if 'rung{0}'.format(rung) not in os.listdir(processed_model_output_dir_params):
        os.mkdir(processed_model_output_dir_params + 'rung{0}'.format(rung))
    output_filename = 'snow_model_params_rung{0}.csv'.format(rung)
    if (output_filename not in os.listdir(processed_model_output_dir_params + 'rung{0}/'.format(rung))) or (overwrite_params is True):
        params.to_csv(processed_model_output_dir_params + 'rung{0}/'.format(rung) + output_filename)

    # launch batch of model runs with initial parameter set
    # - note: if necessary, kill jobs from command line using: killall python3
    env_saved = os.environ
    env_saved['PYTHONPATH'] = env_addition
    for worker_num in range(n_workers):
        # note: the "2>&1" part directs both stderr and stdout to the .out file, preventing it from being captured by the parent-level subprocess call
        subprocess.run('nohup python3 -u "{3}snow_lagrangian_worker.py" worker_tuning {0} {1} > "{2}rung{1}/nohup_rung{1}_worker{0}.out" 2>&1 &'\
                       .format(worker_num,rung,processed_model_output_dir_params,this_code_dir),shell=True,env=env_saved)
        print('Rung {0} - worker {1} launched'.format(rung,worker_num))
        time.sleep(2)
        
    # wait for workers to complete their jobs
    rung_complete = False
    while rung_complete == False:
        time.sleep(30)
        current_files = os.listdir(processed_model_output_dir_params + 'rung{0}/'.format(rung))
        rung_complete = all([any(['snow_model_output_rung{0}_worker{1}'.format(rung,worker_num) in fn for fn in current_files]) for worker_num in arange(0,n_workers)])
        print(str(datetime.now()) + ': Are rung {0} workers all finished? {1}'.format(rung,rung_complete))
    
    # all workers are complete; prepare for next tuning rung
    rung += 1