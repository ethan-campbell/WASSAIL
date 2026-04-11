import os
import subprocess
import sys
import time

# parameters
this_code_dir = sys.argv[1]
first_start_year = int(sys.argv[2])
last_start_year = int(sys.argv[3])
processed_model_output_dir = sys.argv[4]
env_addition = sys.argv[5]

# launch batch of model runs with initial parameter set
# - note: if necessary, kill jobs from command line using: killall python3
env_saved = os.environ
env_saved['PYTHONPATH'] = env_addition
for start_year in range(first_start_year,last_start_year+1):
    # note: the "2>&1" part directs both stderr and stdout to the .out file
    subprocess.call('nohup python3 -u "{0}snow_lagrangian_free.py" worker_free {1} > "{2}{1}/nohup_free_{1}.out" 2>&1 &'\
                    .format(this_code_dir,start_year,processed_model_output_dir),shell=True,env=env_saved)
    print('Worker {0} launched'.format(start_year))
    time.sleep(2)