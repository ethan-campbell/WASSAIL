# University of Washington Snow on Antarctic Ice Lagrangian (WASSAIL) model

**This repository contains the model and analysis Python code for Campbell et al. (preprint), "Lagrangian reconstruction of snow accumulation and loss on Antarctic sea ice", submitted to _The Cryosphere_, doi:TBD.**

Please contact me at [ethancc@uw.edu](mailto:ethancc@uw.edu) if you have any questions regarding this code.

### Attribution:
This code is freely available for reuse as described in the MIT License included in this repository. If using this code and/or model data in an academic publication, we encourage you to provide the following citations, as appropriate:
* **Preprint**: Campbell, E.C., Riser, S.C., Webster, M.A. (2026). Lagrangian reconstruction of snow accumulation and loss on Antarctic sea ice. _EGUsphere_ [preprint]. doi:TBD
* **Zenodo code archive**: Campbell, E.C. (2026, April 10). WASSAIL model and analysis code, v1.0. Zenodo. doi:TBD
* **Zenodo model data archive**: Campbell, E.C., Riser, S.C., Webster, M.A. (2026, April 10). University of Washington Snow on Antarctic Ice Lagrangian (WASSAIL) model, v1.0.0 (2003-2025). Zenodo. doi:TBD

### Description:

This repository contains code to run the University of Washington Snow on Antarctic Ice Lagrangian (WASSAIL) model and generate the figures presented in the associated study. The data used to run the model are all publicly available (see the "Code and data availability" statement in the paper). Model output fields are archived separately on Zenodo (see above) and reuse is welcomed.

### Prerequisites:

1. Python 3 and `conda` (or `mamba`) installed. The [Anaconda](https://www.anaconda.com/download) distribution is recommended.

2. A Linux server with at least several cores, for efficient parallelization of multiple one-year model runs. Single-year model runs can be run sequentially within the provided Jupyter notebook, probably on any machine, but the parallelization functionality has not been tested outside of a Linux environment. RAM may become a limiting factor if running the model on a laptop.

### Step-by-step instructions for using this repository and running the model:

1. Clone or download this GitHub repository. Unzip the `wassail.zip` file (e.g., using the command `unzip wassail.zip`), which contains a mostly-empty directory structure.

> Note: In the interest of future reproducibility, the AWI snow buoy calibration/validation data set used for the paper (last accessed 1 May 2025) is archived within this `.zip` file, as the data are continuously updated at [their source](https://data.meereisportal.de/relaunch/buoy.php) and a static archive does not exist elsewhere. The directories also contain grid files for certain products.

2. Recreate the required Python environment with all dependencies using the provided `wassail.yml` environment file. From within the repository, execute `conda env create -f wassail.yml` (you can also substitute `conda` with `mamba`, if preferred). This will create a new environment called `wassail`. Next, activate the environment using `conda activate wassail`.

3. The directory `Toolbox/` contains the command-line tool `h4toh5`, which converts HDF4 files to HDF5 format. A version for Linux, `h4toh5_linux_centos7`, is provided. If you need a different version for your computing environment, download it [here](https://www.hdfeos.org/software/h4toh5-def-download.php) and update the `exec_filename` variable within the `convert_to_hdf5()` function in the `download_file.py` script.

4. Open and follow the main code notebook `wassail.ipynb`. I strongly recommend using JupyterLab and acquainting yourself with the notebook structure using the left-side heading navigation pane. (To learn more about how to work with Jupyter notebooks, see [jupyter.org](https://jupyter.org).)

> Note: This single notebook contains almost all of the model and analysis code. The `Toolbox/` directory contains a few `.py` Python scripts with auxiliary "helper" functions, mostly for downloading and loading data. The notebook is documented throughout and is intended to be run from top to bottom, in order to download and process input data, configure the model, run the model (both in "calibration mode" and as "free-running simulations"—see the paper for details), process the output, and visualize/analyze results. Housing the entire model within a Jupyter notebook did create challenges for parallelization, which led to some interesting ad hoc solutions that are described below.

5. Start by running the "Import statements" notebook cell. Confirm that the `conda` environment is functioning correctly.

6. Run the "Set file paths and import custom functions" cell after updating directories, as needed:
- Under the appropriate system, the variable `data_dir` should point to the `Data/` directory within this repository.
- The base path for `script_dir` and `this_code_dir` should be updated to reflect the location of this repo on your system.
- `current_results_dir` should be updated to `Results/` within this repo, or a different location to store output visuals.
- The sub-directory paths can be updated if you wish to use existing input data files on your system. However, this may require changes elsewhere, because this code processes and re-exports some input data files and thus expects them in a different format than how they were originally downloaded.
- The sub-directory paths for serialized/processed data must be updated to those in the `Data/Processed/` directory in this repo. Dates may be added, as shown in the notebook, for versioning purposes.

7. 
8. 
