# nc2zarr

A Python tool that converts multiple NetCDF files to single Zarr datasets.

### Create Python environment

    $ conda install -n base -c conda-forge mamba
    $ cd nc2zarr
    $ mamba env create
    
### Install nc2zarr from Sources 

    $ cd nc2zarr
    $ conda activate nc2zarr
    $ python setup.py develop

### Usage

    $ nc2zarr --help
    Usage: nc2zarr [OPTIONS] [INPUT_FILE ...]
    
      Converts multiple NetCDF files to a single Zarr dataset.
    
      INPUT_FILE may refer to a NetCDF file or a glob that identifies multiple
      them, e.g. "L3_SST/**/*.nc".
    
      If CONFIG_FILE is given, any given NetCDF file arguments overwrite setting
      input/path in CONFIG_FILE . Accordingly the following option overwrite
      settings in CONFIG_FILE:
    
      --mode overwrites CONFIG_FILE/mode
      --dry-run overwrites CONFIG_FILE/dry_run
      --batch overwrites CONFIG_FILE/input/batch_size (not implemented yet)
      --decode-cf overwrites CONFIG_FILE/input/decode_cf
      --output overwrites CONFIG_FILE/output/path
    
    Options:
      -o, --output OUTPUT_FILE  Output name. Defaults to "out.zarr".
      -c, --config CONFIG_FILE  Configuration file. Defaults to "nc2zarr-
                                config.yml".
    
      -b, --batch BATCH_SIZE    Batch size. If greater zero, conversion will be
                                performed in batches of the given size.
    
      -m, --mode MODE           Configuration file. Must be one of ('slices',
                                'one_go'). Defaults to "slices".
    
      --decode-cf               Decode variables according to CF conventions.
                                Caution: array data may be converted to floating
                                point type if a "_FillValue" attribute is present.
    
      -d, --dry-run             Open and process inputs only, omit data writing.
      -v, --verbose             Print more output.
      --version                 Show version number and exit.
      --help                    Show this message and exit.


### Config file format

```yaml

mode: "slices" # "slices" | "one_go"
dry_run: false

input:
  paths: 
    - <input_path_or_glob_1>
    - <input_path_or_glob_2>

  variables:
    - <var_name_1>
    - <var_name_2>
  
  # xarray engine 
  engine: "netcdf4"

  # Dimension to be used for appending 
  append_dim: "time"
  
  decode_cf: false


process:
  # Rename variables
  rename:
    <old_var_name_1>: <new_var_name_1>
    <old_var_name_2>: <new_var_name_2>

  # (Re)chunk dimensions
  rechunk:
    <dim_1>: <chunk_size_1>
    <dim_2>: <chunk_size_2>

output:
  path: <output_path>
  # Overwrite existing dataset?
  overwrite: false
  # consolidated arg passed to xarray.Dataset.to_zarr()
  consolidated: false
  # encoding arg passed to xarray.Dataset.to_zarr()
  encoding: null

  # If one of the following is given, 
  # all of them will be passed as args to s3fs.S3FileSystem()
  anon: null
  key: null
  secret: null
  # If one of the following is given, 
  # all of them will be passed as client_kwargs arg to s3fs.S3FileSystem()
  endpoint_url: null
  region_name: null

```