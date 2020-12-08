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

## Usage

### Help

    $ nc2zarr --help
    Usage: nc2zarr [OPTIONS] [INPUT_FILE ...]
    
      Converts multiple NetCDF files to a single Zarr dataset.
    
      INPUT_FILE may refer to a NetCDF file or a glob that identifies multiple
      them, e.g. "L3_SST/**/*.nc".
    
      If CONFIG_FILE is given, any given NetCDF file arguments overwrite setting
      input/path in CONFIG_FILE . Accordingly the following option overwrite
      settings in CONFIG_FILE:
    
      --mode overwrites mode
      --batch overwrites input/batch_size (not implemented yet)
      --decode-cf overwrites input/decode_cf
      --output overwrites output/path
    
    Options:
      -o, --output OUTPUT_FILE  Output name. Defaults to "out.zarr".
      -c, --config CONFIG_FILE  Configuration file. Defaults to "nc2zarr-
                                config.yml".
    
      -b, --batch BATCH_SIZE    Batch size. If greater zero, conversion will be
                                performed in batches of the given size.
    
      -m, --mode MODE           Configuration file. Must be one of ('slices',
                                'one_go'). Defaults to "slices".
    
      -d, --decode-cf           Decode variables according to CF conventions.
      -v, --verbose             Print more output.
      --version                 Show version number and exit.
      --help                    Show this message and exit.

### Config file format

TODO