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

```
$ nc2zarr --help
Usage: nc2zarr [OPTIONS] [INPUT_FILE ...]

  Reads one or input datasets and writes or appends them to a single Zarr
  output dataset.

  INPUT_FILE may refer to a NetCDF file, or Zarr dataset, or a glob that
  identifies multiple of them, e.g. "L3_SST/**/*.nc".

  OUTPUT_PATH must be directory which will contain the output Zarr dataset,
  e.g. "L3_SST.zarr".

  CONFIG_FILE has YAML format. If multiple are given, their "input",
  "process", and "output" entries are merged while other settings overwrite
  each other in the order they appear. Command line arguments overwrite
  settings in any CONFIG_FILE:

  [--dry-run] overwrites /dry_run
  [INPUT_FILE ...] overwrites /input/paths in CONFIG_FILE
  [--multi-file] overwrites /input/multi_file
  [--concat-dim] overwrites /input/concat_dim
  [--decode-cf] overwrites /input/decode_cf
  [--output OUTPUT_FILE] overwrites /output/path
  [--overwrite] overwrites /output/overwrite
  [--append] overwrites /output/append

Options:
  -c, --config CONFIG_FILE  Configuration file (YAML). Multiple may be given.
  -o, --output OUTPUT_PATH  Output name. Defaults to "out.zarr".
  -m, --multi-file          Open multiple input files as one block. Works for
                            NetCDF files only. Use --concat-dim to specify the
                            dimension for concatenation.

  -d, --concat-dim DIM      Dimension for concatenation. Defaults to "time".
  -x, --overwrite           Overwrite existing existing OUTPUT_PATH. If
                            OUTPUT_PATH does not exist, the option has no
                            effect. Cannot be used with --append.

  -a, --append              Append inputs to existing OUTPUT_PATH. If
                            OUTPUT_PATH does not exist, the option has no
                            effect. Cannot be used with --overwrite.

  --decode-cf               Decode variables according to CF conventions.
                            Caution: array data may be converted to floating
                            point type if a "_FillValue" attribute is present.

  -d, --dry-run             Open and process inputs only, omit data writing.
  -v, --verbose             Print more output.
  --version                 Show version number and exit.
  --help                    Show this message and exit.
```

### Examples

Convert multiple NetCDFs to single Zarr:

```bash
$ nc2zarr -o outputs/SST.zarr inputs/**/SST-*.nc
```

Append single NetCDF to an existing Zarr:

```bash
$ nc2zarr -a -o outputs/SST.zarr inputs/2020/SST-20200610.nc
```

Concatenate multiple Zarrs to a new Zarr:

```bash
$ nc2zarr -o outputs/SST.zarr outputs/SST-part1.zarr outputs/SST-part2.zarr
```

Append one Zarr to existing Zarr:

```bash
$ nc2zarr -a -o outputs/SST.zarr outputs/SST-part3.zarr
```

### Config file format

```yaml

dry_run: false
verbosity: 0

input:
  paths:
    - <input_path_or_glob_1>
    - <input_path_or_glob_2>
  # Filter variables. Comment out or set to null for all variables.
  variables:
    - <var_name_1>
    - <var_name_2>
  # Use xarray.open_mfdataset() passing all expanded paths
  multi_file: false
  # Dimension to be used for concatenation if multi_file: true
  concat_dim: "time"
  # xarray engine 
  engine: "netcdf4"
  decode_cf: false
  sort_by: false # true, "path" (= true), or "name"  

process:
  # Rename variables
  rename:
    <var_name>: <new_var_name>
    <var_name>: <new_var_name>

  # (Re)chunk variable dimensions
  rechunk:
    # Set selected dimensions of all variables to chunk_size.  
    "*":
      <dim_name>: <chunk_size>
      <dim_name>: <chunk_size>
    # Set selected dimensions of individual variables to chunk_size.  
    <var_name>:
      <dim_name>: <chunk_size>
      <dim_name>: <chunk_size>
    # Set dimension dim_name=var_name of individual variable to chunk_size.  
    <var_name>: <chunk_size>
    # Don't chunk individual variable at all.  
    <var_name>: null

output:
  # if s3 is given this is a relative path "<bucket_name>/path/to/my.zarr", 
  # otherwise it may be any local FS directory path. 
  path: <output_path>
  # consolidated arg passed to xarray.Dataset.to_zarr()
  consolidated: false
  # encoding arg passed to xarray.Dataset.to_zarr()
  encoding: null
  # Overwrite existing dataset?
  overwrite: false
  # Append to existing dataset?
  append: false
  # Append dimension. Defaults to input/concat_dim or "time"
  append_dim: false
  # If given, will be passed to as s3fs.S3FileSystem(**s3)
  s3:
    anon: null
    key: null
    secret: null
    client_kwargs:
      endpoint_url: null
      region_name: null
```