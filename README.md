[![Build Status](https://ci.appveyor.com/api/projects/status/n8c8lhbvcbvg3mht/branch/main?svg=true)](https://ci.appveyor.com/project/bcdev/nc2zarr)

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

### Testing and Test Coverage

    $ pytest --cov nc2zarr --cov-report=html tests   

### Usage

```
$ nc2zarr --help
Usage: nc2zarr [OPTIONS] [INPUT_FILE ...]

  Reads one or more input datasets and writes or appends them to a single
  Zarr output dataset.

  INPUT_FILE may refer to a NetCDF file, or Zarr dataset, or a glob that
  identifies multiple paths, e.g. "L3_SST/**/*.nc".

  OUTPUT_PATH must be directory which will contain the output Zarr dataset,
  e.g. "L3_SST.zarr".

  CONFIG_FILE must be in YAML format. It comprises the optional objects
  "input", "process", and "output". See nc2zarr/res/config-template.yml for
  a template file that describes the format. Multiple --config options may
  be passed as a chain to allow for reuse of credentials and other common
  parameters. Contained configuration objects are recursively merged, lists
  are appended, and other values overwrite each other from left to right.
  For example:

  nc2zarr -c s3.yml -c common.yml -c inputs-01.yml -o out-01.zarr
  nc2zarr -c s3.yml -c common.yml -c inputs-02.yml -o out-02.zarr
  nc2zarr out-01.zarr out-02.zarr -o final.zarr

  Command line arguments and options have precedence over other
  configurations and thus overwrite settings in any CONFIG_FILE:

  [--dry-run] overwrites /dry_run
  [--verbose] overwrites /verbosity
  [INPUT_FILE ...] overwrites /input/paths in CONFIG_FILE
  [--multi-file] overwrites /input/multi_file
  [--concat-dim] overwrites /input/concat_dim
  [--decode-cf] overwrites /input/decode_cf
  [--input-sort-by] overwrites /input/sort_by
  [--output OUTPUT_FILE] overwrites /output/path
  [--overwrite] overwrites /output/overwrite
  [--append] overwrites /output/append

Options:
  -c, --config CONFIG_FILE        Configuration file (YAML). Multiple may be
                                  given.

  -o, --output OUTPUT_PATH        Output name. Defaults to "out.zarr".
  -d, --concat-dim DIM_NAME       Dimension for concatenation. Defaults to
                                  "time".

  -m, --multi-file                Open multiple input files as one block.
                                  Works for NetCDF files only. Use --concat-
                                  dim to specify the dimension for
                                  concatenation.

  -w, --overwrite                 Overwrite existing OUTPUT_PATH. If
                                  OUTPUT_PATH does not exist, the option has
                                  no effect. Cannot be used with --append.

  -a, --append                    Append inputs to existing OUTPUT_PATH. If
                                  OUTPUT_PATH does not exist, the option has
                                  no effect. Cannot be used with --overwrite.

  --decode-cf                     Decode variables according to CF
                                  conventions. Caution: array data may be
                                  converted to floating point type if a
                                  "_FillValue" attribute is present.

  -s, --input-sort-by [path|name]
                                  Sort input files by specified property.
  -d, --dry-run                   Open and process inputs only, omit data
                                  writing.

  -v, --verbose                   Print more output. Use twice for even more
                                  output.

  --version                       Show version number and exit.
  --help                          Show this message and exit.
```

### Configuration file format

The format of the configuration files passed via the `--config` option is described
as a [configuration template](https://github.com/bcdev/nc2zarr/blob/main/nc2zarr/res/config-template.yml).

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
