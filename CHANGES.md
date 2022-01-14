## nc2zarr Change History

### Version 1.2.4 (in development)

* Fixed ignored `output/append_dim` setting. (#54)

### Version 1.2.3

* Handle consolidated Zarrs correctly when appending data (fixes #47).

* Add configurations for SST_MED_SST_L4_NRT_OBSERVATIONS_010_004

* Update LWQ300 configuration

* Improve validation script

### Version 1.2.2

* Fix incorrect version number.

### Version 1.2.1

* Fix an occasionally failing test and update an example configuration.

### Version 1.2.0

* Introduce the new parameter `output.append_mode` to select behaviour
  when appending new data which overlaps in the append dimension with the
  existing data.

* Adjusted the built-in pre-processing to handle more cases: 
  Bounds dimensions of concatenation coordinates are extended by their
  dimension and transformed to variables if necessary.

* Whether to adjust output metadata (global attributes) after the last 
  write/append can now be forced by the new `output.adjust_metadata` 
  setting whose default is `false`. If set to `true`, this will adjust 
  the following metadata attributes:
  - "history"
  - "source"
  - "time_coverage_start"
  - "time_coverage_end" 
    
  In addition, extra metadata (global attributes) can now be added 
  after the last write/append using the new setting
  `output.metadata` whose value is a mapping from attribute 
  names to values. 
  
  The above functionality is also reflected in two new CLI options
  `--adjust-metadata` and `--finalize-only`. In combination, they
  can be used to later adjust metadata in already existing Zarr 
  datasets. (#20, #34)
  
* Fixed a problem that avoided appending datasets that contained variables
  with `dtype = "|S1"` (ESA SST CCI). Such variables are written initially, 
  but then no longer appended at all given that they do not contain the 
  dimension along we append. Otherwise `nc2zarr` will still fail. (#38) 

* Fixed a severe problem where data was "encoded" twice before 
  being appended to existing data. The bug became apparent
  with `input.decode_cf = false` (the default) and for variables
  that defined a `scale_factor` and/or `add_offset` encoding
  attribute. (#35)
  
* Added a CLI tool `nc2zarr-batch`. 

* Add and update some sample configurations and scripts.

### Version 1.1.1

* Fix an incompatibility with version 8 of the click library which
  was causing command-line parsing errors.

### Version 1.1.0

* Ensure attributes are maintained when missing dimensions are added to 
  concatenation dimension variable. (#32) 

* Added some basic batch utilities that help spawning multiple concurrent
  nc2zarr jobs, see new module `nc2zarr.batch`. (#19)    
  
* Local input and output paths may now include tilde '~' which will expand 
  to the current user's home, and '~username' for the home directory
  of a specified user. (#26)

* Fixed a problem when `input/sort_by` was `"name"` and one of `input/paths` 
  ended with "/". In these cases sorting did not work. (#29)

* Fixed problem where appending unnecessarily required a coordinate 
  variable (#27)

* Input path that were no wildcards have been ignored if the input path did 
  not exist. Now an error is raised in such cases. (#25)
  
* Fixed exception `TypeError: 'int' object is not iterable`
  raised with some `process/rechunk` configurations. (#23)

* You can now provide your Python code for customization 
  of the datasets read and written. (#16)
  - `input/custom_preprocessor: "module:function"` is called on 
    each input after optional variable selection.
  - `process/custom_processor: "module:function"` is called after 
    optional renaming and before optional rechunking.
  - `output/custom_postprocessor: "module:function"` is called before 
    the final dataset is written. (#21)

  All three functions are expected to receive an `xarray.Dataset` object
  as only argument and return the same or modified `xarray.Dataset` object.
  Note: to let Python import `"module"` that is not a user package,
  you can extend the `PYTHONPATH` environment variable before
  calling `nc2zarr`:
    ```
    export PYTHONPATH=${PYTHONPATH}:path/to/my/modules
    nc2zarr ...
    ``` 

* Add more example configurations and scripts.

### Version 1.0.0 (26.02.2021)

* Fixed some issues with Zarr (re-)chunking given by process parameter
  `process/rechunk`: 
  - Fix: New chunking can now overlap multiple chunks in input 
    (Dask chunks). (#12)
  - Introduced new chunk size value `"input"` which forces using chunk sizes 
    from input. (#12)
  - Fixed problems that occurred when using the `process/rechunk` 
    configuration
    + if a dimension was not configured at all;
    + if the dataset contained 0-dimension variables, which is 
      typical for variables used to describe the spatial reference system, 
      e.g. `crs`.
      
    In these cases the error message from Zarr was
    `AssertionError: We should never get here."`. (#14)

* Introduced new input parameter `prefetch_chunks`. (#10) 

* Add an AppVeyor CI configuration file.

* Add the `--sort-by` command-line option.

* Add example configuration files for the following datasets:
  - `C_GLS_LWQ300_GLOBE_OLCI_V1`
  - `OCEANCOLOUR_BAL_CHL_L3_NRT_OBSERVATIONS_009_049`
  - `OCEANCOLOUR_BS_CHL_L4_NRT_OBSERVATIONS_009_045`
  - `OCEANCOLOUR_MED_CHL_L4_NRT_OBSERVATIONS_009_041`

* Add demo Jupyter notebooks which open and plot converted data from object
  storage.

### Version 0.1.0 (08.01.2021)

Initial version. 
