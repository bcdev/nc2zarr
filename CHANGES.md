## nc2zarr Change History

### Version 1.1.0 (in development)

* You can now provide your Python code for customization 
  of the datasets read and written. (#16)
  - `input/custom_preprocessor: "module:function"` is called on 
    each input after optional variable selection.
  - `process/custom_processor: "module:function"` is called after 
    optional renaming and before optional rechunking.

  Both `"function"`s are expected to receive an `xarray.Dataset` object
  as only argument and return the same or modified `xarray.Dataset` object.
  Note: to let Python import `"module"` that is not a user package,
  you can extend the `PYTHONPATH` environment variable before
  calling `nc2zarr`:
    ```
    export PYTHONPATH=${PYTHONPATH}:path/to/my/modules
    nc2zarr ...
    ``` 


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
