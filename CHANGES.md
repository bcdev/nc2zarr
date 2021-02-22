## nc2zarr Change History

### Version 0.2.0 (in development)

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

### Version 0.1.0 (08.01.2021)

Initial version. 
