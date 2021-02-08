## nc2zarr Change History

### Version 0.2.0 (in development)

* Fixed some issues with Zarr output chunk sizes given by process 
  parameter `rechunk`: (#12)
  - Fix: New chunking can now overlap multiple chunks in input (Dask chunks).
  - Introduced new chunk size value `"input"` which forces using chunk sizes 
    from input.
* Introduced new input parameter `prefetch_chunks`. (#10) 
* Add an AppVeyor CI configuration file.
* Add the `--sort-by` command-line option.

### Version 0.1.0 (08.01.2021)

Initial version. 
