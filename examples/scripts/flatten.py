#!/usr/bin/env python3

"""Rechunk a Zarr with chunks of size 1 in time, full size in lat/lon.

If s3fs is installed, "s3://..." arguments can be used and credentials
will be read from standard environment variables or files (see s3fs docs).

The output dataset will have the same data as the input dataset, rechunked
so that the chunks are flat time slices. That is, the chunks will have
size 1 in the time dimension and cover the full extent of the dataset in
the lat and lon dimensions.
"""

import xarray as xr
import argparse

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('input_zarr')
    parser.add_argument('output_zarr')
    args = parser.parse_args()
    rechunk(args.input_zarr, args.output_zarr)


def rechunk(input_path, output_path):
    ds = xr.open_dataset(input_path, engine="zarr")
    for var in ds:
        del ds[var].encoding['chunks']
    full_lat = len(ds.lat)
    full_lon = len(ds.lon)
    ds_rechunked = ds.chunk({'time': 1, 'lat': full_lat, 'lon': full_lon})
    print('Writing output Zarr...')
    ds_rechunked.to_zarr(output_path)


if __name__ == '__main__':
    main()
