#!/usr/bin/env python3

"""Rechunk a Zarr with chunks of size 1 in time, full size in lat/lon.

If s3fs is installed, "s3://..." arguments can be used and credentials
will be read from standard environment variables or files (see s3fs docs).

The output dataset will have the same data as the input dataset, rechunked
so that the chunks are flat time slices. That is, the chunks will have
size 1 in the time dimension.

Spatially (i.e. in lat and lon dimensions), either the current chunk
dimensions can be kept, or the chunk spatial dimensions can be set to cover
the entire extent of the dataset.
"""

import argparse
import xarray as xr
import zarr

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--keep-spatial', '-k',
                        action='store_true',
                        help='Keep current spatial chunk size')
    parser.add_argument('input_zarr')
    parser.add_argument('output_zarr')
    args = parser.parse_args()
    rechunk(args.input_zarr, args.output_zarr, args.keep_spatial)


def rechunk(input_path: str, output_path: str, keep_spatial: bool):
    ds = xr.open_dataset(input_path, engine="zarr")
    for var in ds:
        del ds[var].encoding['chunks']
    chunk_params = {'time': 1}
    if not keep_spatial:
        chunk_params['lat'] = len(ds.lat)
        chunk_params['lon'] = len(ds.lon)
    ds_rechunked = ds.chunk(chunks=chunk_params)
    print('Writing output Zarr...')
    ds_rechunked.to_zarr(output_path)


if __name__ == '__main__':
    main()
