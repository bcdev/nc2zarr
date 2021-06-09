#!/usr/bin/env python

"""set_date_attrs.py: Update a Zarr's start_date and stop_date attributes

Given a Zarr store with a time dimension, this script sets its start_date
and stop_date attributes (using the format YYYY-MM-DD) to match the first
and last time values in the data.
"""

import argparse
import pandas as pd
import xarray as xr
import zarr


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("zarr", metavar="PATH_OR_URL",
                        description="Update a Zarr's start_date and"
                        "stop_date attributes to match its data.")
    parser.add_argument("--dry-run", "-d", action="store_true",
                        help="Don't actually write metadata")
    parser.add_argument("--verbose", "-v", action="store_true",
                        help="Report progress to standard output")
    args = parser.parse_args()
    ds = xr.open_zarr(args.zarr)
    z = zarr.open(args.zarr)
    t0 = ds.time[0].values
    t1 = ds.time[-1].values
    if args.verbose:
        print("First/last times:", t0, t1)
    new_attrs = dict(
        start_date=pd.to_datetime(t0).strftime("%Y-%m-%d"),
        stop_date=pd.to_datetime(t1).strftime("%Y-%m-%d")
    )
    if args.verbose:
        for title, dic in ("Old", z.attrs), ("New", new_attrs):
            print(f"{title} attributes:")
            for key in "start_date", "stop_date":
                print(f'    {key}: ' +
                      (dic[key] if key in dic else "not present"))
    if args.dry_run:
        if args.verbose:
            print("Dry run -- not updating.")
    else:
        z.attrs.update(new_attrs)
        zarr.consolidate_metadata(args.zarr)
        if args.verbose:
            print("Attributes updated.")


if __name__ == "__main__":
    main()
