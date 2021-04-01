#!/usr/bin/env python3

import pandas as pd
import xarray as xr
import argparse
from datetime import datetime
import re
import nc2zarr
import nc2zarr.opener
import nc2zarr.config
import nc2zarr.converter
import fsspec


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", "-c", type=str, action="append")
    parser.add_argument("--verbose", "-v", action="count", default=0)
    parser.add_argument("--dry-run", "-d", action="store_true")
    parser.add_argument("dateformat", type=str,
                        help="Date format in path (e.g. /%Y/%m/%d/)")
    args = parser.parse_args()
    config = nc2zarr.config.load_config(args.config, return_kwargs=True)
    all_paths = nc2zarr.opener.DatasetOpener.resolve_input_paths(
        config["input_paths"])
    last_date_in_zarr = get_last_date_in_zarr(config)

    def pathfilter(path):
        return is_path_after_datetime(path, args.dateformat, last_date_in_zarr)
    new_paths = list(filter(pathfilter, all_paths))
    
    if len(new_paths) == 0:
        print("Zarr dataset is already up to date.")
        return

    config["output_overwrite"] = False
    config["output_append"] = True
    config["input_paths"] = new_paths
    config["verbosity"] = args.verbose
    config["dry_run"] = args.dry_run

    nc2zarr.converter.Converter(**config).run()


def get_last_date_in_zarr(config: dict) -> datetime:
    zarrfile = config["output_path"]
    if zarrfile.startswith("s3://"):
        filesystem = fsspec.filesystem("s3", **(config["output_s3"] or {}))
        dataset = xr.open_zarr(
            store=filesystem.get_mapper(zarrfile),  # anon=True
            consolidated=True
    )
    else:
        dataset = xr.open_zarr(zarrfile, consolidated=True)
    return pd.Timestamp(dataset.time[-1].values).to_pydatetime()


def is_path_after_datetime(date_rep: str, date_format: str,
                           date_limit: datetime) -> bool:
    return extract_date(date_rep, date_format) > date_limit


def extract_date(date_rep: str, date_format: str) -> datetime:
    """
    Extract a date from a substring of a supplied string

    :param date_rep: a string containing a date representation
    :param date_format: a date format using %Y, %m, and/or %d
    :return: the datetime corresponding to the first substring
             matching the given format within the given string
    """
    regex = date_format.replace("%Y", r"\d\d\d\d")\
                       .replace("%m", r"\d\d")\
                       .replace("%d", r"\d\d")
    substring = re.search(regex, date_rep).group()
    return datetime.strptime(substring, date_format)


if __name__ == "__main__":
    main()
