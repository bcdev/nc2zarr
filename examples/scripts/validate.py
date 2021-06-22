#!/usr/bin/env python3

"""Perform simple validation of converted Zarrs against source NetCDFs

This script reads NetCDF files, each representing a single time-slice, and
checks that the data in them matches the corresponding data in a specified
Zarr file. It is intended for spot checks of nc2zarr output against the
corresponding source NetCDFs. It is configured using a YAML file, which
is formatted like the following example:

input_files:
  -
    zarr: "/some/path/zarr_name_1.zarr"
    netcdf_prefix: "/optional/path/containing/the/netcfs"
    netcdfs:
      - "optional/subpath/1/netcdf_1.nc"
      - "optional/subpath/2/netcdf_2.nc"
      - "optional/subpath/n/netcdf_n.nc"
  -
    zarr: "/some/path/zarr_name_2.zarr"
    netcdfs:
      - "another_netcdf_1.nc"
      - "another_netcdf_2.nc"

"""

import argparse
import os
import sys
from time import strftime
from typing import Dict
from typing import List
from typing import Union

import numpy as np
import xarray as xr
import yaml


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("config", type=str)
    parser.add_argument("--verbose", "-v", action="count", default=0,
                        help="Verbose output (repeat for even more verbosity)")
    parser.add_argument("--stop", "-s", action="store_true",
                        help="Stop on first validation failure.")
    args = parser.parse_args()
    with open(args.config, "r") as fh:
        config_dict = yaml.safe_load(fh)
    assert("input_files" in config_dict)
    validator = Validator(config_dict["input_files"], args.verbose,
                          args.stop)
    all_valid = validator.validate_all()
    log(f"Validation {'succeeded' if all_valid else 'failed'}.")
    sys.exit(0 if all_valid else 1)


class Validator:

    def __init__(self, input_files: List[Dict[str, Union[str, List]]],
                 verbosity: int, stop: bool):
        self.input_files = input_files
        self.verbosity = verbosity
        self.stop = stop

    def validate_all(self) -> bool:
        all_valid = True
        for record in self.input_files:
            for netcdf in record["netcdfs"]:
                valid = self.validate(
                    os.path.join(record.get("netcdf_prefix", ""), netcdf),
                    record["zarr"])
                if self.stop and not valid:
                    return False
                all_valid = all_valid and valid
        return all_valid

    def validate(self, nc_path: str, zarr_path: str) -> bool:
        nc_valid = True
        nc_ds = xr.open_dataset(nc_path, decode_cf=False, engine="netcdf4",
                                chunks="auto")
        assert(nc_ds.time.shape == (1,))
        zarr_ds = xr.open_zarr(zarr_path, decode_cf=False)
        zarr_slice = zarr_ds.where(zarr_ds.time == nc_ds.time[0], drop=True)
        if self.verbosity > 1:
            log("Starting validation of " + nc_path)
        for var in nc_ds.coords:
            # There can be slight variations in exact coordinate values
            # (e.g. OCEANCOLOUR_BS_CHL_L4_NRT_OBSERVATIONS_009_045 on 2013-06-19
            # vs. 2019-06-17) so we accommodate this with a tolerance parameter.
            var_valid = np.allclose(nc_ds[var].data, zarr_slice[var].data,
                                    atol=0.01)
            nc_valid = nc_valid and var_valid
            if self.verbosity > 1:
                log(str(var) + "\t" + ("pass" if var_valid else "fail"))
        # A converted Zarr may not contain all the original data variables,
        # so we filter out NetCDF variables which are absent in the Zarr.
        zarr_data_vars = set(map(str, zarr_ds.data_vars))
        vars_to_check = filter(lambda v: v in zarr_data_vars,
                               map(str, nc_ds.data_vars))
        for var in vars_to_check:
            var_valid = np.all(nc_ds[var].data == zarr_slice[var].data)
            nc_valid = nc_valid and var_valid
            if self.verbosity > 1:
                log(str(var) + "\t" + ("pass" if var_valid else "fail"))
        if self.verbosity > 0:
            log(str(nc_path) + "\t" + ("pass" if nc_valid else "fail"))
        return nc_valid


def log(message: str):
    print(strftime("[%Y-%m-%d %H:%M] ") + message, flush=True)


if __name__ == "__main__":
    main()
