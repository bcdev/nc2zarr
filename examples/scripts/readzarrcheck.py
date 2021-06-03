#!/usr/bin/env python

"""readzarrcheck.py: test that a Zarr store is readable

This script attempts to read one data value from each chunk in a specified
Zarr store, and reports any errors encountered. It exits with status 0 if no
read errors were encountered, 1 otherwise. Note that checking an entire store
like this can be slow (especially for large or remote stores), and that many
potential problems (e.g. missing chunks or incorrect data values) are not
detected by this script.

The Zarr path can be a local file. If the appropriate Python libraries
and access credentials are present, http or S3 URLs may also be used.
Examples:

./readzarrcheck.py /some/path/OCEANCOLOUR_ATL_CHL_L4_NRT_OBSERVATIONS_009_037.zarr

./readzarrcheck.py http://cop-services.s3.eu-central-1.amazonaws.com/OCEANCOLOUR_MED_OPTICS_L3_NRT_OBSERVATIONS_009_038.zarr/

./readzarrcheck.py s3://cop-services/LWQ-NRT-300m.zarr
"""

import sys
import argparse
import zarr
from typing import List


def main():
    parser = argparse.ArgumentParser(
        description="Test that each chunk of a zarr store is readable. "
                    "See docstring in source code for more details."
    )
    parser.add_argument("zarr", type=str, metavar="PATH_OR_URL")
    args = parser.parse_args()
    zarr_file = args.zarr
    z = zarr.open(zarr_file, mode="r")
    n_errors = 0
    for name, array in z.arrays():
        n_errors += read_data(name, array)
    print(f"{n_errors} error(s) encountered.")
    sys.exit(1 if n_errors > 0 else 0)


def read_data(array_name: str, array: zarr.Array) -> int:

    n_errors = 0

    def read_data_rec(coords: List[int], shape: List[int], chunks: List[int]):
        if len(shape) > 0:
            # Peel off the first remaining dimension and loop through it
            for i in range(0, shape[0], chunks[0]):
                read_data_rec(coords + [i], shape[1:], chunks[1:])
        else:
            # We've reached a fully specified co-ordinate -- try to read it.
            # We use a very broad exception catch and disable the associated
            # inspection, since we can't predict what errors might be thrown.
            # noinspection PyBroadException
            try:
                # We don't care about the value, only about whether an
                # exception is raised.
                _ = array[tuple(coords)]
            except Exception:
                print(f"Error reading {array_name}{coords}")
                nonlocal n_errors
                n_errors += 1

    read_data_rec([], list(array.shape), list(array.chunks))
    return n_errors


if __name__ == "__main__":
    main()
