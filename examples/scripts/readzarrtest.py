#!/usr/bin/env python

import sys
import argparse
import zarr
from typing import List


def main():
    parser = argparse.ArgumentParser(
        description="Test that each chunk of a zarr file is readable."
    )
    parser.add_argument("zarr", type=str, metavar="PATH_OR_URL")
    args = parser.parse_args()
    zarr_file = args.zarr
    z = zarr.open(zarr_file, mode="r")
    error = False
    for name, array in z.arrays():
        error = error or read_data(name, array)
    if not error:
        print("No errors encountered.")
    sys.exit(1 if error else 0)


def read_data(array_name: str, array: zarr.Array) -> bool:

    error = False

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
                nonlocal error
                error = True

    read_data_rec([], list(array.shape), list(array.chunks))
    return error


if __name__ == "__main__":
    main()
