#!/usr/bin/env python3

"""Find and delete files identical to a specified file.

This utility is useful for removing NaN chunks from generated Zarrs.
"""

import argparse
import os
import pathlib


def main():
    parser = argparse.ArgumentParser(
        description="Find and delete files identical to a specified file."
    )
    parser.add_argument("--dry-run", "-d", action="store_true",
                        help="Don't actually delete, just show what "
                             "would have been deleted.")
    parser.add_argument("--verbose", "-v", action="store_true")
    parser.add_argument("target", type=str,
                        help="Delete files identical to this one.")
    parser.add_argument("search_path", type=str, nargs="+",
                        help="Look for files (recursively) in these "
                             "directories.")
    args = parser.parse_args()
    pruner = Pruner(args.target, args.search_path, args.dry_run, args.verbose)
    pruner.prune()


class Pruner:

    def __init__(self, target: str, search_path: str, dry_run: bool,
                 verbose: bool):
        self.target = target
        self.search_path = search_path
        self.dry_run = dry_run
        self.verbose = verbose
        self.buffering = 10000000

    def prune(self):
        with open(self.target, "rb", buffering=self.buffering) as fh:
            target = fh.read()
        for search_root in self.search_path:
            self.find(target, search_root)

    def find(self, target, search_root):
        for parent, dirnames, filenames in os.walk(search_root):
            for filename in filenames:
                path = pathlib.Path(parent) / filename
                if self.match(target, path):
                    if self.dry_run:
                        if self.verbose:
                            print(f"Match (dry run, not deleting): {path}")
                    else:
                        print(f"Deleting {path}")
                        os.remove(path)

    def match(self, expected_data: bytes, path: pathlib.Path):
        if not path.is_file():
            return False
        if len(expected_data) != path.stat().st_size:
            return False
        with open(path, "rb", buffering=self.buffering) as fh:
            data = fh.read()
        return expected_data == data


if __name__ == "__main__":
    main()
