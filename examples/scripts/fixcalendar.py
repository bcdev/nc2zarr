#!/usr/bin/env python

import sys
import argparse
import zarr
import nc2zarr.config


def main():
    parser = argparse.ArgumentParser(
        description=("Ensure that value of calendar attribute of time "
                     "variable in a Zarr store is in lower case.")
    )
    parser.add_argument("--config", "-c", type=str, action="append",
                        help="Read S3 configuration from this/these config(s)")
    parser.add_argument("--force", "-f", action="store_true",
                        help="Rename calendar even if it's already lower-case")
    parser.add_argument("--dry-run", "-s", action="store_true",
                        help="Don't rename, "
                             "just show what would have been done")
    parser.add_argument("zarr_store", type=str,
                        help="Zarr store specifier (path or URL)")
    args = parser.parse_args()
    store_arg = args.zarr_store

    if args.config and store_arg.lower().startswith("s3://"):
        s3_config = {}
        config = nc2zarr.config.load_config(args.config, return_kwargs=True)
        if "output_s3" in config:
            s3_config = config["output_s3"]
        # We have to create this store manually to set normalize_keys=False,
        # because normalize_keys=True can break consolidate_metadata.
        store = zarr.storage.FSStore(store_arg, mode="r+", **s3_config,
                                     normalize_keys=False)
    else:
        store = zarr.creation.normalize_store_arg(store_arg)
    
    z = zarr.open_group(store, mode="r+")
    calendar = z.time.attrs["calendar"]
    
    log(f"Current calendar: \"{calendar}\"")
    if calendar.islower() and not args.force:
        log("Already lower case; leaving unchanged.")
    else:
        new_calendar = calendar.lower()
        if args.dry_run:
            log(f"New name: \"{new_calendar}\"")
            log("Dry run requested -- not actually renaming.")
        else:
            log(f"Renaming to \"{new_calendar}\"...")
            z.time.attrs["calendar"] = new_calendar
            log("Consolidating...")
            zarr.consolidate_metadata(store)
            log("Done.")

        
def log(s):
    print(s, file=sys.stderr)
    

if __name__ == "__main__":
    main()
