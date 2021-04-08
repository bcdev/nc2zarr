#!/usr/bin/env python

import sys
import pathlib
import argparse
import zarrupdate


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", "-c", action="append", default=[],
                        help="additional configuration file(s) "
                             "(e.g. credentials)")
    parser.add_argument("--dry-run", "-d", action="store_true")
    args = parser.parse_args()

    script_path = pathlib.PurePath(__file__)
    sys.path.append(str(script_path.parents[1] / "processors"))

    config_dir = script_path.parents[1] / "eu-copernicus" / "configs"

    dataset_names = [
        "OCEANCOLOUR_ATL_CHL_L4_NRT_OBSERVATIONS_009_037",
        "OCEANCOLOUR_BAL_CHL_L3_NRT_OBSERVATIONS_009_049",
        "OCEANCOLOUR_BS_CHL_L4_NRT_OBSERVATIONS_009_045",
        "OCEANCOLOUR_MED_CHL_L4_NRT_OBSERVATIONS_009_041",
        "OCEANCOLOUR_MED_OPTICS_L3_NRT_OBSERVATIONS_009_038",
    ]

    for dataset_name in dataset_names:
        print(f"Updating {dataset_name}...", file=sys.stderr)
        zarrupdate.update_zarr(
            [str(config_dir / f"{dataset_name}.yaml"),
             str(config_dir / "OCEANCOLOUR_SHARED.yaml")] + args.config,
            "/%Y/%m/%d/", 1, args.dry_run)


if __name__ == "__main__":
    main()
