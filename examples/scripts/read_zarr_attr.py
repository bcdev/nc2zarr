#!/usr/bin/env python

"""Read metadata attribute from a Zarr file and write it to stdout.

Usage: read_zarr_attr.py <ZARR_PATH> <ATTRIBUTE_NAME>

The Zarr path can be a local file. If the appropriate Python libraries
and access credentials are present, http or S3 URLs may also be used.
Examples:

./read_zarr_attr.py /some/path/OCEANCOLOUR_ATL_CHL_L4_NRT_OBSERVATIONS_009_037.zarr sensor

./read_zarr_attr.py http://cop-services.s3.eu-central-1.amazonaws.com/OCEANCOLOUR_MED_OPTICS_L3_NRT_OBSERVATIONS_009_038.zarr/ product_level

./read_zarr_attr.py s3://cop-services/LWQ-NRT-300m.zarr title
"""

import sys
import zarr

filename = sys.argv[1]
metadata_key = sys.argv[2]
z = zarr.open(filename, mode="r")
if metadata_key in z.attrs:
    print(z.attrs[metadata_key])
else:
    print(f"Key \"{metadata_key}\" not found.")
    sys.exit(1)

