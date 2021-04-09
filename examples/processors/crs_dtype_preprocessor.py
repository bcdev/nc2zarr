import numpy as np
import xarray as xr


def preprocess_crs(ds):
    """Force a dtype of "<U1" on the crs array

    The LWQ dataset includes a dimensionless array called "crs", containing
    a single placeholder value and used as an attachment point for
    CRS-related attributes. There are two problems with the crs array:

    1. Its dtype changes between data format versions:
       for v1.2 NetCDFs, it's an int, and for 1.3 and 1.4 it's
       a char. Appending different dtypes to each other could
       cause problems.

    2. The dtype "char" alone (corresponding to a numpy dtype of
       "|S1") also constitutes a problem, since xarray refuses to
       append byte strings in zarrs -- probably a bug or oversight,
       since there seems to be no reason not to do so.

    This preprocessor replaces the CRS array with one of type "<U1"
    (length-1 Unicode string) bearing the original attributes.

    We create a new crs DataArray with a <U1 dtype and copy the attributes
    rather than using ds.crs.astype, since the latter can't guarantee that
    a cast (e.g. from int) will actually be possible.
    """

    da = xr.DataArray(data=np.array("", dtype="<U1"), name="crs")
    da.attrs.update(ds.crs.attrs)
    ds["crs"] = da
    return ds
