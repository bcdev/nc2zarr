def preprocess_ensure_calendar_lower_case(dataset):
    """Force value of calendar attribute of time variable to be lower case

    If the value of the time variable's calendar attribute is not lower
    case, xarray (as of version 0.17.0) does not recognize it as a standard
    calendar and reads it into a cftime.DatetimeGregorian rather than a
    datetime64[ns]. See https://github.com/pydata/xarray/issues/5093 .
    This preprocessor forces the value into lower-case to work around the
    xarray bug until a fix is released.

    :param dataset: input dataset
    :return: dataset with lower-case value for calendar attribute of time
    """

    if "time" in dataset.variables and \
       "calendar" in dataset.time.attrs and \
       not dataset.time.attrs["calendar"].islower():
        dataset.time.attrs["calendar"] = \
            dataset.time.attrs["calendar"].lower()
    return dataset
