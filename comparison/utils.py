# Utils for loading inputs, manipulating data, and writing out results
import xclim
import thermofeel as tf
import xarray as xr
import numpy as np
import dask


def adjust_pressure(temperature, elevation):
    """
    Approximate surface pressure given the elevation and temperature.
    Method from https://doi.org/10.1038/s41598-019-50047-w
    """
    return 101325 * np.power(10, -elevation / (18400 * temperature / 273.15))


def load_elev():
    elev = xr.open_zarr(
        "s3://carbonplan-climate-impacts/extreme-heat/v1.0/inputs/elevation.zarr",
        storage_options={"anon": True},
    )
    elev = elev.chunk({"lat": -1, "lon": -1}).compute()
    return elev


## calc wbgt
def wbgt(wbt, bgt, tas):
    """
    Calculate wet bulb globe temperature as linear combination of component
    temperatures. All should be in Celcius.
    """
    wbgt = 0.7 * wbt + 0.2 * bgt + 0.1 * tas
    return wbgt


def generate_WBGT(ds: xr.Dataset, output_fpath: str, elev: xr.Dataset):
    # calculate elevation-adjusted pressure
    ds["ps"] = xr.apply_ufunc(adjust_pressure, ds["tas"], elev, dask="allowed").rename(
        {"elevation": "ps"}
    )["ps"]
    ds["ps"].attrs["units"] = "Pa"
    ds["hurs"] = xclim.indices.relative_humidity(
        tas=ds["tasmax"], huss=ds["huss"], ps=ds["ps"]
    )
    ds["tasmax"].attrs = {}

    # windspeed assumption of 0.5 m/s (approximating shaded/indoor
    # conditions)
    ds["sfcWind"] = (ds["tas"] - ds["tas"]) + 0.5
    ds["WBT"] = tf.thermofeel.calculate_wbt(ds["tasmax"] - 273.15, ds["hurs"])

    ds["BGT"] = tf.thermofeel.calculate_bgt(ds["tasmax"], ds["tasmax"], ds["sfcWind"])
    ds["WBGT"] = wbgt(ds["WBT"], ds["BGT"], ds["tasmax"] - 273.15)
    ds["WBGT"].attrs["units"] = "degC"
    ds = ds[["WBGT"]]
    ds = dask.optimize(ds)[0]
    return ds.to_zarr(output_fpath, consolidated=True, compute=True, mode="w")
