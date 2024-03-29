<p align="left" >
<a href='https://carbonplan.org'>
<picture>
  <source media="(prefers-color-scheme: dark)" srcset="https://carbonplan-assets.s3.amazonaws.com/monogram/light-small.png">
  <img alt="CarbonPlan monogram." height="48" src="https://carbonplan-assets.s3.amazonaws.com/monogram/dark-small.png">
</picture>
</a>
</p>

# kerchunk-NEX-GDDP-CMIP6

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)

This repo serves as a comparison of two methods for analyzing the NASA-NEX-GDDP-CMIP6 dataset. In these examples we are building off of the [extreme-heat analysis work](https://carbonplan.org/research/extreme-heat-explainer) done at CarbonPlan and using the calculation of WBGT (WetBulb Glob Temperature) as a testbed to compare two processing approaches. More details on our work in extreme-heat can be found in this [GitHub Repo](https://github.com/carbonplan/extreme-heat).

The first method (`comparison/heat_openmfdataset.ipynb`) uses `Xarray's` `open_mfdataset` function to loop through the NASA-NEX netcdf files, merge across variables and concat along the time dimension. The second approach (`comparison/heat_datatree.ipynb`) is an attempt to speed up and simplify this process using two python projects; `kerchunk` and `xarray-datatree`.

## Approach 1: open_mfdataset

The notebook `comparison/heat_openmfdataset.ipynb` represents a common approach to processing collections of NetCDF files using `Xarray's` `open_mfdataset`. This function allows you to open multiple NetCDF files as a single `Xarray` dataset, merging and concatenating along your desired dimensions. This notebook loops over each `GCM/scenario` combination in the NASA-NEX-GDDP-CMIP6 dataset, uses `open_mfdataset` to create a large `Xarray` dataset, does some custom processing and then writes to Zarr.

## Approach 2: xarray-datatree and kerchunk

The notebook `comparison/heat_datatree.ipynb` represents a different approach to this data processing pipeline. Instead of looping over each `GCM/scenario` combination and opening them with `open_mfdataset` , it uses `xarray-datatree` as a catalog to organize the `GCM/scenario` datasets and `kerchunk` to read them as if they were `Zarr` stores. Details on this approach are described in the next section.

### Generating Kerchunk References for NASA-NEX

The [Kerchunk](https://github.com/fsspec/kerchunk) project provides a way to access collections of archival data as if they were Analysis-Read Cloud-Optimized (ARCO) dataset formats such as Zarr. To accomplish this, `kerchunk` has to scan through the collection of NetCDF files (35.6 TB!) to build reference files. Once these `kerchunk` reference files are created, they can be re-used by any user. In addition, the reference size for this 35.6 TB dataset is only a few hundred MB.

### Xarray-Datatree as a Catalog

The NASA-NEX-GDDP-CMIP6 dataset contains ~150+ individual datasets of GCM and climate scenario combinations. Across these datasets, not all of the time dimensions and coordinates match. Because of this, they cannot be combined into a single `xarray dataset`. Fortunately, `xarray-datatree` can be used to organize all of these datasets into a "datatree", which is a higher level xarray metadata object that can be used to organize similar `xarray datasets`. We can load our `kerchunk` reference files for all of the NASA-NEX-GDDP-CMIP6 datasets into a single datatree and then use it as a catalog for the entire collection. Indiviual datasets can be access through the pattern: `dt[GCM/Scenario]`.

### Kerchunk References as a Zarr Store

Once we have our references generated and loaded into an `xarray-datatree`, we can process the individual datasets as if they were Zarr stores. Zarr is a cloud-optimized format, that provides concurrent access to individual chunks and generally can greatly improve i/o performance.

## Speed Comparison

To compare the two methods, we processed a subset of the total NASA-NEX-GDDP-CMIP6 dataset. The number of workers and instance type was identical for both methods. In the table below you can see that the `kerchunk + datatree` method was significantly faster. In addition, this speedup may increase, as some of the 3 minutes and 32 seconds was creating the datatree.

| Method              | # of Input Datasets | Temporal Extent | # of Workers | Worker Instance Type | Time                  |
| ------------------- | ------------------- | --------------- | ------------ | -------------------- | --------------------- |
| openmfdataset       | 20                  | 365 days        | 10           | m7i.xlarge           | 20 minutes 24 seconds |
| kerchunk + datatree | 20                  | 365 days        | 10           | m7i.xlarge           | 3 min 32 seconds      |

### Structure of the Repo

```
.
├── LICENSE
├── README.md
├── binder
│   └── environment.yml
├── comparison
│   ├── heat_datatree.ipynb
│   ├── heat_openmfdataset.ipynb
│   └── utils.py
└── generation
    ├── pangeo-forge
    └── parallel_reference_generation.ipynb
```

### Generating References

In this repo there are two examples of how to generate the `Kerchunk` reference files for the NASA-NEX-GDDP-CMIP6 dataset. `generation/parallel_reference_generation.ipynb` is a straightforward approach that uses `Kerchunk` to generate the individual references and `Dask` + `Coiled` to parallelize the reference generation. The other approach, `generation/feedstock/` contains the components for a `pangeo-forge recipe`. `Pangeo-Forge` is a open-source `ETL` project for producing ARCO datasets. In this example, `Kerchunk` is being used "under the hood" by `pangeo-forge-recipes` to generate the reference files. This `recipe` can then be run on a local machine or scaled out using `google-dataflow`, `apache-flink` or in the future `Dask`.

## license

All the code in this repository is [MIT](https://choosealicense.com/licenses/mit/)-licensed, but we request that you please provide attribution if reusing any of our digital content (graphics, logo, articles, etc.).

The NEX-GDD-CMIP6 dataset used to create the reference is licensed under Creative Commons Zero license (CC0). The underlying CMIP6 datasets used in the creation the NEX-GDD-CMIP6 dataset are licensed under Creative Commons - Attribution 4.0 (CC4).

## about us

CarbonPlan is a nonprofit organization that uses data and science for climate action. We aim to improve the transparency and scientific integrity of climate solutions with open data and tools. Find out more at [carbonplan.org](https://carbonplan.org/) or get in touch by [opening an issue](https://github.com/carbonplan/python-project-template/issues/new) or [sending us an email](mailto:hello@carbonplan.org).
