
import apache_beam as beam
from pangeo_forge_recipes.patterns import pattern_from_file_sequence
from pangeo_forge_recipes.transforms import OpenURLWithFSSpec, OpenWithXarray, StoreToZarr, OpenWithKerchunk, WriteCombinedReference

years = range(2002, 2002)

input_urls = [f'https://zenodo.org/record/7072512/files/CASM_SM_{year}.nc' for year in years]

pattern = pattern_from_file_sequence(input_urls, concat_dim='date')
recipes = (
    beam.Create(pattern.items())
    | OpenURLWithFSSpec()
    # | OpenWithKerchunk(file_type=pattern.file_type)
    # | WriteCombinedReference(
    #     concat_dims=["date"],
    #     identical_dims=["lat", "lon"],
    #     store_name='temp',
    #     output_file_name='reference.parquet',
    # )
    | OpenWithXarray()
    | StoreToZarr(
        target_chunks={'date': 20},
        target_root = ".",
        store_name='CASM.zarr',
        combine_dims=pattern.combine_dim_keys,
    )
)


import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions


from apache_beam.runners.dask.dask_runner import DaskOptions
from apache_beam.runners.dask.dask_runner import DaskRunner
# Out[2]: '2.42.0'

options = PipelineOptions(["runner=DaskRunner"])

with beam.Pipeline(runner=DaskRunner) as p:
    p | recipes


# from pangeo_forge_recipes.patterns import ConcatDim, FilePattern, MergeDim, pattern_from_file_sequence
# import apache_beam as beam
# import pandas as pd 
# import os 
# from pangeo_forge_recipes.transforms import (
#     OpenWithKerchunk,
#     WriteCombinedReference,
# )
# from pangeo_forge_recipes import patterns
# from fsspec.implementations.reference import ReferenceFileSystem
# from apache_beam.options.pipeline_options import PipelineOptions



# def _nasa_nex_df() -> pd.DataFrame:
#     return pd.read_csv('s3://carbonplan-share/nasa-nex-reference/nasa_nex_formatted.csv')
#     # specify ensemble_member doesn't change # in query (ie, choosing a gcm + scenario gives a unique ensemble member)

# def _GCM_scenarios(df: pd.DataFrame)->pd.DataFrame:
#     # Reads in nasa-nex dataframe
#     colapsed_df = df.groupby(['GCM','scenario','ensemble_member'])['variable'].apply(list).reset_index()
#     colapsed_df['variable'] = colapsed_df['variable'].apply(lambda x: list(set(x)))

#     return colapsed_df

# def read_catalog_file(catalog_url):
#     return pd.read_csv(catalog_url)

# df = _nasa_nex_df()
# unique_df = _GCM_scenarios(df)
# catalog_url = 's3://carbonplan-share/nasa-nex-reference/reference_catalog_pgf_test.csv'
# prune_bool = True


# row = unique_df.iloc[0]
# file_pattern = df.query(f"GCM == '{row['GCM']}'  & scenario == '{row['scenario']}'")
# grid_code = file_pattern.url.str.split('.',expand=True)[0].str[-7:-5].iloc[0]
# avail_years = file_pattern.url.str.split('.',expand=True)[0].str[-4::]
# max_year = max(avail_years)
# min_year = min(avail_years)

# GCM = row['GCM']
# scenario = row['scenario']
# ensemble_member = row['ensemble_member']
# # Check if entry already exists
# cat_df = read_catalog_file(catalog_url)
# exists_bool = cat_df['ID'].str.contains(f'{GCM}_{scenario}').any()
# # if not exists_bool:

# def format_function(variable, time):
#     return f"s3://nex-gddp-cmip6/NEX-GDDP-CMIP6/{GCM}/{scenario}/{ensemble_member}/{variable}/{variable}_day_{GCM}_{scenario}_{ensemble_member}_{grid_code}_{time}.nc"

# years = list(range(int(min_year), int(max_year) + 1))
# variable_merge_dim = patterns.MergeDim("variable", keys=row['variable'])
# concat_dim = patterns.ConcatDim("time", keys=years)

# pattern = patterns.FilePattern(format_function, variable_merge_dim, concat_dim, file_type="netcdf4")

# if prune_bool:
#     pattern = pattern.prune(2)
# store_name = f"{GCM}_{scenario}"
# output_file_name = 'reference.parquet'

# recipes = (
#     beam.Create(pattern.items())
#     | OpenWithKerchunk(file_type=pattern.file_type)
#     | WriteCombinedReference(
#         concat_dims=["time"],
#         identical_dims=["lat", "lon"],
#         store_name=store_name,
#         output_file_name=output_file_name,
#     )
# )

# cat_df.loc[-1] = [store_name, os.path.join(store_name, output_file_name)]
# cat_df.reset_index().drop(['index'], axis=1).to_csv(catalog_url, index=False)


# with beam.Pipeline(options=options) as p:
#     p | recipes
# options = PipelineOptions([
#     "--runner=FlinkRunner",
#     "--flink_master=localhost:8081",
#     "--environment_type=LOOPBACK"
# ])


# pangeo-forge-runner bake --repo=~/Documents/carbonplan/nasa-nex-kerchunk -f ~/Documents/carbonplan/nasa-nex-kerchunk/feedstock/config.json --Bake.job_name=nasanex --prune

# pangeo-forge-runner bake --repo=/Users/juliusbusecke/Code/CMIP6-LEAP-feedstock -f /Users/juliusbusecke/Code/CMIP6-LEAP-feedstock/configs/config_local.json --Bake.job_name=cmip6test`

# // "root_path": "s3://carbonplan-share/nasa-nex-reference/references/"



from pangeo_forge_recipes.patterns import ConcatDim, FilePattern, MergeDim, pattern_from_file_sequence
import apache_beam as beam
import pandas as pd 
import os 
from pangeo_forge_recipes.transforms import (
    OpenWithKerchunk,
    WriteCombinedReference,
)
from pangeo_forge_recipes import patterns
from fsspec.implementations.reference import ReferenceFileSystem
from apache_beam.options.pipeline_options import PipelineOptions



def _nasa_nex_df() -> pd.DataFrame:
    return pd.read_csv('s3://carbonplan-share/nasa-nex-reference/nasa_nex_formatted.csv')
    # specify ensemble_member doesn't change # in query (ie, choosing a gcm + scenario gives a unique ensemble member)

def _GCM_scenarios(df: pd.DataFrame)->pd.DataFrame:
    # Reads in nasa-nex dataframe
    colapsed_df = df.groupby(['GCM','scenario','ensemble_member'])['variable'].apply(list).reset_index()
    colapsed_df['variable'] = colapsed_df['variable'].apply(lambda x: list(set(x)))

    return colapsed_df

def read_catalog_file(catalog_url):
    return pd.read_csv(catalog_url)

df = _nasa_nex_df()
unique_df = _GCM_scenarios(df)
catalog_url = 's3://carbonplan-share/nasa-nex-reference/reference_catalog.csv'
prune_bool = True



df = pd.read_csv(catalog_url)

df['url'] = 's3://carbonplan-share/nasa-nex-reference/references_prod/' + df['url']
# for index,row in unique_df.iterrows():
#     file_pattern = df.query(f"GCM == '{row['GCM']}'  & scenario == '{row['scenario']}'")
#     grid_code = file_pattern.url.str.split('.',expand=True)[0].str[-7:-5].iloc[0]
#     avail_years = file_pattern.url.str.split('.',expand=True)[0].str[-4::]
#     max_year = max(avail_years)
#     min_year = min(avail_years)

#     GCM = row['GCM']
#     scenario = row['scenario']
#     ensemble_member = row['ensemble_member']
#     # Check if entry already exists
#     cat_df = read_catalog_file(catalog_url)
#     exists_bool = cat_df['ID'].str.contains(f'{GCM}_{scenario}').any()
#     # if not exists_bool:

#     def format_function(variable, time):
#         return f"s3://nex-gddp-cmip6/NEX-GDDP-CMIP6/{GCM}/{scenario}/{ensemble_member}/{variable}/{variable}_day_{GCM}_{scenario}_{ensemble_member}_{grid_code}_{time}.nc"

#     years = list(range(int(min_year), int(max_year) + 1))
#     variable_merge_dim = patterns.MergeDim("variable", keys=row['variable'])
#     concat_dim = patterns.ConcatDim("time", keys=years)

#     pattern = patterns.FilePattern(format_function, variable_merge_dim, concat_dim, file_type="netcdf4")

#     if prune_bool:
#         pattern = pattern.prune(2)
#     store_name = f"{GCM}_{scenario}"
#     output_file_name = 'reference.parquet'

#     pattern = (
#         beam.Create(pattern.items())
#         | OpenWithKerchunk(file_type=pattern.file_type)
#         | WriteCombinedReference(
#             concat_dims=["time"],
#             identical_dims=["lat", "lon"],
#             store_name=store_name,
#             output_file_name=output_file_name,
#         )
#     )

#     cat_df.loc[-1] = [store_name, os.path.join(store_name, output_file_name)]
#     cat_df.reset_index().drop(['index'], axis=1).to_csv(catalog_url, index=False)



# # pangeo-forge-runner bake --repo=~/Documents/carbonplan/nasa-nex-kerchunk -f ~/Documents/carbonplan/nasa-nex-kerchunk/feedstock/config.json --Bake.job_name=nasanex --prune

# # pangeo-forge-runner bake --repo=/Users/juliusbusecke/Code/CMIP6-LEAP-feedstock -f /Users/juliusbusecke/Code/CMIP6-LEAP-feedstock/configs/config_local.json --Bake.job_name=cmip6test`

# # // "root_path": "s3://carbonplan-share/nasa-nex-reference/references/"
