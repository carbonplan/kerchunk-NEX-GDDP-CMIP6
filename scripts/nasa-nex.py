from pangeo_forge_recipes.patterns import ConcatDim, FilePattern, MergeDim, pattern_from_file_sequence
import apache_beam as beam
import pandas as pd 
import os 
from pangeo_forge_recipes.transforms import (
    CombineReferences,
    OpenWithKerchunk,
    WriteCombinedReference,
)
from pangeo_forge_recipes import patterns
from fsspec.implementations.reference import ReferenceFileSystem
from apache_beam.runners.interactive.interactive_runner import InteractiveRunner
from apache_beam.options.pipeline_options import PipelineOptions

beam_options = PipelineOptions(direct_num_workers=8, direct_running_mode="multi_processing")


#df.value_counts(subset=['GCM','scenario'])
#gives
#164 entries with var/time combos ranging from 455 to 744. 
# timing 744 * 164 * # of time to create reference will be good time estimation 


# --------------- PLAN -----------------------
# OPEN/CREATE ref lookup file 
# Create DF lookup dataframe
# Iterate through  GCM, scenario
# If GCM, scenario ref exists in ref lookup table, skip
# Run beam pipeline with name, url, and injected variables
# Update ref lookup table/file


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

for index,row in unique_df.iterrows():
    file_pattern = df.query(f"GCM == '{row['GCM']}'  & scenario == '{row['scenario']}'")
    grid_code = file_pattern.url.str.split('.',expand=True)[0].str[-7:-5]
    avail_years = file_pattern.url.str.split('.',expand=True)[0].str[-4::]
    max_year = max(avail_years)
    min_year = min(avail_years)

    GCM = row['GCM']
    scenario = row['scenario']
    ensemble_member = row['ensemble_member']

    # Check if entry already exists
    cat_df = read_catalog_file(catalog_url)
    exists_bool = cat_df['ID'].str.contains(f'{GCM}_{scenario}').any()
    if not exists_bool:

        def format_function(variable, time):
            return f"s3://nex-gddp-cmip6/NEX-GDDP-CMIP6/{GCM}/{scenario}/{ensemble_member}/{variable}/{variable}_day_{GCM}_{scenario}_{ensemble_member}_{grid_code}_{time}.nc"

        years = list(range(int(min_year), int(max_year) + 1))
        variable_merge_dim = patterns.MergeDim("variable", keys=row['variable'])
        concat_dim = patterns.ConcatDim("time", keys=years)

        pattern = patterns.FilePattern(format_function, variable_merge_dim, concat_dim, file_type="netcdf4")

        if prune_bool:
            pattern = pattern.prune(2)
        target_root = "s3://carbonplan-share/nasa-nex-reference/references/"
        store_name = f"{GCM}_{scenario}"
        output_file_name = 'reference.parquet'

        transforms = (
            # Create a beam PCollection from our input file pattern
            beam.Create(pattern.items())
            # Open with Kerchunk and create references for each file
            | OpenWithKerchunk(file_type=pattern.file_type)
            # Use Kerchunk's `MultiZarrToZarr` functionality to combine the reference files into a single
            # reference file. *Note*: Setting the correct contact_dims and identical_dims is important.
            | CombineReferences(
            concat_dims=["time"],
            identical_dims=["lat", "lon"],
            )
            Write the combined Kerchunk reference to file.
            | WriteCombinedReference(
                store_name=store_name,
                target_root=target_root,
                concat_dims=["time"],
                output_file_name=output_file_name,
            )
        )
        break
        with beam.Pipeline(runner=InteractiveRunner(), options=beam_options) as p:
            p | transforms


        cat_df.loc[-1] = [store_name, os.path.join(target_root, store_name, output_file_name)]
        cat_df.reset_index().drop(['index'], axis=1).to_csv(catalog_url, index=False)
    
    



# # import xarray as xr 
# fs = ReferenceFileSystem(
#     url, remote_protocol="s3", target_protocol="s3", lazy=True
# )

# ds = xr.open_dataset(
#     fs.get_mapper(), engine="zarr", backend_kwargs={"consolidated": False}, chunks={}
# )

