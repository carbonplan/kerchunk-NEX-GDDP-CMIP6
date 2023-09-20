import pandas as pd 
import xarray as xr 
import s3fs
import hvplot.xarray
import glob
import logging
from tempfile import TemporaryDirectory
from fsspec.implementations.reference import LazyReferenceMapper, ReferenceFileSystem
import os 
import dask
import fsspec
import s3fs
import ujson
from distributed import Client
from kerchunk.combine import MultiZarrToZarr
from kerchunk.hdf import SingleHdf5ToZarr




client = Client(n_workers=8, silence_logs=logging.ERROR)
client


### ---------------------------------------
### Insert reading funcs
### ---------------------------------------
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

def generate_json_reference(fil, output_dir: str):
    fs_read = fsspec.filesystem("s3", anon=True, skip_instance_cache=True)
    so = dict(mode="rb", anon=True, default_fill_cache=False, default_cache_type="first")
    with fs_read.open(fil, **so) as infile:
        h5chunks = SingleHdf5ToZarr(infile, fil, inline_threshold=300)
        fname = fil.split("/")[-1].strip(".nc")
        outf = f"{output_dir}/{fname}.json"
        with open(outf, "wb") as f:
            f.write(ujson.dumps(h5chunks.translate()).encode())
        return outf
### ---------------------------------------
### ---------------------------------------
### ---------------------------------------

df = _nasa_nex_df()
unique_df = _GCM_scenarios(df)
catalog_url = 's3://carbonplan-share/nasa-nex-reference/reference_catalog_dask.csv'
prune_bool = True

for index,row in unique_df.iterrows():
    file_pattern = df.query(f"GCM == '{row['GCM']}'  & scenario == '{row['scenario']}'")

    GCM = row['GCM']
    scenario = row['scenario']
    target_root = "s3://carbonplan-share/nasa-nex-reference/dask_references/"

    store_name = f"{GCM}_{scenario}"
    output_file_name = 'reference.parquet'


    # Check if entry already exists
    cat_df = read_catalog_file(catalog_url)
    exists_bool = cat_df['ID'].str.contains(f'{GCM}_{scenario}').any()

    if not exists_bool:
        if prune_bool:
            file_pattern = file_pattern[0:2]
        
        fs_read = fsspec.filesystem("s3", anon=True, skip_instance_cache=True)
        so = dict(mode="rb", anon=True, default_fill_cache=False, default_cache_type="first")
        td = TemporaryDirectory()
        temp_dir = td.name

        # Generate Dask Delayed objects
        tasks = [dask.delayed(generate_json_reference)(fil, temp_dir) for fil in file_pattern]
        dask.compute(tasks)

        output_files = glob.glob(f"{temp_dir}/*.json")

        fs = fsspec.filesystem("s3")
        outpath = target_root + store_name + '/' + output_file_name

        if fs.exists(outpath):
            fs.rm(outpath, recursive=True)
        fs.makedir(outpath)

        out = LazyReferenceMapper.create(1000, outpath, fs)

        mzz = MultiZarrToZarr(
            output_files,
            remote_protocol="memory",
            concat_dims=["time"],
            identical_dims=["lat", "lon"],
            out=out,
        ).translate()

        out.flush()

        cat_df.loc[-1] = [store_name, os.path.join(target_root, store_name, output_file_name)]
        cat_df.reset_index().drop(['index'], axis=1).to_csv(catalog_url, index=False)
        break 

