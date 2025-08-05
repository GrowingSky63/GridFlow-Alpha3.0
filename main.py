from bdgd_downloader import BDGDDownloader, BDGDListDownloader
from sqlalchemy import Column, DateTime, Integer, String, UniqueConstraint, create_engine, URL, Table, MetaData
from sqlalchemy.exc import IntegrityError
from geoalchemy2 import Geometry
import geopandas as gpd
import pandas as pd
from datetime import datetime
from tempfile import TemporaryDirectory
from tqdm import tqdm
import os, dotenv
import multiprocessing as mp
from concurrent.futures import ProcessPoolExecutor
from functools import partial

VERBOSE = True

dotenv.load_dotenv(dotenv.find_dotenv())
drivername = 'postgresql'
username = 'gridflow'
password = os.getenv('DB_PASSWORD', 'postgres')
host = '172.25.0.233'
port = 5432
database = 'bdgd'
url = URL.create(
    drivername='postgresql',
    username='gridflow',
    password=os.getenv('DB_PASSWORD', 'postgres'),
    host='172.25.0.233',
    port=5432,
    database='bdgd'
)

engine = create_engine(url, echo=False)

metadata = MetaData()

region_table = Table(
    'region',
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("geometry", Geometry('MULTIPOLYGON', srid=4326)),
    Column("bdgd_name", String, unique=True),
    Column("bdgd_id", String, unique=True),
    Column("cod_id", String),
    Column("dist", String),
    Column("year_ref", DateTime),
    UniqueConstraint("cod_id", "year_ref")
)

def get_bdgd_list_df(year: int | None = None):
    with TemporaryDirectory(prefix='gridflow-bdgd-list-') as temp_folder:
        with BDGDListDownloader(temp_folder) as f:
            df = pd.read_csv(f)
            df = df[df['type'] == 'File Geodatabase']
            df = df[df['title'] != "DUP"]
            df['year'] = df['title'].apply(lambda s: int(s[-10:-6]))
            df = df[['id', 'title', 'year']]
            df = df.sort_values(['year', 'title'], ascending=[False, True])

            df = df[df['year'] == df['year'].iloc[0]] if not year else df[df['year'] == year]
            return df[['id', 'title']]

def ensure_table_exists():
    with engine.begin() as conn:
        metadata.create_all(conn)

def region_already_exists(bdgd_id):
    with engine.begin() as conn:
        result = conn.execute(region_table.select().where(region_table.c.bdgd_id == bdgd_id)).all()
    return len(result) > 0

def get_region(bdgd_name, bdgd_id):
    with TemporaryDirectory(prefix='gridflow-bdgd-') as temp_folder:
        with BDGDDownloader(bdgd_id, bdgd_name, temp_folder, True, VERBOSE) as bdgd_file:
            gdf = gpd.read_file(bdgd_file, layer='ARAT')
            gdf = gdf.rename(columns={'COD_ID': 'cod_id', 'DIST': 'dist'})
            gdf = gdf[['cod_id', 'dist', 'geometry']]
            gdf['bdgd_name'] = bdgd_name
            gdf['bdgd_id'] = bdgd_id
            gdf['year_ref'] = datetime(
                year=int(bdgd_name[-10:-6]),
                month=int(bdgd_name[-5:-3]),
                day=int(bdgd_name[-2:])
            )
            return gdf

def save_gdf_to_db(gdf: gpd.GeoDataFrame):
    for _, row in gdf.iterrows():
        try:
            with engine.begin() as conn:
                stmt = region_table.insert().values(
                    geometry=row['geometry'].wkt,
                    bdgd_name=row['bdgd_name'],
                    bdgd_id=row['bdgd_id'],
                    cod_id=row['cod_id'],
                    dist=row['dist'],
                    year_ref=row['year_ref']
                )
                conn.execute(stmt)
        except IntegrityError as e:
            continue
        except Exception as e:
            raise
    
if __name__ == '__main__':
    ensure_table_exists()

    # Obter lista de BDGDs do ano mais recente
    bdgd_df = get_bdgd_list_df(2023)
    
    for _, row in tqdm(bdgd_df.iterrows(), total=len(bdgd_df), desc='Extracting regions') if VERBOSE else bdgd_df.iterrows():
        bdgd_name = row['title']
        bdgd_id = row['id']
        
        if region_already_exists(bdgd_id):
            continue
        gdf = get_region(bdgd_name, bdgd_id)
        save_gdf_to_db(gdf)