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

def process_single_bdgd(row_data):
    """
    Processa um único BDGD em um processo separado
    """
    bdgd_name = row_data['title']
    bdgd_id = row_data['id']
    
    try:
        # Verificar se já existe (conexão local para cada processo)
        if region_already_exists(bdgd_id):
            return {'status': 'skipped', 'bdgd_name': bdgd_name, 'bdgd_id': bdgd_id}
        
        # Processar o BDGD
        gdf = get_region(bdgd_name, bdgd_id)
        save_gdf_to_db(gdf)
        
        return {'status': 'success', 'bdgd_name': bdgd_name, 'bdgd_id': bdgd_id}
        
    except Exception as e:
        return {'status': 'error', 'bdgd_name': bdgd_name, 'bdgd_id': bdgd_id, 'error': str(e)}

def process_bdgds_parallel(bdgd_df, max_workers=None):
    """
    Processa BDGDs em paralelo usando multiprocessing
    """
    if max_workers is None:
        max_workers = mp.cpu_count()
    
    # Converter DataFrame em lista de dicts para passar para os processos
    bdgd_list = bdgd_df.to_dict('records')
    
    print(f"Processando {len(bdgd_list)} BDGDs usando {max_workers} processadores")
    
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        # Usar tqdm para mostrar progresso
        if VERBOSE:
            results = list(tqdm(
                executor.map(process_single_bdgd, bdgd_list),
                total=len(bdgd_list),
                desc='Processing BDGDs'
            ))
        else:
            results = list(executor.map(process_single_bdgd, bdgd_list))
    
    return results

def print_processing_summary(results):
    """
    Imprime resumo do processamento
    """
    successful = [r for r in results if r['status'] == 'success']
    skipped = [r for r in results if r['status'] == 'skipped']
    failed = [r for r in results if r['status'] == 'error']
    
    print(f"\n=== Resumo do Processamento ===")
    print(f"Total processados: {len(results)}")
    print(f"Sucessos: {len(successful)}")
    print(f"Ignorados (já existiam): {len(skipped)}")
    print(f"Erros: {len(failed)}")
    
    if failed:
        print(f"\nBDGDs com erro:")
        for result in failed:
            print(f"- {result['bdgd_name']}: {result['error']}")
    
if __name__ == '__main__':
    ensure_table_exists()

    # Obter lista de BDGDs do ano mais recente
    bdgd_df = get_bdgd_list_df()
    
    # Opções de processamento
    USE_PARALLEL = True  # Altere para False para usar processamento sequencial
    MAX_WORKERS = 4      # Número de processadores a usar (None = usar todos)
    
    if USE_PARALLEL:
        # Processamento paralelo
        results = process_bdgds_parallel(bdgd_df, MAX_WORKERS)
        print_processing_summary(results)
    else:
        # Processamento sequencial (código original)
        iterator = tqdm(bdgd_df.iterrows(), total=len(bdgd_df), desc='Extracting regions') if VERBOSE else bdgd_df.iterrows()
        
        for _, row in iterator:
            bdgd_name = row['title']
            bdgd_id = row['id']
            
            if region_already_exists(bdgd_id):
                continue
            gdf = get_region(bdgd_name, bdgd_id)
            save_gdf_to_db(gdf)