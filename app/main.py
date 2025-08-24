from app.aneel_downloader import BDGDDownloader, BDGDListDownloader
from sqlalchemy import Column, DateTime, Float, ForeignKeyConstraint, Integer, String, UniqueConstraint, create_engine, URL, Table, MetaData, Executable
from sqlalchemy.exc import IntegrityError
from geoalchemy2 import Geometry
from geoalchemy2.functions import ST_Distance, ST_Transform, ST_GeomFromText, ST_Contains
from geoalchemy2.elements import WKTElement
import geopandas as gpd
import pandas as pd
from datetime import datetime
from tempfile import TemporaryDirectory
from tqdm import tqdm
import os

VERBOSE = True

def load_env_if_exists():
    try:
        import dotenv
        dotenv_path = dotenv.find_dotenv()
        if dotenv_path:
            dotenv.load_dotenv(dotenv_path)
    except ModuleNotFoundError:
        pass

def make_url_by_environment(dbname='postgres') -> URL:
    return URL.create(
        drivername=os.getenv('DB_DRIVER_NAME', 'postgresql'),
        host=os.getenv('DB_HOST', '127.0.0.1'),
        port=int(os.getenv('DB_PORT', '5432')),
        username=os.getenv('DB_USERNAME', 'postgres'),
        password=os.getenv('DB_PASSWORD', 'postgres'),
        database=os.getenv('DB_BDGD_NAME', dbname),
)

class BDGDManager:
    def __init__(self):
        self.setup_bdgd_db()

    def setup_bdgd_db(self):
        bdgd_url = make_url_by_environment('bdgd')
        self.bdgd_engine = create_engine(bdgd_url, echo=False)

        bdgd_metadata = MetaData()

        self.region_table = Table(
            'region',
            bdgd_metadata,
            Column('id', Integer, primary_key=True, autoincrement=True),
            Column('geometry', Geometry('MULTIPOLYGON', srid=4326)),
            Column('bdgd_name', String, unique=True),
            Column('bdgd_id', String, unique=True),
            Column('cod_id', String),
            Column('dist', String),
            Column('year_ref', DateTime),
            UniqueConstraint('cod_id', 'year_ref')
        )

        self.substation_table = Table(
            'substation',
            bdgd_metadata,
            Column('id', Integer, primary_key=True, autoincrement=True),
            Column('geometry', Geometry('MULTIPOLYGON', srid=4326)),
            Column('cod_id', String, unique=True),
            Column('dist', String),
            Column('name', String)
        )

        self.trhv_table = Table(
            'trhv',
            bdgd_metadata,
            Column('id', Integer, primary_key=True, autoincrement=True),
            Column('geometry', Geometry('POINT', srid=4326)),
            Column('cod_id', String, unique=True),
            Column('substation', String),
            Column('power', Float),
            ForeignKeyConstraint(['substation'], ['substation.cod_id'])
        )

    def get_bdgd_list_df(self, year: int | None = None):
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

    def region_already_exists(bdgd_id: str) -> bool:
        with engine.begin() as conn:
            stmt = region_table.select().where(region_table.c.bdgd_id == bdgd_id)
            result = conn.execute(stmt).all()
            return len(result) > 0

    def download_search_layers(bdgd_name: str, bdgd_id: str) -> dict[Table, gpd.GeoDataFrame]:
        with TemporaryDirectory(prefix='gridflow-bdgd-') as temp_folder:
            with BDGDDownloader(bdgd_id, bdgd_name, temp_folder, True, VERBOSE) as bdgd_file:
                region_gdf = gpd.read_file(bdgd_file, layer='ARAT')
                region_gdf = region_gdf.rename(columns={
                    'COD_ID': 'cod_id',
                    'DIST': 'dist'
                })
                region_gdf = region_gdf[['cod_id', 'dist', 'geometry']]
                region_gdf['bdgd_name'] = bdgd_name
                region_gdf['bdgd_id'] = bdgd_id
                region_gdf['year_ref'] = datetime(
                    year=int(bdgd_name[-10:-6]),
                    month=int(bdgd_name[-5:-3]),
                    day=int(bdgd_name[-2:])
                )

                substation_gdf = gpd.read_file(bdgd_file, layer='SUB')
                substation_gdf = substation_gdf.rename(columns={
                    'COD_ID': 'cod_id',
                    'DIST': 'dist',
                    'NOME': 'name'
                })
                substation_gdf = substation_gdf[['cod_id', 'dist', 'name', 'geometry']]

                trhv_gdf = gpd.read_file(bdgd_file, layer='UNTRAT')
                trhv_gdf = trhv_gdf.rename(columns={
                    'COD_ID': 'cod_id',
                    'SUB': 'substation',
                    'POT_NOM': 'power'
                })
                trhv_gdf = trhv_gdf[['cod_id', 'substation', 'power', 'geometry']]

                return {
                    region_table: region_gdf,
                    substation_table: substation_gdf,
                    trhv_table: trhv_gdf
                }

    def save_gdf_to_db(layer_table: Table, gdf: gpd.GeoDataFrame):
        for _, row in gdf.iterrows():
            try:
                if 'geometry' in row:
                    values = row.drop('geometry').to_dict()
                    values['geometry'] = WKTElement(row.geometry.wkt, srid=4326)
                else:
                    values = row.to_dict()
                with engine.begin() as conn:
                    conn.execute(layer_table.insert().values(values))
            except IntegrityError:
                continue
            except Exception:
                raise

    def get_most_recent_bdgd_for_coordinate(longitude: float, latitude: float):
        """
        Retorna o BDGD mais recente que contém a coordenada fornecida.

        Args:
            longitude (float): Longitude da coordenada
            latitude (float): Latitude da coordenada

        Returns:
            dict: Dados do BDGD mais recente ou None se nenhum for encontrado
        """
        with engine.begin() as conn:
            point = WKTElement(f"POINT({longitude} {latitude})", srid=4326)

            stmt = (
                region_table.select()
                .where(ST_Contains(region_table.c.geometry, point))
                .order_by(region_table.c.year_ref.desc())
                .limit(1)
            )

            result = conn.execute(stmt).fetchone()

            if result:
                return {
                    "id": result.id,
                    "bdgd_name": result.bdgd_name,
                    "bdgd_id": result.bdgd_id,
                    "cod_id": result.cod_id,
                    "dist": result.dist,
                    "year_ref": result.year_ref
                }
            return None

    def get_all_bdgds_for_coordinate(longitude: float, latitude: float):
        """
        Retorna todos os BDGDs que contêm a coordenada fornecida, ordenados por data mais recente.

        Args:
            longitude (float): Longitude da coordenada
            latitude (float): Latitude da coordenada

        Returns:
            list: Lista de dicionários com dados dos BDGDs encontrados
        """
        with engine.begin() as conn:
            point = WKTElement(f"POINT({longitude} {latitude})", srid=4326)

            stmt = (
                region_table.select()
                .where(ST_Contains(region_table.c.geometry, point))
                .order_by(region_table.c.year_ref.desc())
            )

            results = conn.execute(stmt).fetchall()

            return [
                {
                    "id": row.id,
                    "bdgd_name": row.bdgd_name,
                    "bdgd_id": row.bdgd_id,
                    "cod_id": row.cod_id,
                    "dist": row.dist,
                    "year_ref": row.year_ref
                }
                for row in results
            ]
    
if __name__ == '__main__':
    # # Criar tabelas de pesquisa
    # ensure_table_exists()

    # # Buscar BDGDs disponíveis nos dados abertos da ANEEL
    # bdgd_df = get_bdgd_list_df()
    
    # # Baixar e instanciar camadas de pesquisa de cada BDGD
    # for _, row in tqdm(bdgd_df.iterrows(), total=len(bdgd_df), desc='Extracting regions') if VERBOSE else bdgd_df.iterrows():
    #     bdgd_name = row['title']
    #     bdgd_id = row['id']
        
    #     if region_already_exists(bdgd_id):
    #         continue
        
    #     for table, gdf in download_search_layers(bdgd_name, bdgd_id).items():
    #         save_gdf_to_db(table, gdf)
    load_env_if_exists()