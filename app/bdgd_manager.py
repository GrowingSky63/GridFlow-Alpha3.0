from typing import Any
from aneel_downloader import BDGDDownloader, BDGDListDownloader
from sqlalchemy import Column, DateTime, Float, ForeignKeyConstraint, Integer, Row, String, UniqueConstraint, create_engine, URL, Table, MetaData, inspect, select, func, cast
from sqlalchemy.schema import CreateSchema
from sqlalchemy.exc import IntegrityError
from geoalchemy2 import Geometry, Geography
from geoalchemy2.functions import ST_Contains, ST_Intersection
from geoalchemy2.elements import WKTElement
import geopandas as gpd
import pandas as pd
from tqdm import tqdm
from datetime import datetime
from tempfile import TemporaryDirectory
import os, math

VERBOSE = True

def load_env_if_exists():
    """
    Try to import, find and load a .env file to the env variables.
    """
    try:
        import dotenv
        dotenv_path = dotenv.find_dotenv()
        if dotenv_path:
            dotenv.load_dotenv(dotenv_path)
    except ModuleNotFoundError:
        pass

def make_url_by_environment(dbname='postgres') -> URL:
    """
    Create sqlalchemy.URL using environment or default variables.
    """
    load_env_if_exists()
    return URL.create(
        drivername=os.getenv('DB_DRIVER_NAME', 'postgresql'),
        host=os.getenv('DB_HOST', '127.0.0.1'),
        port=int(os.getenv('DB_PORT', '5432')),
        username=os.getenv('DB_USERNAME', 'postgres'),
        password=os.getenv('DB_PASSWORD', 'postgres'),
        database=os.getenv('DB_BDGD_NAME', dbname),
)

class BDGDManager:
    """
    Main class to ensure and manage BDGDs. Instantiation will ensure that each most recent search
    needed BDGDs layers in the ANEEL online list is in the database. After instantiation, this class
    permits to download all needed layers of especific BDGDs, using the search layer ones to find the
    BDGD of interest.
    """
    def __init__(self):
        poi = -49.7239321, -25.5576706
        # poi_sulgipe = -37.65667799755352, -11.379702596727167
        load_env_if_exists()
        self.setup_bdgd_db()
        self.ensure_table_exists()
        self.bdgd_list_df = self.get_bdgd_list_df()
        self.download_and_save_all_bdgd(2024)
        self.layers_of_interest = [
            'CTMT', 'SEGCON', 'UNTRMT',
            'RAMLIG', 'SSDMT', 'SSDBT',
            'UCAT_tab', 'UCMT_tab', 'UCBT_tab',
            'UGAT_tab', 'UGMT_tab', 'UGBT_tab'
        ]
        self.donwload_and_save_in_cache_bdgd_by_poi(poi)


    # Banco de dados PostGIS
    def setup_bdgd_db(self):
        """
        Método para instanciar propriedades do gerenciador BDGD referentes ao banco de dados (engine, metadata, tables)
        """
        url = make_url_by_environment('bdgd')

        self.engine = create_engine(url, echo=False)

        self.metadata = MetaData()

        self.region_table = Table(
            'region',
            self.metadata,
            Column('id', Integer, primary_key=True, autoincrement=True),
            Column('geometry', Geometry('MULTIPOLYGON', srid=4326)),
            Column('bdgd_id', String, unique=True),
            Column('cod_id', String, unique=True),
            Column('bdgd_full_name', String, unique=True),
            Column('bdgd_name', String),
            Column('bdgd_date', DateTime),
            Column('dist', String),
            UniqueConstraint('bdgd_name', 'bdgd_date'),
            schema = 'search'
        )

        self.substation_table = Table(
            'substation',
            self.metadata,
            Column('id', Integer, primary_key=True, autoincrement=True),
            Column('geometry', Geometry('MULTIPOLYGON', srid=4326)),
            Column('cod_id', String),
            Column('dist', String),
            Column('name', String),
            UniqueConstraint('cod_id', 'dist'),
            schema = 'search'
        )

        self.trhv_table = Table(
            'trhv',
            self.metadata,
            Column('id', Integer, primary_key=True, autoincrement=True),
            Column('geometry', Geometry('POINT', srid=4326)),
            Column('cod_id', String),
            Column('substation', String),
            Column('dist', String),
            Column('power', Float),
            UniqueConstraint('cod_id', 'substation'),
            ForeignKeyConstraint(['substation', 'dist'], ['substation.cod_id', 'substation.dist']),
            schema = 'search'
        )

    def ensure_table_exists(self):
        """
        Cria as tabelas de pesquisa
        """
        with self.engine.begin() as conn:
            self.metadata.create_all(conn)

    # Download do catálogo de bdgds nos dados abertos da ANEEL
    def get_bdgd_list_df(self):
        """
        Método para download e normalização da lista de arquivos disponíveis nos dados abertos da ANEEL
        """
        with TemporaryDirectory(prefix='gridflow-bdgd-list-') as temp_folder:
            with BDGDListDownloader(temp_folder) as f:
                return self.normalize_df_bdgd_list(pd.read_csv(f))

    def normalize_df_bdgd_list(self, raw_df: pd.DataFrame) -> pd.DataFrame:
        """
        Método para normalização do df responsável por guardar o índice de arquivos disponíveis nos dados abertos da ANEEL
        """
        df = raw_df[['id', 'title', 'type', 'tags']]
        df = df[df['type'] == 'File Geodatabase'].copy()
        df['tags'] = df['tags'].apply(lambda i: i.split(','))
        required_tags = {'BDGD', 'SIG-R', 'Distribuicao'}
        mask_has_required = df['tags'].apply(lambda tags: required_tags.issubset(set(tags)))
        df = df[mask_has_required].copy()
        df = df[df['tags'].apply(lambda tags: ' - '.join(tags[-2:])) == df['title']].copy()
        df['bdgd_date'] = df['tags'].apply(lambda tags: datetime(*(int(i) for i in tags[-1].split('-')))) # type: ignore
        df['bdgd_name'] = df['tags'].apply(lambda tags: tags[-2])
        df['tags'] = df['tags'].apply(lambda tags: tags[:-2])
        df = df.sort_values(by='bdgd_date', ascending=False).reset_index(drop=True)

        return df

    # Download dos bdgds nos dados abertos da ANEEL
    def donwload_and_save_bdgd(self, bdgd_full_name: str, bdgd_id: str):
        with self.engine.begin() as conn:
            exists_stmt = (
                select(1)
                .select_from(self.region_table)
                .where(self.region_table.c.bdgd_id == bdgd_id)
                .limit(1)
            )
            if conn.execute(exists_stmt).first() is not None:
                return False

        search_gdfs = self.get_all_search_layers_to_gdf(bdgd_full_name, bdgd_id)

        if search_gdfs:
            for table, gdf in search_gdfs.items():
                self.save_gdf_to_db(table, gdf)
            return True
        return False

    def download_and_save_all_bdgd(self, year: int):
        filtered_by_year = self.bdgd_list_df.loc[self.bdgd_list_df['bdgd_date'].dt.year == year]
        for _, row in tqdm(filtered_by_year.iterrows(), total=len(filtered_by_year)):
            self.donwload_and_save_bdgd(row['title'], row['id'])

    def download_and_save_each_most_recent_bdgd(self):
        """
        Method to download and save only the most recent search layers of each bdgd.
        """
        # Depreciado. Preciso arrumar este método
        covered_territory = self.get_covered_territory()
        progress = covered_territory*100
        with tqdm(total=100, initial=progress, unit='%', desc='Área reconhecida', ) as pbar:
            def update_pbar():
                covered_territory = self.get_covered_territory()
                progress = covered_territory*100
                pbar.update(progress - previous_progress)
            for _, row in self.bdgd_list_df.iterrows():
                previous_progress = progress
                if covered_territory >= 100:
                    return
                bdgd_full_name = row['title']
                bdgd_id = row['id']
                
                with self.engine.begin() as conn:
                    exists_stmt = (
                        select(1)
                        .select_from(self.region_table)
                        .where(self.region_table.c.bdgd_id == bdgd_id)
                        .limit(1)
                    )
                    if conn.execute(exists_stmt).first() is not None:
                        continue

                search_gdfs = self.get_all_search_layers_to_gdf(bdgd_full_name, bdgd_id)

                if search_gdfs:
                    for table, gdf in search_gdfs.items():
                        self.save_gdf_to_db(table, gdf)
                update_pbar()

    def donwload_and_save_in_cache_bdgd_by_poi(self, poi: tuple[float, float]):
        region_of_interest = self.get_region_by_poi(poi)
        if not region_of_interest:
            return
        
        schema_name = f'{region_of_interest.bdgd_name}_{region_of_interest.bdgd_date.year}'
        with self.engine.begin() as conn:
            inspector = inspect(conn)
            if schema_name in inspector.get_schema_names():
                return
            conn.execute(CreateSchema(schema_name))

        with TemporaryDirectory(prefix='gridflow-bdgd-') as temp_folder:
            with BDGDDownloader(region_of_interest.bdgd_id, region_of_interest.bdgd_full_name, temp_folder, True, VERBOSE) as bdgd_file:
                layers = self.layers_of_interest
                if VERBOSE:
                    layers = tqdm(layers)
                first = True
                for layer in layers:
                    layer_gdf = gpd.read_file(bdgd_file, layer=layer)
                    chunk_size = 10000
                    chunk_iterator = range(0, len(layer_gdf), chunk_size)
                    if VERBOSE:
                        chunk_iterator = tqdm(chunk_iterator)
                    with self.engine.connect() as conn:
                        for start in chunk_iterator:
                            end = start + chunk_size
                            chunk = layer_gdf.iloc[start:end]

                            to_db = chunk.to_sql
                            if isinstance(layer_gdf, gpd.GeoDataFrame) and 'geometry' in layer_gdf.columns:
                                to_db = chunk.to_postgis
                            to_db(
                                name=layer,
                                con=conn,
                                schema=schema_name,
                                if_exists='replace' if first else 'append'
                            )
                            conn.commit()
                            first = False

    # Leitura dos GDBs usando GeoDataFrames
    def get_all_search_layers_to_gdf(self, bdgd_full_name: str, bdgd_id: str) -> dict[Table, gpd.GeoDataFrame] | None:
        """
        Método para download, normalização e instanciação no DB das camadas necessárias para pesquisa dos BDGDs. Este método fará o download de todos os BDGDs e coletará as camadas de pesquisa de cada um (ARAT, SUB e UNTRAT).
        """
        with TemporaryDirectory(prefix='gridflow-bdgd-') as temp_folder:
            with BDGDDownloader(bdgd_id, bdgd_full_name, temp_folder, True, VERBOSE) as bdgd_file:
                region_gdf = self.normalize_gdf_region(gpd.read_file(bdgd_file, layer='ARAT'), bdgd_full_name, bdgd_id)
                # if 'RGE-397' in bdgd_full_name:
                #     pass
                if self.region_already_exists(region_gdf):
                    print(f'\033[31m{bdgd_full_name} already exists.')
                    return
                
                substation_gdf = self.normalize_gdf_substation(gpd.read_file(bdgd_file, layer='SUB'))

                trhv_gdf = self.normalize_gdf_trhv(gpd.read_file(bdgd_file, layer='UNTRAT'))

                return {
                    self.region_table: region_gdf,
                    self.substation_table: substation_gdf,
                    self.trhv_table: trhv_gdf
                }

    def save_gdf_to_db(self, layer_table: Table, gdf: gpd.GeoDataFrame):
        for _, row in gdf.iterrows():
            try:
                if 'geometry' in row:
                    values = row.drop('geometry').to_dict()
                    values['geometry'] = WKTElement(row.geometry.wkt, srid=4326)
                else:
                    values = row.to_dict()
                with self.engine.begin() as conn:
                    conn.execute(layer_table.insert().values(values))
            except IntegrityError:
                continue

    def normalize_gdf_region(self, raw_gdf: gpd.GeoDataFrame, bdgd_full_name: str, bdgd_id: str) -> gpd.GeoDataFrame:
        """
        Método para normalizar o gdf da layer ARAT de um BDGD. Este gdf alimentará a tabela region do DB
        """
        region_gdf = raw_gdf.rename(columns={
            'COD_ID': 'cod_id',
            'DIST': 'dist'
        })
        region_gdf = region_gdf[['cod_id', 'dist', 'geometry']]

        region_gdf['bdgd_full_name'] = bdgd_full_name

        bdgd_name, bdgd_date = self.get_name_and_date_from_bdgd_full_name(bdgd_full_name)
        region_gdf['bdgd_name'] = bdgd_name
        region_gdf['bdgd_date'] = bdgd_date

        region_gdf['bdgd_id'] = bdgd_id

        return region_gdf

    def normalize_gdf_substation(self, raw_gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        """
        Método para normalizar o gdf da layer SUB de um BDGD. Este gdf alimentará a tabela substation do DB
        """
        substation_gdf = raw_gdf.rename(columns={
            'COD_ID': 'cod_id',
            'DIST': 'dist',
            'NOME': 'name'
        })
        return substation_gdf[['cod_id', 'dist', 'name', 'geometry']]

    def normalize_gdf_trhv(self, raw_gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        """
        Método para normalizar o gdf da layer UNTRAT de um BDGD. Este gdf alimentará a tabela trhv do DB
        """
        trhv_gdf = raw_gdf.rename(columns={
            'COD_ID': 'cod_id',
            'SUB': 'substation',
            'POT_NOM': 'power'
        })
        return trhv_gdf[['cod_id', 'substation', 'power', 'geometry']]

    # Controle de cobertura das áreas de atuação
    def get_covered_territory(self) -> float:
        # Depreciado. Preciso arrumar este método
        with self.engine.begin() as conn:
            brazil_area_m2 = float(os.getenv('BRAZIL_AREA_M2', '8515767000000'))  # ~8,515,767 km²

            union_geom = func.ST_ReorientGeometries(
                func.ST_UnaryUnion(
                    func.ST_Union(func.ST_MakeValid(self.region_table.c.geometry))
                )
            )

            total_area_stmt = select(
                func.ST_Area(cast(union_geom, Geography))
            )
            total_area = conn.execute(total_area_stmt).scalar()

            if not total_area or total_area <= 0 or not math.isfinite(total_area):
                return 0.0

            ratio = float(total_area) / brazil_area_m2
            return max(0.0, min(1.0, ratio))

    def region_already_exists(self, region_gdf: gpd.GeoDataFrame) -> bool:
        """
        Retorna True se:
        1) Já existir registro da MESMA concessionária (bdgd_name) com data >= à que está chegando; ou
        2) OUTRAS concessionárias com data > cobrirem pelo menos OVERLAP_THRESHOLD (default 0.9) da geometria.
        Caso contrário, False.
        """
        # Depreciado. Preciso arrumar este método
        bdgd_name = str(region_gdf['bdgd_name'].iloc[0])
        bdgd_date = pd.to_datetime(region_gdf['bdgd_date'].iloc[0]).to_pydatetime()

        incoming_union = region_gdf.geometry.union_all()

        if incoming_union is None or incoming_union.is_empty:
            return True

        incoming_geom = WKTElement(incoming_union.wkt, srid=4326)

        with self.engine.begin() as conn:
            same_name_count = conn.execute(
                select(func.count()).select_from(self.region_table).where(
                    (self.region_table.c.bdgd_name == bdgd_name) &
                    (self.region_table.c.bdgd_date >= bdgd_date)
                )
            ).scalar_one()

            if same_name_count and same_name_count > 0:
                return True

            overlap_threshold = float(os.getenv('OVERLAP_THRESHOLD', '0.9'))

            newer_union_subq = select(
                func.ST_Union(self.region_table.c.geometry).label('geom')
            ).where(
                (self.region_table.c.bdgd_date > bdgd_date) &
                (self.region_table.c.bdgd_name != bdgd_name)
            ).subquery()

            numerator = func.ST_Area(
                cast(ST_Intersection(newer_union_subq.c.geom, incoming_geom), Geography)
            )
            denominator = func.ST_Area(cast(incoming_geom, Geography))

            ratio_stmt = select(func.coalesce(numerator / func.nullif(denominator, 0), 0.0))
            ratio = float(conn.execute(ratio_stmt).scalar_one())

            return ratio >= overlap_threshold

    # Utils
    def get_name_and_date_from_bdgd_full_name(self, bdgd_full_name: str) -> tuple[str, datetime]:
        bdgd_name, bdgd_str_date = bdgd_full_name.split(' - ')
        year, month, day = [int(i) for i in bdgd_str_date.split('-')]
        return bdgd_name, datetime(year, month, day)

    def get_region_by_poi(self, poi: tuple[float, float]) -> Row[Any] | None:
        poi_wkt_element = WKTElement(f'POINT({poi[0]} {poi[1]})', srid=4326)
        region_of_interest_stmt = (self.region_table.select()
            .where(ST_Contains(self.region_table.c.geometry, poi_wkt_element))
            .limit(1)
        )

        with self.engine.begin() as conn:
            return conn.execute(region_of_interest_stmt).first()

if __name__ == '__main__':
    bdgd_manager = BDGDManager()