from typing import Any, Literal, Sequence
from geoalchemy2 import Geometry, WKTElement
from geoalchemy2.functions import ST_Contains, ST_Area, ST_AsText
from geopandas import GeoDataFrame
from pandas import DataFrame
from sqlalchemy import BinaryExpression, Column, ColumnElement, DateTime, Engine, Float, ForeignKeyConstraint, Integer, MetaData, Row, RowMapping, Select, String, Table, UniqueConstraint, create_engine, func, inspect, select, text
from sqlalchemy.schema import CreateSchema
from sqlalchemy.exc import IntegrityError

from utils import make_url_by_environment

class BDGDDBInterface:
    def __init__(self, engine: Engine | None = None):
        self.engine = engine if engine is not None else self.create_engine()
        self.setup_bdgd_db()

    def create_engine(self):
        url = make_url_by_environment('bdgd')
        return create_engine(url)

    def setup_bdgd_db(self):
        """
        Método para instanciar propriedades do gerenciador BDGD referentes ao banco de dados (metadata, tables)
        """

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
            UniqueConstraint('cod_id', 'substation', 'dist'),
            ForeignKeyConstraint(['substation', 'dist'], ['search.substation.cod_id', 'search.substation.dist']),
            schema = 'search'
        )

        with self.engine.begin() as conn:
            conn.execute(text('CREATE EXTENSION IF NOT EXISTS postgis;'))
            inspector = inspect(conn)
            if 'search' not in inspector.get_schema_names():
                conn.execute(CreateSchema('search'))
            self.metadata.create_all(conn)

    # Insert
    def save_search_gdf_to_db(self, layer_name: str, gdf: GeoDataFrame):
        layer_table = self.metadata.tables[f'search.{layer_name}']
        for _, row in gdf.iterrows():
            if 'geometry' in row:
                values = row.drop('geometry').to_dict()
                values['geometry'] = WKTElement(row.geometry.wkt, srid=4326)
            else:
                values = row.to_dict()
            try:
                with self.engine.begin() as conn:
                    conn.execute(layer_table.insert().values(values))
            except IntegrityError as e:
                if 'ForeignKeyViolation' in e.args[0]:
                    pass

    def save_generic_gdf_to_db(self, gdf: GeoDataFrame | DataFrame, layer_name: str, schema_name: str, first: bool):
        to_db = gdf.to_sql
        if isinstance(gdf, GeoDataFrame):
            to_db = gdf.to_postgis
            
        with self.engine.begin() as conn:
            to_db(
                name=layer_name,
                con=conn,
                schema=schema_name,
                if_exists='replace' if first else 'append'
            )

    # Insert caso não exista, das layers responsáveis por pesquisas
    def save_bdgd_search_layers_to_db(self, bdgd_search_gdfs: dict[Literal['region', 'substation', 'trhv'], GeoDataFrame]):
        for layer_name, gdf in bdgd_search_gdfs.items():
            self.save_search_gdf_to_db(layer_name, gdf)

    # Criação do schema para a região de interesse, caso não exista
    def create_bdgd_schema(self, region_of_interest: Row[Any]):
        """
        Cria um schema com o nome da concessionária/permissionária em questão, tal qual armazenará todas as camadas de interesse.
        """
        schema_name = f'{region_of_interest.bdgd_name}_{region_of_interest.bdgd_date.year}' # type: ignore
        with self.engine.begin() as conn:
            inspector = inspect(conn)
            if schema_name in inspector.get_schema_names():
                return
            conn.execute(CreateSchema(schema_name))

    # Pesquisa regions
    def region_exists(self, bdgd_id: str) -> bool:
        with self.engine.begin() as conn:
            stmt = (
                select(self.region_table)
                .where(self.region_table.c.bdgd_id == bdgd_id)
                .limit(1)
            )
            if conn.execute(stmt).first() is not None:
                return True
            return False
        
    def get_count_region(self) -> int:
        with self.engine.begin() as conn:
            stmt = select(func.count()).select_from(self.region_table)
            count = conn.execute(stmt).scalar()
            return count if count is not None else 0
        
    def get_all_region(self, limit: int | None = None, offset: int | None = None, mapped: bool = True) -> Sequence[RowMapping]:
        return self._execute_select_many_regions_stmt(mapped=mapped) # type: ignore
        with self.engine.begin() as conn:
            stmt = (
                select(self.region_table)
                .offset(offset)
                .limit(limit)
            )
            return conn.execute(stmt).mappings().all()
        
    def get_region_by_id(self, id: str, mapped: bool = True) -> RowMapping | None:
        whereclause = self.region_table.c.bdgd_id == id
        return self._execute_select_region_stmt(whereclause, mapped=mapped) # type: ignore
        
    def get_region_by_bdgd_id(self, bdgd_id: str, mapped: bool = True) -> RowMapping | None:
        whereclause = self.region_table.c.bdgd_id == bdgd_id
        return self._execute_select_region_stmt(whereclause, mapped=mapped) # type: ignore

        
    def get_region_by_cod_id(self, cod_id: str, mapped: bool = True) -> RowMapping | None:
        whereclause = self.region_table.c.bdgd_id == cod_id
        return self._execute_select_region_stmt(whereclause, mapped=mapped) # type: ignore

        
    def get_region_by_dist(self, dist: str | int, mapped: bool = True) -> RowMapping | None:
        whereclause = self.region_table.c.bdgd_id == dist
        return self._execute_select_region_stmt(whereclause, mapped=mapped) # type: ignore

        
    def get_region_by_bdgd_name(self, bdgd_name: str, mapped: bool = True) -> RowMapping | None:
        whereclause = self.region_table.c.bdgd_name.like(bdgd_name)
        return self._execute_select_region_stmt(whereclause, mapped=mapped) # type: ignore

    
    def get_region_by_poi(self, poi: tuple[float, float], mapped: bool = True) -> RowMapping | None:
        poi_wkt_element = WKTElement(f'POINT({poi[0]} {poi[1]})', srid=4326)
        whereclause = ST_Contains(self.region_table.c.geometry, poi_wkt_element)
        # region_of_interest_stmt = (self.region_table.select()
        #     .where(ST_Contains(self.region_table.c.geometry, poi_wkt_element))
        #     .order_by(ST_Area(self.region_table.c.geometry))
        # )
        # with self.engine.begin() as conn:
        #     return conn.execute(region_of_interest_stmt).mappings().first()
        return self._execute_select_region_stmt(whereclause, mapped=mapped) # type: ignore
    
    def _execute_select_region_stmt(self, whereclause: BinaryExpression[bool] | ColumnElement[bool], mapped: bool = True) -> RowMapping | Row[Any] | None:
        """
        Recebe uma expressão booleana como filtro, monta a query de seleção de regiões, a executa, e retorna o resultado da query.
        """
        region_cols_without_geom = [c for c in self.region_table.c if c.name != 'geometry']
        with self.engine.begin() as conn:
            stmt = (
                select(*region_cols_without_geom,
                   ST_AsText(self.region_table.c.geometry).label('geometry'))
                .where(whereclause)
                .limit(1)
            )
            if mapped:
                return conn.execute(stmt).mappings().first()
            return conn.execute(stmt).first()
    
    def _execute_select_many_regions_stmt(
            self,
            whereclause: BinaryExpression[bool] | ColumnElement[bool] | None = None,
            limit: int | None = None,
            offset: int | None = None,
            mapped: bool = True
        ) -> Sequence[RowMapping] | Sequence[Row[Any]]:
        region_cols_without_geom = [c for c in self.region_table.c if c.name != 'geometry']
        with self.engine.begin() as conn:
            if whereclause is not None:
                stmt = (
                    select(*region_cols_without_geom,
                    ST_AsText(self.region_table.c.geometry).label('geometry'))
                    .where(whereclause)
                    .offset(offset)
                    .limit(limit)
                )
            else:
                stmt = (
                    select(*region_cols_without_geom,
                    ST_AsText(self.region_table.c.geometry).label('geometry'))
                    .offset(offset)
                    .limit(limit)
                )

            if mapped:
                return conn.execute(stmt).mappings().all()
            return conn.execute(stmt).all()