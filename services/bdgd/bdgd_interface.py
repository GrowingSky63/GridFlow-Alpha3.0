from typing import Literal
from geoalchemy2 import Geometry, WKTElement
from geopandas import GeoDataFrame
from pandas import DataFrame
from sqlalchemy import Column, DateTime, Engine, Float, ForeignKeyConstraint, Integer, MetaData, String, Table, UniqueConstraint, create_engine, inspect, text
from sqlalchemy.schema import CreateSchema
from sqlalchemy.exc import IntegrityError

from services.utils import make_url_by_environment

from .bdgd_queries import GenericQueryMixin, RegionQueryMixin, SubstationQueryMixin, TrhvQueryMixin

class _BDGDCore:
    """
    Responsável apenas por engine, metadata e definição de tabelas.
    """
    def __init__(self, engine: Engine | None = None):
        self.engine = engine if engine is not None else self._create_engine()
        self._setup_tables()

    def _create_engine(self):
        url = make_url_by_environment('bdgd')
        return create_engine(url)

    def _setup_tables(self):
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
            schema='search'
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
            schema='search'
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
            ForeignKeyConstraint(
                ['substation', 'dist'],
                ['search.substation.cod_id', 'search.substation.dist']
            ),
            schema='search'
        )

        with self.engine.begin() as conn:
            conn.execute(text('CREATE EXTENSION IF NOT EXISTS postgis;'))
            inspector = inspect(conn)
            if 'search' not in inspector.get_schema_names():
                conn.execute(CreateSchema('search'))
            self.metadata.create_all(conn)

class BDGDDBInterface(
    GenericQueryMixin,
    RegionQueryMixin,
    SubstationQueryMixin,
    TrhvQueryMixin,
    _BDGDCore
):
    """
    Interface pública. Métodos region_* e substation_* preservados via Mixins.
    """

    def remove_bdgd_search_layers_from_db(self, dist: str):
        """
        Remove as camadas de busca existentes para uma determinada distribuidora.
        Remove region, substation e trhv associados ao dist especificado.
        """
        with self.engine.begin() as conn:
            # Remove trhv primeiro (por causa da foreign key)
            conn.execute(
                self.trhv_table.delete().where(self.trhv_table.c.dist == dist)
            )
            # Remove substation
            conn.execute(
                self.substation_table.delete().where(self.substation_table.c.dist == dist)
            )
            # Remove region
            conn.execute(
                self.region_table.delete().where(self.region_table.c.dist == dist)
            )

    # Inserts originais mantidos
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

    def save_bdgd_search_layers_to_db(self, bdgd_search_gdfs: dict[Literal['region', 'substation', 'trhv'], GeoDataFrame]):
        for layer_name, gdf in bdgd_search_gdfs.items():
            self.save_search_gdf_to_db(layer_name, gdf)

    def create_bdgd_schema(self, region_of_interest):
        schema_name = f'{region_of_interest.bdgd_name}_{region_of_interest.bdgd_date.year}'  # type: ignore
        with self.engine.begin() as conn:
            inspector = inspect(conn)
            if schema_name in inspector.get_schema_names():
                return
            conn.execute(CreateSchema(schema_name))

if __name__ == "__main__":
    from sqlalchemy.sql import select, cast
    from geoalchemy2.functions import ST_AsGeoJSON
    from json import loads
    from sqlalchemy.dialects.postgresql import JSONB
    interface = BDGDDBInterface()
    with interface.engine.begin() as conn:
        stmt = select(cast(ST_AsGeoJSON(interface.region_table.c.geometry), JSONB)).where(interface.region_table.c.id == 1)
        result = conn.execute(stmt).scalar()
        pass