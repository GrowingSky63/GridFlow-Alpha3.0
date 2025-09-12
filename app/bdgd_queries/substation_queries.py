from typing import Any, Sequence
from sqlalchemy import Engine, Table
from sqlalchemy.engine import Row, RowMapping
from sqlalchemy.sql import select
from geoalchemy2 import WKTElement
from geoalchemy2.functions import ST_Distance, ST_AsText, ST_Centroid

class SubstationQueryMixin:
    """
    Requer:
      self.engine
      self.substation_table
      Métodos do GenericQueryMixin.
    """
    engine: Engine
    substation_table: Table

    def get_all_substations(
        self,
        limit: int | None = None,
        offset: int | None = None,
        mapped: bool = True,
        geometry: bool = True
    ) -> Sequence[RowMapping] | Sequence[Row[Any]]:
        return self._select_many(self.substation_table, None, limit, offset, mapped, geometry)  # type: ignore

    def get_substation_by_id(self, id: int, mapped: bool = True, geometry: bool = True):
        where = self.substation_table.c.id == id
        return self._select_one(self.substation_table, where, mapped, geometry)  # type: ignore

    def get_substation_by_cod_id(self, cod_id: str, mapped: bool = True, geometry: bool = True):
        where = self.substation_table.c.cod_id == cod_id
        return self._select_one(self.substation_table, where, mapped, geometry)  # type: ignore

    def get_substation_by_name(self, name: str, mapped: bool = True, geometry: bool = True):
        where = self.substation_table.c.name.like(name)
        return self._select_one(self.substation_table, where, mapped, geometry)  # type: ignore

    def get_substations_by_dist(self, dist: str, mapped: bool = True, geometry: bool = True):
        return self._select_many(self.substation_table, self.substation_table.c.dist == dist, None, None, mapped, geometry)  # type: ignore

    def get_substation_by_poi(self, poi: tuple[float, float], mapped: bool = True, geometry: bool = True):
        """
        Retorna a subestação mais próxima do ponto (lon, lat) usando o centróide da geometria.
        """
        lon, lat = poi
        point = WKTElement(f'POINT({lon} {lat})', srid=4326)

        cols_wo_geom = [c for c in self.substation_table.c if c.name != 'geometry']
        stmt = (
            select(
                *cols_wo_geom,
                ST_AsText(self.substation_table.c.geometry).label('geometry'),
                ST_Distance(ST_Centroid(self.substation_table.c.geometry), point).label('distance')
            )
            .order_by('distance')
            .limit(1)
        )
        with self.engine.begin() as conn:
            if mapped:
                return conn.execute(stmt).mappings().first()
            return conn.execute(stmt).first()