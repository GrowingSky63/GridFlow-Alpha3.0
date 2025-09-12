from typing import Any, Sequence
from sqlalchemy import Engine, Table
from sqlalchemy.engine import Row, RowMapping
from sqlalchemy.sql import select
from geoalchemy2 import WKTElement
from geoalchemy2.functions import ST_Contains

class RegionQueryMixin:
    """
    Requer atributos definidos na classe principal:
      self.engine
      self.region_table
      Métodos utilitários do GenericQueryMixin:
        _select_one
        _select_many
        _count_rows
    """
    engine: Engine
    region_table: Table

    # Existência
    def region_exists(self, bdgd_id: str) -> bool:
        with self.engine.begin() as conn:
            stmt = (
                select(self.region_table.c.id)
                .where(self.region_table.c.bdgd_id == bdgd_id)
                .limit(1)
            )
            return conn.execute(stmt).first() is not None

    def get_count_region(self) -> int:
        return self._count_rows(self.region_table)  # type: ignore

    # Listagem
    def get_all_regions(
        self,
        limit: int | None = None,
        offset: int | None = None,
        mapped: bool = True,
        geometry: bool = True
    ) -> Sequence[RowMapping] | Sequence[Row[Any]]:
        return self._select_many(self.region_table, None, limit, offset, mapped, geometry)  # type: ignore

    # Filtros
    def get_region_by_id(self, id: int, mapped: bool = True, geometry: bool = True):
        where = self.region_table.c.id == id
        return self._select_one(self.region_table, where, mapped, geometry)  # type: ignore

    def get_region_by_bdgd_id(self, bdgd_id: str, mapped: bool = True, geometry: bool = True):
        where = self.region_table.c.bdgd_id == bdgd_id
        return self._select_one(self.region_table, where, mapped, geometry)  # type: ignore

    def get_region_by_cod_id(self, cod_id: str, mapped: bool = True, geometry: bool = True):
        where = self.region_table.c.cod_id == cod_id
        return self._select_one(self.region_table, where, mapped, geometry)  # type: ignore

    def get_region_by_dist(self, dist: str | int, mapped: bool = True, geometry: bool = True):
        where = self.region_table.c.dist == str(dist)
        return self._select_one(self.region_table, where, mapped, geometry)  # type: ignore

    def get_region_by_bdgd_name(self, bdgd_name: str, mapped: bool = True, geometry: bool = True):
        where = self.region_table.c.bdgd_name.like(bdgd_name)
        return self._select_one(self.region_table, where, mapped, geometry)  # type: ignore

    def get_region_by_poi(self, poi: tuple[float, float], mapped: bool = True, geometry: bool = True):
        # POINT(lon lat)
        poi_wkt = WKTElement(f'POINT({poi[0]} {poi[1]})', srid=4326)
        where = ST_Contains(self.region_table.c.geometry, poi_wkt)
        return self._select_one(self.region_table, where, mapped, geometry)  # type: ignore