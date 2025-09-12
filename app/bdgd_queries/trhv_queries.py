from typing import Any, Sequence
from sqlalchemy import Engine, Table
from sqlalchemy.engine import Row, RowMapping

class TrhvQueryMixin:
    """
    Requer:
      self.engine
      self.trhv_table
      Métodos do GenericQueryMixin.
    """
    engine: Engine
    trhv_table: Table

    def get_all_trhvs(
        self,
        limit: int | None = None,
        offset: int | None = None,
        mapped: bool = True,
        geometry: bool = True
    ) -> Sequence[RowMapping] | Sequence[Row[Any]]:
        return self._select_many(self.trhv_table, None, limit, offset, mapped, geometry)  # type: ignore

    def get_trhv_by_id(self, id: int, mapped: bool = True, geometry: bool = True):
        where = self.trhv_table.c.id == id
        return self._select_one(self.trhv_table, where, mapped, geometry)  # type: ignore

    def get_trhv_by_cod_id(self, cod_id: str, mapped: bool = True, geometry: bool = True):
        where = self.trhv_table.c.cod_id == cod_id
        return self._select_one(self.trhv_table, where, mapped, geometry)  # type: ignore

    def get_trhv_by_name(self, name: str, mapped: bool = True, geometry: bool = True):
        where = self.trhv_table.c.name.like(name)
        return self._select_one(self.trhv_table, where, mapped, geometry)  # type: ignore

    def get_trhvs_by_dist(self, dist: str, mapped: bool = True, geometry: bool = True):
        return self._select_many(self.trhv_table, self.trhv_table.c.dist == dist, None, None, mapped, geometry)  # type: ignore