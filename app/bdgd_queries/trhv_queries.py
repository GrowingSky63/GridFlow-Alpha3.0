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

    def get_trhvs_by_substation(
        self,
        substation_cod_id: str,
        dist: str,
        mapped: bool = True,
        geometry: bool = True
    ) -> Sequence[RowMapping] | Sequence[Row[Any]]:
        """
        Retorna todos os transformadores (UNTRAT) de uma subestação específica.
        """
        where = (
            (self.trhv_table.c.substation == substation_cod_id) &
            (self.trhv_table.c.dist == dist)
        )
        return self._select_many(self.trhv_table, where, None, None, mapped, geometry)  # type: ignore

    def get_trhvs_grouped_by_substations(
        self,
        pairs: list[tuple[str, str]],
        geometry: bool = True
    ) -> dict[tuple[str, str], list[dict]]:
        """
        Recupera transformadores de várias subestações em UMA consulta e agrupa.
        pairs: lista de (substation_cod_id, dist)
        Retorna: { (cod_id, dist): [ {transformador...}, ... ] }
        """
        if not pairs:
            return {}

        from sqlalchemy import select, tuple_
        from sqlalchemy.dialects.postgresql import JSONB
        from geoalchemy2.functions import ST_AsGeoJSON
        rows_expr = [
            self.trhv_table.c.substation,
            self.trhv_table.c.dist,
            self.trhv_table.c.id,
            self.trhv_table.c.cod_id,
            self.trhv_table.c.power
        ]
        if geometry:
            geom = ST_AsGeoJSON(self.trhv_table.c.geometry).cast(JSONB).label('geometry')
            rows_expr.append(geom)

        stmt = (
            select(*rows_expr)
            .where(
                tuple_(self.trhv_table.c.substation, self.trhv_table.c.dist)
                .in_(pairs)
            )
        )

        grouped: dict[tuple[str, str], list[dict]] = {}
        with self.engine.begin() as conn:  # type: ignore
            for r in conn.execute(stmt).mappings():
                key = (r['substation'], r['dist'])
                grouped.setdefault(key, []).append(dict(r))
        return grouped