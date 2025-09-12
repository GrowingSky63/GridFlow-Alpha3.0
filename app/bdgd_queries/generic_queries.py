from typing import Sequence, Any
from geoalchemy2 import WKTElement
from sqlalchemy import BinaryExpression, Engine, Table, cast
from sqlalchemy.engine import Row, RowMapping
from sqlalchemy.sql import ColumnElement, select
from geoalchemy2.functions import ST_AsText, ST_Distance, ST_Centroid
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy import func

class GenericQueryMixin:
    """
    Métodos genéricos reutilizados pelos Mixins específicos.
    Requer:
      self.engine
    """
    engine: Engine
    
    def _select_one(
        self,
        table: Table,
        whereclause: BinaryExpression[bool] | ColumnElement[bool],
        mapped: bool = True,
        geometry: bool = True
    ) -> RowMapping | Row[Any] | None:
        cols_wo_geom = [c for c in table.c if c.name != 'geometry']
        if geometry:
            geom_col = table.c.geometry
            if mapped:
                geom_col = cast(geom_col, JSONB)
            stmt = select(*cols_wo_geom, geom_col)
        else:
            stmt = select(*cols_wo_geom)
        stmt = stmt.where(whereclause).limit(1)
        with self.engine.begin() as conn:
            if mapped:
                return conn.execute(stmt).mappings().first()
            return conn.execute(stmt).first()

    def _select_many(
        self,
        table: Table,
        whereclause: BinaryExpression[bool] | ColumnElement[bool] | None = None,
        limit: int = 20,
        offset: int = 1,
        mapped: bool = True,
        geometry: bool = True
    ) -> Sequence[RowMapping] | Sequence[Row[Any]]:
        cols_wo_geom = [c for c in table.c if c.name != 'geometry']
        if geometry:
            geom_col = table.c.geometry
            if mapped:
                geom_col = cast(geom_col, JSONB)
            stmt = select(*cols_wo_geom, geom_col)
        else:
            stmt = select(*cols_wo_geom)
        if whereclause is not None:
            stmt = stmt.where(whereclause)
        if offset is not None:
            stmt = stmt.offset(offset)
        if limit is not None:
            stmt = stmt.limit(limit)
        with self.engine.begin() as conn:
            if mapped:
                return conn.execute(stmt).mappings().all()
            return conn.execute(stmt).all()

    def _select_one_by_poi_nearest(
            self,
            table: Table,
            poi: tuple[float, float],
            mapped: bool = True,
            geometry: bool = True
            ):
        lon, lat = poi
        point = WKTElement(f'POINT({lon} {lat})', srid=4326)
        cols_wo_geom = [c for c in table.c if c.name != 'geometry']
        if geometry:
            geom_col = table.c.geometry
            if mapped:
                geom_col = cast(geom_col, JSONB)
            stmt = select(
                *cols_wo_geom,
                geom_col,
                ST_Distance(ST_Centroid(table.c.geometry), point).label('distance')
            )
        else:
            stmt = select(
                *cols_wo_geom,
                ST_Distance(ST_Centroid(table.c.geometry), point).label('distance')
            )
        stmt = stmt.order_by('distance').limit(1)
        with self.engine.begin() as conn:
            if mapped:
                return conn.execute(stmt).mappings().first()
            return conn.execute(stmt).first()

    def _count_rows(self, table: Table) -> int:
        stmt = select(func.count()).select_from(table)
        with self.engine.begin() as conn:
            val = conn.execute(stmt).scalar()
            return val or 0