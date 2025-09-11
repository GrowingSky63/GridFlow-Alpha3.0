from typing import Sequence, Any
from sqlalchemy import BinaryExpression, Engine, Select, Table
from sqlalchemy.engine import Row, RowMapping
from sqlalchemy.sql import ColumnElement, select
from geoalchemy2.functions import ST_AsText
from sqlalchemy import func

class GenericQueryMixin:
    """
    Métodos genéricos reutilizados pelos Mixins específicos.
    Requer:
      self.engine
    """
    engine: Engine
    
    def _select_one_with_geometry(
        self,
        table: Table,
        whereclause: BinaryExpression[bool] | ColumnElement[bool],
        mapped: bool = True
    ) -> RowMapping | Row[Any] | None:
        cols_wo_geom = [c for c in table.c if c.name != 'geometry']
        stmt: Select = (
            select(*cols_wo_geom, ST_AsText(table.c.geometry).label('geometry'))
            .where(whereclause)
            .limit(1)
        )
        with self.engine.begin() as conn:
            if mapped:
                return conn.execute(stmt).mappings().first()
            return conn.execute(stmt).first()

    def _select_many_with_geometry(
        self,
        table: Table,
        whereclause: BinaryExpression[bool] | ColumnElement[bool] | None = None,
        limit: int | None = None,
        offset: int | None = None,
        mapped: bool = True
    ) -> Sequence[RowMapping] | Sequence[Row[Any]]:
        cols_wo_geom = [c for c in table.c if c.name != 'geometry']
        base = select(*cols_wo_geom, ST_AsText(table.c.geometry).label('geometry'))
        if whereclause is not None:
            base = base.where(whereclause)
        if offset is not None:
            base = base.offset(offset)
        if limit is not None:
            base = base.limit(limit)
        with self.engine.begin() as conn:
            if mapped:
                return conn.execute(base).mappings().all()
            return conn.execute(base).all()

    def _count_rows(self, table: Table) -> int:
        stmt = select(func.count()).select_from(table)
        with self.engine.begin() as conn:
            val = conn.execute(stmt).scalar()
            return val or 0