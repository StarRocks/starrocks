from alembic.ddl.mysql import MySQLImpl
from sqlalchemy import Column
from sqlalchemy import BIGINT
from sqlalchemy import MetaData
from sqlalchemy import Table
from sqlalchemy import VARCHAR
from typing import Optional


class StarrocksImpl(MySQLImpl):
    __dialect__ = "starrocks"

    def version_table_impl(
        self,
        *,
        version_table: str,
        version_table_schema: Optional[str],
        version_table_pk: bool, # ignored as StarRocks requires a primary key
        **kw,
    ) -> Table:
        return Table(
            version_table,
            MetaData(),
            Column("id", BIGINT, autoincrement=True, primary_key=True),
            Column("version_num", VARCHAR(32), primary_key=False),
            schema=version_table_schema,
            starrocks_primary_key="id",
        )

