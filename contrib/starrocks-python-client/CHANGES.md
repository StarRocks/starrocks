Version history
===============

**Unreleased**

- Add the `starrocks_temp_view_schema` Alembic option (`context.configure(...)`) to designate
  the schema in which the transient view used to canonicalize view/MV definitions is created.
  Lets a locked-down migration user be granted the required privileges on a single schema
  (e.g. the same one as `version_table_schema`) instead of on every schema that holds a view.
- Coalesce multiple `ADD`/`DROP COLUMN` autogenerate operations on a table into a single `ALTER TABLE` statement via the `combine_column_alters` rewriter, avoiding StarRocks in-progress schema-change failures (#XXXXX by @chris-celerdata)
- Add opt-in `starrocks_wait_for_schema_change` to block until a column schema-change job reaches a terminal state (#XXXXX by @chris-celerdata)

**1.3.4**

- Add `to_diff_tuple` for Alembic alter operations (#70146 by @arvindKandpal-ksolves)
- Fix parsing of reflected nested `STRUCT` / `ARRAY` / `MAP` column types when inline field comments are present (#69817)
- Deserialize complex types to matching Python list, dict types (#70480 by @chris-celerdata)
- Add `__hash__` to reflected dataclasses to fix unhashable type errors (#70734 by @aholowko)
- Fix spurious Alembic regeneration for syntax and property changes using SQLGlot (#75984 by @chris-celerdata)
- Add `SCHEDULE` keyword to the Python parser (#76762 by @chris-celerdata)

**1.3.3**

- Add back support for SQLAlchemy 1.4 (#65976 by @rad-pat)
- Enable select from a FilesSource and support for Python 3.14 (#66797 by @rad-pat)
- Add support for async SQLAlchemy connection via asyncmy driver and
  ignore errors from querying tables_config on non-default catalog (#67479 by @rad-pat)

**1.3.2**
- Fix DEFAULT compilation, type comparison, and column ordering (#66125 by @jaogoy)

**1.3.1**
- Enable SQLAlchemy Test Suite (#65025 by @rad-pat)
- Supports Views and Materialized Views and enhance documentation (#65808 by @jaogoy)

**1.3.0**
- Support more attributes for comparing StarRocks tables by using Alembic (#64161 by jaogoy)

**1.2.3**
Older