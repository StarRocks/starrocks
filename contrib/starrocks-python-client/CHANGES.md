Version history
===============

**Unreleased**

- Autogenerate no longer resets a table or materialized view property the model omits back to a hardcoded default; it is left as the database has it. Many defaults depend on the StarRocks version or FE config (`replication_num`, `fast_schema_evolution`, `bucket_size`), so the reset was often spurious. To manage a property, declare it in the model (#80738 by @chris-celerdata)
- Stop phantom property ALTERs when StarRocks does not report a value the model sets: add defaults for properties only reported when non-default (`write_quorum`, `compaction_strategy`, `enable_load_profile`, `mutable_bucket_num`, `primary_index_cache_expire_sec`, `storage_type`), compare enum and boolean values case-insensitively, and warn instead of generating an ALTER for a property the table cannot hold (`storage_format`, `bucket_size` on a non-RANDOM table, `enable_persistent_index` on a non-PRIMARY KEY table) (#80738 by @chris-celerdata)
- Stop phantom ALTERs for materialized view properties `query_rewrite_consistency`, `force_external_table_query_rewrite` and `refresh_mode`: `information_schema.tables_config` omits them, so they are now reflected from the MV DDL, and their values (and `partition_refresh_strategy`'s) are compared case-insensitively (#80738 by @chris-celerdata)

**1.3.5**

- Stop autogenerate from resetting an unmanaged `bucket_size` on RANDOM-distributed tables to a hardcoded 4 GiB default; StarRocks 4.0+ defaults to 1 GiB (#80015 by @chris-celerdata)
- Reflect `TIME` and `VARIANT` columns instead of returning `NullType` (#77259 by @rad-pat)
- Reflect `colocate_with` for materialized views from the MV DDL (#77619 by @chris-celerdata)
- Detect `run_mode` via the privilege-free `@@run_mode` variable, falling back to `ADMIN SHOW FRONTEND CONFIG` on older servers (#79873 by @chris-celerdata)
- Support the `ON_CHANGE` refresh keyword that StarRocks 26.2 introduced for
  base-table-change-triggered materialized views, which rejects the bare `ASYNC` it replaces.
  Reflection accepts either spelling and keeps reporting `ASYNC`, Alembic autogenerate treats
  them as the same refresh mode, and DDL is emitted with the keyword the connected server
  accepts, so one metadata definition works against every supported release. The scheduled
  mode (`ASYNC EVERY(...)`) is unchanged. (#81635)
- Add the `starrocks_temp_view_schema` Alembic option (`context.configure(...)`) to designate
  the schema in which the transient view used to canonicalize view/MV definitions is created.
  Lets a locked-down migration user be granted the required privileges on a single schema
  (e.g. the same one as `version_table_schema`) instead of on every schema that holds a view.
  (#77276 by @chris-celerdata)
- Coalesce multiple `ADD`/`DROP COLUMN` autogenerate operations on a table into a single `ALTER TABLE` statement via the `combine_column_alters` rewriter, avoiding StarRocks in-progress schema-change failures (#78647 by @chris-celerdata)
- Add opt-in `starrocks_wait_for_schema_change` to block until a column schema-change job reaches a terminal state (#78647 by @chris-celerdata)
- Support RANGE distribution (StarRocks 4.1+, opt-in via `enable_range_distribution` on 4.1; the shared-data default from #76543): reflect it as `RANGE`, accept `starrocks_distributed_by="RANGE"` (compiled by omitting `DISTRIBUTED BY`), and stop autogenerate from raising on RANGE-distributed tables whose metadata leaves `DISTRIBUTED BY` unset (#80014 by @chris-celerdata)

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