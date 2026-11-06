Version history
===============

**Unreleased**

- Stop autogenerate from resetting an unmanaged `bucket_size` on RANDOM-distributed tables to a hardcoded 4 GiB default; StarRocks 4.0+ defaults to 1 GiB (by @chris-celerdata)
- Reflect `TIME` and `VARIANT` columns instead of returning `NullType` (#77259 by @rad-pat)
- Add the `starrocks_temp_view_schema` Alembic option (`context.configure(...)`) to designate
  the schema in which the transient view used to canonicalize view/MV definitions is created.
  Lets a locked-down migration user be granted the required privileges on a single schema
  (e.g. the same one as `version_table_schema`) instead of on every schema that holds a view.
- Coalesce multiple `ADD`/`DROP COLUMN` autogenerate operations on a table into a single `ALTER TABLE` statement via the `combine_column_alters` rewriter, avoiding StarRocks in-progress schema-change failures (#78647 by @chris-celerdata)
- Add opt-in `starrocks_wait_for_schema_change` to block until a column schema-change job reaches a terminal state (#78647 by @chris-celerdata)
- Fix the `FILES()` storage, unload and format options to write the properties StarRocks reads (by @rad-pat):
  - `AzureBlobStorage` takes `wasb://` and `wasbs://` paths instead of `azure://`, and adds `endpoint`, `storage_account` and `container`.
  - `AzureDataLakeStorage1` takes `adl://` paths and writes `azure.adls1.*` properties. Its `oauth2_client_credential` and `oauth2_client_endpoint` arguments are now `oauth2_credential` and `oauth2_endpoint`; the old names still work.
  - `AzureDataLakeStorage2` takes `abfs://` and `abfss://` paths, writes `azure.adls2.oauth2_use_managed_identity` instead of `oauth2_managed_identity`, and adds `sas_token`, `endpoint` and `oauth2_token_file`.
  - A path with the wrong scheme raises `ValueError`. The Data Lake classes no longer take `wasb://` paths; use `AzureBlobStorage` for those.
  - `FilesTargetOptions` writes `target_max_file_size` and `partition_by` instead of `target_max_files_size` and `partitioned_by`, which StarRocks ignored. The old argument names still work, so they now take effect: an unload with `partitioned_by` is partitioned, and fails where StarRocks refuses the partitioning, as with `single=True`.
  - `CSVFormat(line_delimiter=...)` writes `csv.row_delimiter`, which StarRocks reads for unloading as well as loading, instead of `csv.line_delimiter`, which it ignored; the row delimiter now takes effect. `CSVFormat` gains `include_header` for unloading.
  - Every property value is written as a string, a bool as `true` or `false`: `trim_space=True` and `use_legacy_encoding=True` used to be a bare `True`, which is a syntax error. A missing value, or one that isn't a string, number or bool, raises `CompileError`.
  - A `%` in a property value, as in a SAS token, is escaped for the driver instead of failing in pymysql's parameter formatting. Write it once: a `%%` doubled by hand now reaches StarRocks as `%%`.

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