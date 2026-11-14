# AGENTS.md - StarRocks Python Client

> Guidelines for AI coding agents working on the StarRocks SQLAlchemy dialect and its Alembic integration.
> This client is a standalone Python package; the backend and frontend guides elsewhere in the repository do not apply here.

## Overview

`starrocks-python-client` is the `starrocks` package on PyPI: a SQLAlchemy dialect for StarRocks plus an Alembic
integration that autogenerates migrations for StarRocks tables, views and materialized views.

- **Language**: Python 3.10 to 3.14
- **Libraries**: SQLAlchemy 1.4 and 2.0, Alembic, pymysql or asyncmy as the driver, lark for the DDL parser,
  sqlglot (optional) for view definition canonicalization
- **Packaging**: `pyproject.toml` (dependencies and the `test` and `alembic` extras)

Run every command below from this directory (`contrib/starrocks-python-client`).

## Layout

| Path | Contents |
|---|---|
| `starrocks/dialect.py` | The dialect: compilers, type mapping, connection setup, reflection entry points |
| `starrocks/reflection.py`, `starrocks/engine/interfaces.py` | Reflecting tables, views, MVs and their StarRocks-specific attributes |
| `starrocks/drivers/` | The lark grammar and parser for `SHOW CREATE` output |
| `starrocks/sql/` | `View` and `MaterializedView` (`schema.py`), and StarRocks DDL constructs such as `ALTER TABLE ...` (`ddl.py`) |
| `starrocks/datatype.py` | StarRocks types (`BITMAP`, `HLL`, `ARRAY`, `MAP`, `STRUCT`, ...) |
| `starrocks/alembic/` | Autogenerate comparators (`compare.py`), operations (`ops.py`), rendering (`render.py`) and execution (`toimpl.py`, `starrocks.py`) |
| `starrocks/common/` | Shared defaults, enums and SQL canonicalization |
| `starrocks/provision.py`, `starrocks/requirements.py` | Hooks and requirements for SQLAlchemy's testing plugin |
| `test/` | Tests; see [Tests](#tests) |
| `docs/design/`, `docs/test/`, `docs/usage_guide/` | Design notes, test strategy, and user guides |

## Setup

```bash
pip install -e ".[test]"
```

The tests need a running StarRocks cluster, even the unit tests: SQLAlchemy's testing plugin connects to it at
startup. On a fresh cluster:

```sql
CREATE DATABASE IF NOT EXISTS test_sqla;
-- on a cluster with a single BE:
ADMIN SET FRONTEND CONFIG ("default_replication_num" = "1");
```

The database URL is taken from, in order: `--dburi`, the `STARROCKS_URL` environment variable, a git-ignored
`test.cfg` with a `[db]` section, then `setup.cfg` (`starrocks://root@localhost:9030/test_sqla`).

## Tests

All tests run under SQLAlchemy's testing plugin, which `test/conftest.py` loads.

```bash
pytest test/unit test/sql          # unit and compiler tests
pytest test/integration test/system
pytest test/test_suite.py          # SQLAlchemy's dialect suite
pytest test/test_suite_alembic.py  # Alembic's dialect suite (SQLAlchemy 2.0 only)
pytest -n0 ...                     # run serially; pyproject.toml defaults to -n4
```

Run the suites on SQLAlchemy 2.0 and on 1.4 when a change touches code shared by both.

### Writing tests

- **Every test is a `test_*` method of a `sqlalchemy.testing.fixtures.TestBase` subclass.** The plugin silently
  drops module-level test functions and plain pytest classes; `test/conftest.py` fails collection for them instead.
- Parametrize with `@testing.combinations`, not `pytest.mark.parametrize`.
- Compare compiled SQL with `assert_compile` (mix in `AssertsCompiledSQL`, set `__dialect__ = StarRocksDialect()`).
- Test Alembic operations through `alembic.testing.fixtures.op_fixture("starrocks")` rather than mocking Alembic
  internals.
- A test that needs the database sets `__backend__ = True` and uses the plugin's `connection` and `metadata` fixtures.
- Views, materialized views and the Alembic integration need SQLAlchemy 2.0; gate their tests with
  `__requires__ = ("sqlalchemy_2",)` or `@testing.requires.sqlalchemy_2`.
- Mark what doesn't work, with the reason:
  - a StarRocks limitation: `testing.skip("starrocks", "<what StarRocks rejects>")`;
  - a dialect bug: `testing.fails_on("starrocks", "<what the dialect gets wrong>")`. It fails on unexpected success,
    so the mark must go with the fix.
- Before recording a StarRocks limitation, confirm it against a live cluster; behavior differs between versions.

## StarRocks behavior that affects the dialect

- No foreign keys and no `FOREIGN KEY` DDL; table creation order and drop order don't matter.
- Index and column changes (`CREATE INDEX`, `ALTER TABLE ... MODIFY/ADD/DROP COLUMN`) run as asynchronous jobs,
  listed by `SHOW ALTER TABLE COLUMN`. Until a job finishes, the table rejects further schema changes and `TRUNCATE`,
  and a duplicate-key table also rejects `DELETE`. Alembic waits for them when
  `starrocks_wait_for_schema_change=True`; reuse that wait (`starrocks/alembic/starrocks.py`) rather than writing
  another polling loop.
- A dropped database or table stays in the recycle bin with its tablets unless dropped with `FORCE`.
- StarRocks rewrites view and MV definitions when storing them, so reflected SQL differs from what was created;
  `starrocks/common/` canonicalizes both sides before autogenerate compares them.

## Coding standards

- Annotate types on every function and method; write Google-style docstrings with `Args`/`Returns`/`Raises`.
- Keep changes to the task's scope; don't reformat or refactor unrelated code.
- Follow markdownlint for Markdown files.

## Documentation

Keep the docs current with the code:

- `docs/design/`: the design of a feature or significant change, written before or with the implementation.
- `docs/test/`: test strategy and coverage for it.
- `docs/usage_guide/`: user-facing behavior; update it with every user-visible change.
- `CHANGES.md`: one entry per user-visible change.
- `README.md`: installation, quick start, and running the tests.

The public page [`docs/en/integrations/starrocks_sqlalchemy.md`](../../docs/en/integrations/starrocks_sqlalchemy.md)
follows the rules in [`docs/AGENTS.md`](../../docs/AGENTS.md).

## PR conventions

Follow the repository's [PR contract](../../AGENTS.md#pr-contract). Titles name the client, e.g.
`[BugFix] Python SQLAlchemy Dialect - Reflect TIME and VARIANT columns`.
