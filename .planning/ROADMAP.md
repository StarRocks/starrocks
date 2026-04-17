# Roadmap: ADBC Dynamic Driver Loader Rework

## Overview

Rework the in-flight ADBC external catalog on `feature/adbc-catalog-2` so that ADBC drivers are loaded at runtime via the upstream `adbc-driver-jni` (FE Java) and `libadbc_driver_manager.so` (BE C++) instead of bundling Apache Arrow Flight SQL into the StarRocks build. The journey: prove the runtime-loader contract end-to-end with a throwaway spike (Phase 1), do the entire FE-side swap as one coherent change with all loader-lifetime decisions baked in (Phase 2), do the BE rewrite plus build-system purge plus SQLite CI as one atomic landing (Phase 3), then ship docs and the hard-break migration messaging (Phase 4). Granularity: **coarse** (4 phases).

## Phases

**Phase Numbering:**
- Integer phases (1, 2, 3, 4): Planned milestone work
- Decimal phases (2.1, 3.1, ...): Reserved for urgent insertions, none planned

- [ ] **Phase 1: Scaffold, Schema Freeze, Loader Spike** - Prove `adbc-driver-jni` loads SQLite end-to-end in an FE unit-test spike; freeze the property schema and migration policy with no product code touched.
- [ ] **Phase 2: FE Rewrite — Driver Acquisition, Validation, Plan Path** - Single coherent FE-side swap: pom deps, `ADBCMetadata`/`ADBCConnector` rewrite, Thrift IDL additions, plan-path forwarding, FE tests.
- [ ] **Phase 3: BE Rewrite, Build-System Purge, CI Smoke** - Atomic BE-side landing: scanner rewrite mirroring the JDBC BE pattern (read connection contract from `TADBCTable` via a new `ADBCTableDescriptor`, not from the scan node), dlopen lifecycle, RAII wrappers, CMake swap, Go toolchain removal, revert of `88f715bd79`, SQLite CI gating with TSan/ASan.
- [ ] **Phase 4: Docs, Hard-Break Migration, Final Audit** - User docs (en + zh), release notes, residual `flight_sql` / `dlclose` / `RTLD_GLOBAL` audit pass.

## Phase Details

### Phase 1: Scaffold, Schema Freeze, Loader Spike
**Goal**: Validate that `adbc-driver-jni` **0.21.0** (the newest ADBC release that still pins Arrow at 18.0.0, matching fe-core) can load `libadbc_driver_sqlite.so` from inside an FE unit-test JVM and call `getObjects` against an in-memory database, before any product code in `feature/adbc-catalog-2` is touched. Freeze the property schema and the hard-break migration policy so Phase 2 has nothing left to debate.

**Depends on**: Nothing (first phase)

**Requirements**: (none — this phase produces design artifacts and a throwaway spike; it de-risks STK-01/02/03 and the FS-1 schema before they formally land in Phase 2)

**Success Criteria** (what must be TRUE):
  1. A throwaway FE unit test in `fe/fe-core/src/test/java/...` (not committed to the product code path) loads `libadbc_driver_sqlite.so` via `JniDriverFactory.getDriver()` + `JniDriver.open(params)` against an in-memory SQLite database and asserts that `AdbcConnection.getObjects` returns at least one schema entry.
  2. The frozen property schema is captured in this roadmap and in REQUIREMENTS.md PROP-01..09: exactly the keys `type`, `driver_url`, `driver_name`, `driver_entrypoint`, `uri`, `user`, `password`, `path` allowed at top level; everything else must use the `adbc.*` prefix.
  3. The migration policy is documented as **hard break** — no soft alias, no in-place migration shim — with the rationale that ADBC catalog has not shipped in any released StarRocks version, so released-user impact is zero.
  4. Build strategy is decided: **source-build only**, `libadbc_driver_manager.so` is built from vendored `thirdparty/` source with `-DADBC_DRIVER_MANAGER=ON -DADBC_BUILD_SHARED=ON -DADBC_DRIVER_FLIGHTSQL=OFF`. No prebuilt apt dependency. Airgapped builds work by default. Go toolchain not required because the C++ driver manager has no Go in it.
  5. The Go-cgo driver multi-load policy is decided and recorded: a hard-coded list (`adbc_driver_flightsql`, `adbc_driver_snowflake`, `adbc_driver_bigquery`) blocks loading a second Go-based driver into the same BE process, with a structured DDL error.

**Plans**: TBD

**Risks** (from PITFALLS.md, deferred to later phases but flagged here so Phase 2/3 plans pre-empt them):
- Arrow version skew (Pitfall 5) — Phase 2 keeps `<arrow.version>` at **18.0.0** (fe-core's existing pin) and uses `adbc-driver-jni` **0.21.0**, which is the newest ADBC release whose parent pom still pins Arrow 18.0.0. ADBC 0.22+ bumps Arrow to 18.3.0, which we deliberately avoid.
- Bridge module scope question — resolved here in favor of **keeping ADBC code inside `fe/fe-core`**, no new `java-extensions/adbc-bridge` module. Locked.

---

### Phase 2: FE Rewrite — Driver Acquisition, Validation, Plan Path
**Goal**: Replace every Java-side reference to `FlightSqlDriver` / `FlightSqlConnectionProperties` with the runtime-loaded `JniDriver` model, deliver the new property schema end-to-end on the FE side, and forward the new fields through `TADBCScanNode` to BE so Phase 3 has something to consume. After this phase, FE compiles cleanly against the new APIs while BE temporarily still links the old `adbc_driver_flightsql` static library — that mid-state lasts exactly until Phase 3 ships.

**Depends on**: Phase 1

**Requirements**: STK-01, STK-02, STK-03, PROP-01, PROP-02, PROP-03, PROP-04, PROP-05, PROP-06, PROP-07, PROP-08, PROP-09, META-01, META-02, META-03, META-04, META-05, META-06, VAL-01, VAL-02, VAL-03, VAL-04, VAL-05, PLAN-01, PLAN-02, PLAN-03, PLAN-04, TEST-02, TEST-06

**Success Criteria** (what must be TRUE):
  1. `CREATE EXTERNAL CATALOG c PROPERTIES("type"="adbc","driver_url"="/abs/path/to/libadbc_driver_sqlite.so","uri"=":memory:")` succeeds against the SQLite ADBC driver and the resulting catalog appears in `SHOW CATALOGS`. Catalog creation eagerly loads the driver and probes its reported ADBC version (logged once).
  2. `CREATE CATALOG` with both `driver_url` and `driver_name` set, with neither set, with an unknown top-level key, with a non-existent `driver_url` path, with an entrypoint symbol that does not resolve, or with an ABI-mismatched driver each fails with a **distinct, structured** error message naming the failure class.
  3. Any `adbc.*` property on `CREATE CATALOG` arrives verbatim at `AdbcDatabase.SetOption(key, value)`; the legacy `flight_sql` driver-name whitelist, the `grpc://`/`grpc+tls://` URI-scheme whitelist, and the `PROP_TLS_*` InputStream-based cert-loading constants no longer exist in `ADBCConnector.java`.
  4. `DROP CATALOG` against a legacy v1-shape catalog (one created on the old `adbc.driver=flight_sql` schema) succeeds without invoking driver init, preserving the migration path.
  5. FE plan tests (`ADBCScanPlanTest`, `ADBCScanImplementationRuleTest`) assert that a serialized `TADBCScanNode` carries the new optional fields `driver_url`, `entrypoint`, and `adbc_options`, populated from catalog properties at `toThrift()` time. The legacy `getADBCDriverName()` mapping at `ADBCScanNode.java:140-147` is deleted.
  6. The FE test base class enables `arrow.memory.debug.allocator=true` so any Arrow Java allocator leak fails the test loudly. The SQLite-driver FE integration test passes under this allocator.
  7. Concurrent `CREATE CATALOG` against the same `driver_url` from two threads is safe: `ConcurrentHashMap.computeIfAbsent` guards the per-driver-path init and the loader is invoked at most once per resolved absolute path.

**Plans:** 5 plans

Plans:
- [x] 02-01-PLAN.md — Maven deps, Thrift IDL, surefire config, spike deletion
- [x] 02-02-PLAN.md — ADBCConnector rewrite: property validation, driver loading, error model
- [x] 02-03-PLAN.md — ADBCMetadata rewrite: SQL-based metadata via JniDriver
- [x] 02-04-PLAN.md — ADBCScanNode rewrite: plan path forwarding of new Thrift fields
- [x] 02-05-PLAN.md — SQLite integration test: full lifecycle with real native driver

**Risks** (from PITFALLS.md):
- **Arrow version lockstep (Pitfall 5).** STK-03 is non-negotiable: `<arrow.version>` stays at exactly **18.0.0** (the version ADBC 0.21.0's parent pom pins). Picking `adbc-driver-jni` 0.21.0 instead of 0.23.0 is a deliberate choice to avoid the Arrow 18.0.0 → 18.3.0 bump that ADBC 0.22+ forces. Drift causes SIGSEGV on first JNI marshal. CI must fail the build on drift.
- **Staged loader error classification (VAL-03 / Pitfall 6).** Each failure mode must be raised as a distinct exception class so DDL surfaces can map to user-facing messages. A single "loader failed" catch-all defeats the FS-6 contract.
- **Concurrent DDL race (META-05 / Pitfall 7).** Two simultaneous `CREATE CATALOG` against the same driver path must not race on driver-manager internal state. `computeIfAbsent` guard is the mitigation.
- **`PROP_TLS_*` removal (PROP-09 / Pitfall 11).** The InputStream-based cert-loading path does not survive the JniDriver migration — the Java pass-through layer rejects non-String option values. TLS becomes a driver-native file-path option (e.g. `adbc.flight.sql.client_option.tls_root_certs=/path/to/ca.pem`). Phase 4 docs must call this out for Flight SQL users.

**Notes for plan-phase**:
- Repo invariant: Thrift IDL additions for `TADBCScanNode` must be `optional` only, must not reuse ordinals, and must not promote any field to `required`.
- Repo invariant: any user-facing config or metric change must update both `docs/en/` and `docs/zh/` — but documentation work for this phase batches into Phase 4.
- Per user preference: never mention GSD, planning sessions, roadmaps, or `.planning/` in commit messages. Imperative English; PR titles use the standard `[Refactor] ...` / `[Feature] ...` prefixes.

---

### Phase 3: BE Rewrite, Build-System Purge, CI Smoke
**Goal**: Atomically replace the BE-side static link against `adbc_driver_flightsql` with runtime loading via `libadbc_driver_manager.so`, rewrite `be/src/connector/adbc_connector.cpp` to mirror the JDBC BE pattern (read scan fields from `TADBCScanNode`, cast `_tuple_desc->table_desc()` to a new `ADBCTableDescriptor` subclass to read `driver_url` / `entrypoint` / `adbc_options` from `TADBCTable`), add the `ADBCTableDescriptor` class in `be/src/runtime/descriptors_ext.{h,cpp}` alongside the existing `JDBCTableDescriptor`, purge the Go toolchain dependency from `thirdparty/` (including reverting commit `88f715bd79`), and prove the whole stack against the SQLite ADBC driver under TSan and ASan in CI. This phase lands as one unit because BE-scanner-without-build-update and build-update-without-scanner are both broken mid-states.

**Depends on**: Phase 2

**Requirements**: STK-04, STK-05, STK-06, STK-07, BE-01, BE-02, BE-03, BE-04, BE-05, BE-06, BE-07, BE-08, BE-09, TEST-01, TEST-03, TEST-04, TEST-05

**Success Criteria** (what must be TRUE):
  1. A `SELECT *` against an ADBC catalog backed by `libadbc_driver_sqlite.so` returns rows end-to-end (FE plan → BE scan → result set) in CI, executed by an ADBC scan node whose `_init_adbc()` calls go through `libadbc_driver_manager.so` rather than statically linked Flight SQL symbols. Every `adbc.*` option from `TADBCScanNode.adbc_options` is forwarded via `AdbcDatabaseSetOption` before `AdbcDatabaseInit`.
  2. A TSan-instrumented BE build runs an 8-thread concurrent-scan test against the SQLite-backed catalog with no data races and no `ADBC_STATUS_INVALID_STATE` errors. Each fragment owns its own `AdbcConnection`; `AdbcConnection` instances are never cached across fragments. An ASan-instrumented BE build runs the full ADBC scan regression suite with zero leaks and zero use-after-free reports at the Arrow C Data Interface boundary.
  3. The BE never calls `dlclose` on an ADBC driver handle. Each driver `.so` is loaded exactly once per process, guarded by a `std::once_flag` keyed on the resolved absolute `driver_url`. Shutdown drains per-scanner state via `AdbcStatementRelease` → `AdbcConnectionRelease` → `AdbcDatabaseRelease` with a 30-second deadline; on deadline expiry, BE logs the hanging driver and calls `_exit(0)`.
  4. BE `dlopen` of any ADBC driver uses `RTLD_NOW | RTLD_LOCAL | RTLD_DEEPBIND` on glibc (with `RTLD_DEEPBIND` dropped + a runtime log note on non-glibc platforms). Attempting to register a second Go-based ADBC driver (Flight SQL, Snowflake, BigQuery) into the same BE process is refused at `CREATE CATALOG` time with a structured error.
  5. All `ArrowArray`, `ArrowSchema`, and `ArrowArrayStream` instances crossing the C Data Interface boundary are owned by move-only RAII wrappers (`ArrowArrayHolder`, `ArrowSchemaHolder`, `ArrowArrayStreamHolder`). Grep finds zero raw `release()` calls outside the wrapper module.
  6. `be/CMakeLists.txt:638` links `adbc_driver_manager` (shared) instead of `adbc_driver_flightsql` (static). Line 637 (`arrow_flight_sql`, used by the unrelated outbound Arrow Flight SQL server in `service_be/arrow_flight_sql_service.cpp`) is preserved unchanged. `thirdparty/build-thirdparty.sh:build_adbc()` is rewritten to source-build the manager as a shared library from vendored sources with no Go subshell. `build_go()` and all `GO_*` vars are deleted from `thirdparty/build-thirdparty.sh`, `thirdparty/vars-x86_64.sh`, `thirdparty/vars-aarch64.sh`, and `thirdparty/package-manifest.sh`. Commit `88f715bd79` is reverted in full.
  7. BE release packaging copies `thirdparty/installed/lib64/libadbc_driver_manager.so*` into the BE output directory so the BE binary's loader path resolves it at runtime without operator intervention.

**Plans:** 4 plans

Plans:
- [x] 03-01-PLAN.md — Thirdparty build rewrite (shared lib) + Go toolchain purge + CMakeLists swap
- [x] 03-02-PLAN.md — ADBCTableDescriptor + connector rewrite (descriptor-based data flow)
- [x] 03-03-PLAN.md — Scanner rewrite + driver registry + RAII wrappers
- [x] 03-04-PLAN.md — Throwaway C++ smoke tests (SQLite, concurrent scan)

**Risks** (from PITFALLS.md):
- **Never-`dlclose` correctness (BE-06 / Pitfall 3).** This is a correctness requirement, not an optimization — Go-based drivers (Flight SQL, Snowflake, BigQuery) hang on `dlclose`. ADBC 22 changed the upstream Rust manager for exactly this reason. The 30-second drain deadline + `_exit(0)` is the safety valve.
- **Per-fragment connection contract (BE-03 / Pitfall 4).** ADBC's thread-safety model is weaker than JDBC's: serialized access only; concurrent `AdbcStatementExecuteQuery` on the same connection scrambles protocol state. Per-fragment `AdbcConnection` is a correctness invariant the TSan test must enforce, not a performance choice.
- **`RTLD_DEEPBIND` interaction with StarRocks OpenSSL linkage (BE-04 / Pitfall 2).** Known to interact poorly with ASan and with system-OpenSSL linkage. Plan-phase for this phase MUST budget a focused spike to validate the flag combination against StarRocks's existing linkage before committing it to the dlopen wrapper.
- **Preserving `arrow_flight_sql` at CMakeLists.txt:637 (STK-06).** That line belongs to an unrelated outbound Arrow Flight SQL server, not to the ADBC catalog. Touching it breaks `service_be/arrow_flight_sql_service.cpp`. The diff must surgically swap line 638 only.
- **Reverting `88f715bd79` cleanly (STK-05).** The Go toolchain workaround introduced by that commit may have been touched by intervening commits on `feature/adbc-catalog-2`. Plan-phase must verify the revert applies cleanly and resolve any conflicts before assuming the workaround is gone.
- **SQLite-only CI is insufficient (Pitfall 8).** SQLite has no network, no TLS, no Go runtime, no async — every interesting failure mode is hidden. SQLite is the **blocking per-PR** smoke gate; the nightly Flight SQL/Postgres integration layer is explicitly **deferred to v2 (CI-01..03)** per scope decision. Risk accepted.

**Notes for plan-phase**:
- Repo invariant: no Thrift `required` fields, no reused ordinals on any IDL touched by this phase.
- Per user preference: commit messages must not reference GSD, roadmaps, planning, or `.planning/` artifacts. Use imperative English with `[Refactor]` / `[Feature]` / `[BugFix]` PR-title prefixes.

---

### Phase 4: Docs, Hard-Break Migration, Final Audit
**Goal**: Make the rework shippable: ground-truth user-facing documentation in both English and Chinese, surface the hard-break migration loudly so pre-release users notice, and grep the entire ADBC code path one more time for residual `dlclose`, `RTLD_GLOBAL`, and `flight_sql` references that should not have survived Phases 2 and 3.

**Depends on**: Phase 3

**Requirements**: DOC-01, DOC-02, DOC-03, DOC-04, DOC-05

**Success Criteria** (what must be TRUE):
  1. A user reading `docs/en/data_source/catalog/adbc_catalog.md` can: install an ADBC driver `.so` via `dbc` or apt, write a `CREATE EXTERNAL CATALOG` statement, query their data, and understand the per-driver `adbc.*` keys for at least Flight SQL, SQLite, Postgres, and DuckDB. The doc explicitly notes the Go-based-driver single-load constraint.
  2. The Chinese peer document `docs/zh/data_source/catalog/adbc_catalog.md` is content-equivalent to the English version (per repo policy: `docs/en/` and `docs/zh/` stay in sync for user-facing changes).
  3. The release notes entry "ADBC external catalog: property schema changed" appears **above the fold** as a breaking change. The migration note explicitly says: existing catalogs created on `feature/adbc-catalog-2`'s old `adbc.driver=flight_sql` shape must be `DROP`-ed and recreated with `driver_url` / `driver_name`. The note records that ADBC catalog has not shipped in any released StarRocks version, so the released-user impact is zero — the loud signal is for pre-release branch users only.
  4. A final grep audit of the ADBC code path (`fe/fe-core/.../connector/adbc/`, `fe/fe-core/.../catalog/`, `fe/fe-core/.../planner/ADBCScanNode.java`, `be/src/exec/adbc_*`, `be/src/connector/adbc_*`) finds zero `dlclose` calls, zero `RTLD_GLOBAL` flags, and zero residual `flight_sql` / `FlightSql` / `flightsql` identifiers except where intentionally preserved (the `FlightSQLSchemaResolver` class name is acceptable as a vestigial driver-agnostic name; the `arrow_flight_sql` link at `be/CMakeLists.txt:637` is preserved unchanged). Each surviving occurrence is justified in writing.

**Plans**: TBD
**UI hint**: yes

**Risks** (from PITFALLS.md):
- **Hard-break migration messaging loudness (DOC-01, DOC-04 / Pitfall 9).** ADBC catalog is unreleased, but pre-release branch builds exist in user environments. The release notes line and the migration section in the user doc must both be unmissable or pre-release users will hit silent `DROP CATALOG` failures at upgrade time.

**Notes for plan-phase**:
- This is the only phase with a UI/docs surface (`UI hint: yes`) — covers user-facing reference docs, not application UI.
- Per user preference: commit messages must not reference GSD, planning sessions, or `.planning/` artifacts. PR title prefix `[Doc] ...`.

### Phase 5: External Integration Verification Suite

**Goal:** Create a standalone Python test suite at `/home/mete/coding/opensource/adbc_verification` that exercises the full ADBC catalog stack end-to-end against four backends (SQLite, FlightSQL via sqlflite Docker, PostgreSQL via Docker, DuckDB), with and without TLS, testing catalog operations, schema operations, data read/write, cross-driver joins, error handling, and negative cases. Docker lifecycle management included. Produces agent-native JSON diagnostics for AI-driven fix convergence.
**Requirements**: GOAL-05
**Depends on:** Phase 3
**Plans:** 4 plans

Plans:
- [x] 05-01-PLAN.md — Project scaffold, venv, library modules (StarRocks startup, Docker lifecycle, driver registry, TLS, catalog helpers), conftest.py fixtures
- [x] 05-02-PLAN.md — SQLite + DuckDB test modules (local backends, all D-09 scenarios)
- [x] 05-03-PLAN.md — FlightSQL + PostgreSQL test modules (Docker backends, TLS testing)
- [x] 05-04-PLAN.md — Cross-driver join tests + negative tests + JSON report verification

---

## Progress

**Execution Order:**
Phases execute in numeric order: 1 → 2 → 3 → 4 → 5

| Phase | Plans Complete | Status | Completed |
|-------|----------------|--------|-----------|
| 1. Scaffold, Schema Freeze, Loader Spike | 0/TBD | Not started | - |
| 2. FE Rewrite — Driver Acquisition, Validation, Plan Path | 0/5 | Planning complete | - |
| 3. BE Rewrite, Build-System Purge, CI Smoke | 0/4 | Planning complete | - |
| 4. Docs, Hard-Break Migration, Final Audit | 0/TBD | Not started | - |
| 5. External Integration Verification Suite | 0/4 | Planning complete | - |
