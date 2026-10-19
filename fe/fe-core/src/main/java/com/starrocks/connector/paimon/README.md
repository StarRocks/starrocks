# Paimon reader routing

## Verified baseline

Capability investigation: **2026-10-15**. Selector and integration validation:
**2026-10-16**. Implementation reference: StarRocks commit `c8f4807692d`.

| Component | Version / source |
| --- | --- |
| paimon-cpp | `0.3.0`; source archive MD5 `e82940588fa423926cb47b2617cf21dc`, pinned in [vars.sh](../../../../../../../../../thirdparty/vars.sh) |
| Java Paimon / fixture writer | `2.0.0`, pinned in [fe/pom.xml](../../../../../../../../pom.xml) |

These findings apply to this dependency combination and the tested fixtures.
Updating this document's date alone does not extend that verification to a new version.

## Responsibilities and precedence

[PaimonReaderSelector](PaimonReaderSelector.java) chooses a reader and a reason for
each split. [PaimonScanNode](../../planner/PaimonScanNode.java) constructs scan ranges:

| Reader | Task sent to BE | Execution |
| --- | --- | --- |
| `STARROCKS` | Raw file ranges and optional deletion files | SR Parquet/ORC scanners |
| `NATIVE` | Binary Paimon split | paimon-cpp; Arrow batches converted to SR chunks |
| `JNI` | Java-serialized split and predicates | Java Paimon SDK through JNI |

Selection order:

1. Non-DataSplit, including system-table splits: JNI, even when NATIVE is requested.
2. IndexedSplit: native, even when JNI or `paimon_force_jni_reader` is requested.
   Preserve index row ranges; never expand it into raw files. Incompatible native
   splits fail instead of falling back. Indexed splits wrapping FallbackDataSplit
   are rejected because the native binary decoder cannot handle that wrapper.
3. Ordinary DataSplit: explicit JNI/NATIVE selects that SDK reader. The legacy
   `paimon_force_jni_reader` setting overrides the requested mode with JNI.
4. Ordinary AUTO: compatible SR raw files, then compatible native reads, then JNI.

Native compatibility failures are planning errors for explicit NATIVE. AUTO can
fall back only if JNI supports the selected columns. Native execution failures do
not retry through JNI.

FallbackDataSplit resolves the fallback branch's schema before selection. If its
logical row type differs from the main branch, AUTO conservatively uses JNI;
explicit NATIVE reports an error. Explicit JNI stays on Java.

## Capability boundaries

The native checks below apply after the raw-file fast path is ruled out.
"Conservative" means metadata cannot prove the actual values are safe; it does
not mean every value or conversion fails. Explicit NATIVE bypasses the
conservative timestamp, nullable MAP-key and schema-conversion checks, while
retaining checks for known unsupported conditions.

| Condition | Boundary and routing policy |
| --- | --- |
| Raw-file fast path | Parquet/ORC with compatible selected field IDs, names and types. BLOB, timestamp precision above 6, and ORC timestamp/TIME projections skip this path. Renames and drop/re-add with a different field ID require SDK handling. |
| File formats | Native supports Parquet, ORC, Avro and BLOB. JSON/CSV/TEXT use JNI in AUTO. Check actual files and current/historical schema format options. |
| Whole-schema parsing | TIME, MULTISET and VECTOR in current or historical schemas block native, even if unselected. JNI fallback does not imply SR supports selecting that type; selected MULTISET is rejected by FE. |
| Primary-key tables | Fixed and dynamic buckets, including `bucket=-1`, were tested for reads. Bucket mode alone is not a JNI criterion. This does not establish native write support. |
| Merge semantics | Deduplicate, first-row, partial-update and aggregation were tested. Required aggregates depend on the selected fields, primary keys, sequence dependencies and file layout. `product`, `collect` and `merge_map` require JNI when actually needed for merging. Other aggregate/type combinations are constrained by `mergeSupported()`. |
| Legacy raw-convertible files | Missing delete-row counts can still trigger the native merge reader. Raw-convertible alone does not prove SDK reads need no merge. |
| Schema evolution | AUTO accepts unchanged types, integral widening and FLOAT-to-DOUBLE. Other changes conservatively use JNI. Tests found nested evolution failures and scalar conversion differences; ordinary nested reads without evolution were also tested. |
| Deletion vectors | Actual bitmap64 deletion vectors block native when enabled in current or historical options. Safe SR raw-file reads remain eligible. |
| ORC legacy TIMESTAMP_LTZ | Native can fail while initializing an unselected historical field. Check both current and historical legacy options and the file schema. |
| SDK timestamps | Conservative JNI in AUTO: negative fractional timestamps can fail during Arrow-to-SR conversion, and nanosecond timestamps can overflow. Eligible Parquet raw reads at precision <= 6 remain available. |
| Nullable MAP keys | ORC/Avro can contain NULL keys that fail native-to-SR conversion. AUTO conservatively uses JNI for relevant fields; non-nullable keys and Parquet are not rejected by this rule. |
| BLOB / VARIANT | Selected BLOB requires native. JNI rejects selected BLOB/VARIANT, including nested occurrences. Excluding those columns may permit JNI; an impossible combination fails planning. |

Merge dependencies are included in compatibility checks, not just output columns.
Nested access paths are currently checked conservatively using the selected
top-level Paimon types.

## Evidence and diagnostics

- [PaimonReaderSelectorTest](../../../../../../test/java/com/starrocks/connector/paimon/PaimonReaderSelectorTest.java):
  18 tests for selection, compatibility reasons, historical schemas and wrappers.
- [PaimonScanNodeTest](../../../../../../test/java/com/starrocks/planner/PaimonScanNodeTest.java):
  11 tests for task construction, serialization, routing and partition tracking.
  Together with PaimonSplitUtilsTest, the 30 related FE tests passed on the baseline.
- [SQL suite](../../../../../../../../../test/sql/test_paimon_catalog/README.md):
  40 static cases covering results, FE routing and native BE profiles.
- Additional local validation used 460 small, nonempty fixture tables and passed
  1,837 checks: full rows, ID projection, counts, and routing/profile checks.
  This includes three expected MULTISET analysis rejections. These temporary
  generators and fixtures are not a checked-in or continuously run CI suite.

From the repository root, run the related FE tests:

```bash
./run-fe-ut.sh -j 24 --test \
  'com.starrocks.connector.paimon.PaimonReaderSelectorTest,com.starrocks.planner.PaimonScanNodeTest,com.starrocks.connector.paimon.PaimonSplitUtilsTest'
```

`TRACE VALUES EXTERNAL` exposes
`Paimon.metadata.reader.<table>.selectionReason.<REASON>`, reader counts/bytes,
and the `<table>.selectPaimonReader` timing scope. FE routing is planning evidence;
verify actual native execution with the BE `PaimonNativeReader` profile on a
nonempty scan. Empty scans do not establish reader support.

Unverified combinations remain outside this snapshot, including arbitrary
cross-branch schema/option combinations and specialized index wrappers. Static
fixtures do not establish live write, cache invalidation or refresh behavior.

## When dependencies or routing change

Recheck the affected native source paths and conversion code, reproduce the
boundary with Java reference results, and add a focused regression test before
relaxing a guard. Run result, projection, predicate and actual reader-path checks
for the affected layouts. Update this baseline, capability table and user-facing
`paimon_reader_mode` documentation with the same change.
