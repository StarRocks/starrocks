// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.alter.reshard.presplit;

import com.google.common.annotations.VisibleForTesting;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.TableFunctionTable;
import com.starrocks.common.Pair;
import com.starrocks.common.StarRocksException;
import com.starrocks.load.BrokerFileGroup;
import com.starrocks.sql.ast.BrokerDesc;
import com.starrocks.sql.ast.ImportColumnDesc;
import com.starrocks.thrift.TBrokerFileStatus;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Production data-tier {@link SampleSubqueryExecutor} for the Broker Load path.
 * Translates the load's resolved {@link BrokerLoadScanContext} into a FILES
 * property map and delegates the SQL synthesis, BE invocation, and JSON
 * decode to {@link FilesSampleSubqueryExecutor}.
 *
 * <p>The sub-query reads exactly the file-status snapshot the load's pending
 * task resolved — re-globbing here would race with the load's own
 * enumeration and risk planning quantile cuts from a different file set
 * than the one actually loaded.
 *
 * <p>A {@code COLUMNS FROM PATH} declaration is forwarded to FILES as its own
 * {@code columns_from_path} property, so a path-derived column — typically the
 * partition column of a table partitioned by load directory — is a real column
 * of the sub-query's schema and the sampler can project it to attribute samples
 * to partitions.
 *
 * <p>Sources the FILES table function or this sampler cannot honor are
 * rejected with {@link StarRocksException}; the coordinator maps that to
 * {@link SkipReason#SAMPLE_FAILED} and the load proceeds without pre-split.
 * Rejected shapes include broker-backed loads, missing/disagreeing file-group
 * formats, non-Parquet/ORC sources, file groups disagreeing on
 * {@code columns_from_path}, paths containing FILES's {@code ,} list separator,
 * empty file lists, and the column mappings
 * {@link #rejectKeyPerturbingFileGroups} screens out — a per-group {@code WHERE},
 * a negative load, a hadoop function, a {@code SET} / derived column, or a key
 * column that the mapping either supplies from the path or never names.
 * (Example: {@code SET sort_key = upper(file_x)} would have the sampler read
 * the file's raw {@code sort_key} column instead of the mapped value the load
 * inserts.)
 */
final class BrokerLoadSampleSubqueryExecutor extends FilesSampleSubqueryExecutor {

    private static final String ERROR_PREFIX = "Broker Load data tier ";

    BrokerLoadSampleSubqueryExecutor() {
        super(ERROR_PREFIX);
    }

    @VisibleForTesting
    BrokerLoadSampleSubqueryExecutor(SampleQueryRunner sampleQueryRunner) {
        super(ERROR_PREFIX, sampleQueryRunner);
    }

    @Override
    protected Source resolveSource(SampleRequest request) throws StarRocksException {
        BrokerLoadScanContext context = requireBrokerLoadContext(request);
        BrokerDesc brokerDesc = requireBrokerDesc(context);
        rejectIfBrokerBacked(brokerDesc);
        // Resolve the format BEFORE the mapping guard: that guard admits an all-identity COLUMNS
        // list, which is only sound for the self-describing formats whose declared columns the BE
        // resolves by name. A CSV COLUMNS list is positional, and this call rejects CSV outright.
        String format = resolveSharedFormat(context.fileGroups());
        rejectKeyPerturbingFileGroups(context.fileGroups(), sampledKeyColumns(request));

        List<String> columnsFromPath = resolveSharedColumnsFromPath(context.fileGroups());
        ResolvedFiles resolved = collectResolvedFiles(context.fileStatusesPerGroup());

        Map<String, String> filesProperties = buildFilesProperties(
                brokerDesc, String.join(",", resolved.paths()), format, columnsFromPath);
        return new Source(filesProperties, resolved.totalBytes(), context.computeResource());
    }

    private static BrokerLoadScanContext requireBrokerLoadContext(SampleRequest request) throws StarRocksException {
        ScanContext scanContext = request.getScanContext();
        if (!(scanContext instanceof BrokerLoadScanContext brokerLoadContext)) {
            throw new StarRocksException(ERROR_PREFIX + "received a "
                    + scanContext.getClass().getSimpleName() + " — wire only the Broker Load load kind here");
        }
        return brokerLoadContext;
    }

    private static BrokerDesc requireBrokerDesc(BrokerLoadScanContext context) throws StarRocksException {
        BrokerDesc brokerDesc = context.brokerDesc();
        if (brokerDesc == null) {
            // BrokerLoadJob's construction requires a BrokerDesc; surface as a
            // clean data tier failure rather than NPE if a future caller violates
            // that invariant.
            throw new StarRocksException(ERROR_PREFIX + "scan context is missing BrokerDesc");
        }
        return brokerDesc;
    }

    private static void rejectIfBrokerBacked(BrokerDesc brokerDesc) throws StarRocksException {
        if (brokerDesc.hasBroker()) {
            throw new StarRocksException(ERROR_PREFIX
                    + "broker-backed sources are not supported — FILES uses FE-local Hadoop "
                    + "access which does not honor the broker's filesystem/auth");
        }
    }

    /**
     * Every key column this request will sample. Pre-split only admits a table with a single
     * visible index (a rollup or synchronous MV is declined as HAS_MATERIALIZED_VIEW_OR_ROLLUP
     * before sampling), so the sampled keys are exactly the base sort key.
     */
    static List<Column> sampledKeyColumns(SampleRequest request) {
        return new ArrayList<>(request.getSortKey());
    }

    /**
     * Rejects the file-group shapes whose column mapping would make the sampled key distribution
     * diverge from the distribution the load actually writes. Shared with the meta-tier
     * {@link BrokerLoadRowGroupStatisticsProvider} so both tiers agree on the supported window.
     *
     * <p>The test is deliberately narrower than "any column mapping at all". A {@code COLUMNS} list
     * and {@code COLUMNS FROM PATH} are how a Broker Load <i>names</i> its source fields; neither
     * changes a value by itself, and rejecting them outright left the ordinary
     * "Parquet + partition directory" load with no pre-split on <i>either</i> tier. What still has
     * to be rejected is a mapping that changes a sampled key column, or leaves one unpopulated:
     *
     * <ul>
     *   <li>a per-group {@code WHERE}, a negative load, or a legacy hadoop function — these filter
     *       or rewrite rows wholesale;</li>
     *   <li>a non-identity {@link ImportColumnDesc} (a {@code SET} / derived column), because the
     *       sampler reads the file's raw column while the load inserts the mapped value;</li>
     *   <li>a {@code COLUMNS FROM PATH} name that is itself a sampled key column — its value lives
     *       in the directory name, not in the file, so no footer or file scan can reproduce it;</li>
     *   <li>a {@code COLUMNS} list that omits a sampled key column — the load then leaves that
     *       column at its default while the sampler would read whatever the file carries.</li>
     * </ul>
     *
     * <p>Admitting an all-identity {@code COLUMNS} list is sound because both tiers are
     * Parquet/ORC-only ({@link #resolveSharedFormat} here, {@link MetaTierFormat} on the meta tier)
     * and the BE resolves such a load's declared columns <b>by name</b>
     * ({@code ParquetReaderWrap::column_indices} fails with "Column: X is not found in file" when a
     * declared name is absent). The sampler's own {@code SELECT <key> FROM FILES(...)} is by name
     * too, so both sides land on the same physical column. A CSV {@code COLUMNS} list is positional
     * and would <i>not</i> be safe — which is why the caller resolves the format first.
     */
    static void rejectKeyPerturbingFileGroups(List<BrokerFileGroup> fileGroups, List<Column> sampledKeyColumns)
            throws StarRocksException {
        for (BrokerFileGroup fileGroup : fileGroups) {
            if (fileGroup.getWhereExpr() != null) {
                throw new StarRocksException(ERROR_PREFIX
                        + "WHERE filter on file group is not supported "
                        + "(sampler would observe rows the load will exclude)");
            }
            if (fileGroup.isNegative()) {
                throw new StarRocksException(ERROR_PREFIX
                        + "negative-load file groups are not supported");
            }
            Map<String, Pair<String, List<String>>> hadoopFunctions = fileGroup.getColumnToHadoopFunction();
            if (hadoopFunctions != null && !hadoopFunctions.isEmpty()) {
                throw new StarRocksException(ERROR_PREFIX
                        + "legacy hadoop column functions on file group are not supported");
            }
            rejectRemappedKeyColumns(fileGroup, sampledKeyColumns);
        }
    }

    private static void rejectRemappedKeyColumns(BrokerFileGroup fileGroup, List<Column> sampledKeyColumns)
            throws StarRocksException {
        List<ImportColumnDesc> columnExpressions = fileGroup.getColumnExprList();
        if (columnExpressions != null) {
            for (ImportColumnDesc columnExpression : columnExpressions) {
                // isColumn() == expr is null == "this entry only names a source field".
                if (!columnExpression.isColumn()) {
                    throw new StarRocksException(ERROR_PREFIX + "SET clause / derived column \""
                            + columnExpression.getColumnName() + "\" is not supported "
                            + "(sampler would read the file's raw column, not the mapped value)");
                }
            }
        }
        List<String> columnsFromPath = fileGroup.getColumnsFromPath();
        if (columnsFromPath != null) {
            for (String pathColumn : columnsFromPath) {
                if (namesAnyColumn(sampledKeyColumns, pathColumn)) {
                    throw new StarRocksException(ERROR_PREFIX + "columns_from_path column \"" + pathColumn
                            + "\" is a tablet key column; its value is in the directory name rather "
                            + "than in the file, so tablet boundaries cannot be sampled from it");
                }
            }
        }
        // A COLUMNS list enumerates every field the load reads (file fields first, then the path
        // columns). When one is present, a key column it does not name is never populated from the
        // source: the load leaves it at its default while the sampler would read whatever the file
        // happens to carry under that name, so the boundaries would describe the wrong values.
        if (columnExpressions == null || columnExpressions.isEmpty()) {
            return;
        }
        for (Column keyColumn : sampledKeyColumns) {
            if (!namesImportedColumn(columnExpressions, keyColumn.getName())) {
                throw new StarRocksException(ERROR_PREFIX + "COLUMNS list does not name key column \""
                        + keyColumn.getName() + "\", so the load does not populate it from the source");
            }
        }
    }

    private static boolean namesAnyColumn(List<Column> columns, String columnName) {
        for (Column column : columns) {
            if (column.getName().equalsIgnoreCase(columnName)) {
                return true;
            }
        }
        return false;
    }

    private static boolean namesImportedColumn(List<ImportColumnDesc> columnExpressions, String columnName) {
        for (ImportColumnDesc columnExpression : columnExpressions) {
            if (columnExpression.getColumnName().equalsIgnoreCase(columnName)) {
                return true;
            }
        }
        return false;
    }

    /**
     * One FILES call takes one {@code columns_from_path} list, so every file group must declare the
     * same one. FILES appends these as real string columns of its inferred schema
     * ({@code TableFunctionTable.getSchemaFromPath}), exactly as Broker Load appends them to the
     * loaded tuple — which is what lets the sampler project a path-derived partition column and
     * attribute samples to partitions.
     */
    private static List<String> resolveSharedColumnsFromPath(List<BrokerFileGroup> fileGroups)
            throws StarRocksException {
        List<String> shared = null;
        for (BrokerFileGroup fileGroup : fileGroups) {
            List<String> columnsFromPath = fileGroup.getColumnsFromPath();
            List<String> declared = columnsFromPath == null ? List.of() : columnsFromPath;
            if (shared == null) {
                shared = declared;
            } else if (!shared.equals(declared)) {
                throw new StarRocksException(ERROR_PREFIX + "file groups disagree on columns_from_path: "
                        + shared + " vs " + declared);
            }
        }
        return shared == null ? List.of() : shared;
    }

    /**
     * A single FILES call takes one {@code format} property, so every file
     * group must declare the same non-null format. CSV and JSON are rejected
     * in this commit — translating CSV byte-valued options onto FILES
     * {@code csv.*} string properties is a follow-up.
     */
    private static String resolveSharedFormat(List<BrokerFileGroup> fileGroups) throws StarRocksException {
        if (fileGroups.isEmpty()) {
            throw new StarRocksException(ERROR_PREFIX + "no file groups in scan context");
        }
        String sharedFormat = null;
        for (BrokerFileGroup fileGroup : fileGroups) {
            String declaredFormat = fileGroup.getFileFormat();
            if (declaredFormat == null) {
                throw new StarRocksException(ERROR_PREFIX
                        + "file group has no declared format (extension inference is not supported by data tier)");
            }
            String normalizedFormat = declaredFormat.toLowerCase(Locale.ROOT);
            if (sharedFormat == null) {
                sharedFormat = normalizedFormat;
            } else if (!sharedFormat.equals(normalizedFormat)) {
                throw new StarRocksException(ERROR_PREFIX + "file groups disagree on format: \""
                        + sharedFormat + "\" vs \"" + normalizedFormat + "\"");
            }
        }
        if (!"parquet".equals(sharedFormat) && !"orc".equals(sharedFormat)) {
            throw new StarRocksException(ERROR_PREFIX
                    + "format \"" + sharedFormat + "\" is not yet supported (Parquet and ORC only)");
        }
        return sharedFormat;
    }

    /**
     * Walks the load's resolved file-status snapshot, dropping directory
     * entries, summing file byte totals, and returning the per-file paths
     * for the FILES {@code path} property. A path containing {@code ,} is
     * rejected because FILES has no escape syntax for its path-list
     * separator.
     */
    private static ResolvedFiles collectResolvedFiles(
            List<List<TBrokerFileStatus>> fileStatusesPerGroup) throws StarRocksException {
        List<String> paths = new ArrayList<>();
        long totalBytes = 0L;
        for (List<TBrokerFileStatus> filesInGroup : fileStatusesPerGroup) {
            for (TBrokerFileStatus fileStatus : filesInGroup) {
                if (fileStatus.isDir) {
                    continue;
                }
                if (fileStatus.path.indexOf(',') >= 0) {
                    throw new StarRocksException(ERROR_PREFIX
                            + "file path contains \",\" which FILES treats as a path-list separator: "
                            + fileStatus.path);
                }
                paths.add(fileStatus.path);
                totalBytes += fileStatus.size;
            }
        }
        if (paths.isEmpty()) {
            throw new StarRocksException(ERROR_PREFIX + "no files to sample (all entries were directories or empty)");
        }
        return new ResolvedFiles(paths, totalBytes);
    }

    /**
     * Broker properties (e.g. {@code fs.s3a.access.key}) pass through verbatim
     * — FILES and Broker Load share the same Hadoop {@code FileSystem}
     * configuration surface. {@code path}, {@code format} and
     * {@code columns_from_path} are appended last so a misconfigured
     * BrokerDesc cannot silently override them.
     */
    private static Map<String, String> buildFilesProperties(
            BrokerDesc brokerDesc, String commaSeparatedPaths, String format, List<String> columnsFromPath) {
        Map<String, String> filesProperties = new LinkedHashMap<>(brokerDesc.getProperties());
        filesProperties.put(TableFunctionTable.PROPERTY_PATH, commaSeparatedPaths);
        filesProperties.put(TableFunctionTable.PROPERTY_FORMAT, format);
        if (!columnsFromPath.isEmpty()) {
            // Mirrors the load's own COLUMNS FROM PATH so the sub-query can select a path-derived
            // column (typically the partition column) by name.
            filesProperties.put(TableFunctionTable.PROPERTY_COLUMNS_FROM_PATH,
                    String.join(",", columnsFromPath));
        }
        return filesProperties;
    }

    private record ResolvedFiles(List<String> paths, long totalBytes) {
    }
}
