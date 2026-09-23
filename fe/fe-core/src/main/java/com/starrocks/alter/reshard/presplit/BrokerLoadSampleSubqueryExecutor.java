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
import com.starrocks.common.util.SqlUtils;
import com.starrocks.load.BrokerFileGroup;
import com.starrocks.sql.ast.BrokerDesc;
import com.starrocks.sql.ast.ImportColumnDesc;
import com.starrocks.thrift.TBrokerFileStatus;
import com.starrocks.type.ScalarType;
import com.starrocks.type.StringType;
import com.starrocks.type.Type;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;

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
 * <p>CSV is admitted alongside Parquet and ORC, but it needs one extra step.
 * Parquet and ORC name their own columns, so the sampler's {@code SELECT <key>}
 * and the load's own read both resolve by name and land on the same physical
 * column with nothing else declared. A CSV field has no name: Broker Load maps
 * field <i>i</i> onto the <i>i</i>-th entry of the file group's {@code COLUMNS}
 * list, or — when there is no {@code COLUMNS} list — onto the <i>i</i>-th
 * loadable column of the target's base schema. The sampler pins that same
 * layout by declaring FILES's {@code schema} property, which CSV matches
 * <b>by position</b> with the declared names acting as aliases, and forwards
 * the file group's CSV dialect ({@code csv.column_separator} and friends) so
 * both sides split rows into fields identically. The meta tier stays
 * Parquet/ORC-only ({@link MetaTierFormat}) — a footer is the whole point
 * there — so a CSV load falls through to this tier.
 *
 * <p>Sources the FILES table function or this sampler cannot honor are
 * rejected with {@link StarRocksException}; the coordinator maps that to
 * {@link SkipReason#SAMPLE_FAILED} and the load proceeds without pre-split.
 * Rejected shapes include broker-backed loads, missing/disagreeing file-group
 * formats, formats outside Parquet/ORC/CSV, file groups disagreeing on
 * {@code columns_from_path}, paths containing FILES's {@code ,} list separator,
 * empty file lists, and the column mappings
 * {@link #rejectKeyPerturbingFileGroups} screens out — a per-group {@code WHERE},
 * a negative load, a hadoop function, a {@code SET} / derived column, or a key
 * column that the mapping either supplies from the path or never names.
 * (Example: {@code SET sort_key = upper(file_x)} would have the sampler read
 * the file's raw {@code sort_key} column instead of the mapped value the load
 * inserts.) CSV adds its own: file groups disagreeing on the positional field
 * layout or on the CSV dialect, a non-ASCII {@code enclose} / {@code escape}
 * byte, and a missing {@code COLUMNS} list with no base schema to fall back on.
 */
final class BrokerLoadSampleSubqueryExecutor extends FilesSampleSubqueryExecutor {

    private static final String ERROR_PREFIX = "Broker Load data tier ";

    private static final String FORMAT_PARQUET = "parquet";
    private static final String FORMAT_ORC = "orc";
    private static final String FORMAT_CSV = "csv";

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
        // list, which the BE resolves by name on the self-describing formats but POSITIONALLY on
        // CSV. Knowing the format up front is what lets the CSV branch below pin that position.
        String format = resolveSharedFormat(context.fileGroups());
        rejectKeyPerturbingFileGroups(context.fileGroups(), sampledKeyColumns(request));

        List<String> columnsFromPath = resolveSharedColumnsFromPath(context.fileGroups());
        ResolvedFiles resolved = collectResolvedFiles(context.fileStatusesPerGroup());

        Map<String, String> filesProperties = buildFilesProperties(
                brokerDesc, String.join(",", resolved.paths()), format, columnsFromPath);
        if (FORMAT_CSV.equals(format)) {
            appendCsvProperties(filesProperties, context, request);
        }
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
     * The union of every key column this request will sample: the base sort key plus each visible
     * rollup's own sort key. The mapping guard must clear all of them, not just the base — a rollup
     * sort-key column supplied from the path is just as unsamplable as a base one.
     */
    static List<Column> sampledKeyColumns(SampleRequest request) {
        List<Column> sampledKeyColumns = new ArrayList<>(request.getSortKey());
        for (SecondaryIndexSpec secondaryIndex : request.getSecondaryIndexSortKeys()) {
            sampledKeyColumns.addAll(secondaryIndex.sortKey());
        }
        return sampledKeyColumns;
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
     * group must declare the same non-null format. Parquet, ORC and CSV are
     * supported. JSON is rejected because FILES cannot read it at all; Avro,
     * which FILES does read, is simply not wired here yet.
     *
     * <p>A file group with no declared format is rejected rather than inferred
     * from the file extension: Broker Load's own inference ({@code Load.getFormatType})
     * is per file, so one file group can legitimately mix Parquet and CSV files
     * while one FILES call cannot.
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
        if (!FORMAT_PARQUET.equals(sharedFormat) && !FORMAT_ORC.equals(sharedFormat)
                && !FORMAT_CSV.equals(sharedFormat)) {
            throw new StarRocksException(ERROR_PREFIX
                    + "format \"" + sharedFormat + "\" is not yet supported (Parquet, ORC and CSV only)");
        }
        return sharedFormat;
    }

    /**
     * Pins the CSV field → column mapping the load will apply, and the dialect it will apply it
     * with, onto the FILES sub-query.
     *
     * <p>{@code schema} carries the positional layout. FILES matches a declared {@code schema}
     * against a CSV file <b>by position</b> — the declared names are pure aliases for the ordinal
     * fields — which is exactly how Broker Load reads CSV ({@code CSVScanner} fills source slot
     * <i>j</i> from field <i>j</i>). Declaring the load's own field list therefore makes the
     * sampler's by-name {@code SELECT <key>} land on the same ordinal the load reads that key
     * from. Declaring it also skips FILES's BE-side schema inference, which would otherwise name
     * the fields {@code $1, $2, ...} and leave the sub-query nothing to select.
     *
     * <p>The dialect properties matter for the same reason: a different separator, enclose,
     * escape, trimming, or header skip splits a row into different fields, so the positions the
     * {@code schema} names would no longer be the positions the load reads.
     */
    private static void appendCsvProperties(
            Map<String, String> filesProperties, BrokerLoadScanContext context, SampleRequest request)
            throws StarRocksException {
        List<String> fileFieldNames = resolveSharedCsvFileFields(context);
        filesProperties.put(TableFunctionTable.PROPERTY_SCHEMA,
                buildCsvSchema(fileFieldNames, projectedColumnsByName(request)));
        appendCsvDialect(filesProperties, resolveSharedCsvDialect(context.fileGroups()));
    }

    /**
     * The one positional field layout every file group must share, since one FILES call takes one
     * {@code schema}. A group's layout is its {@code COLUMNS} list when it has one, and the
     * target's default layout otherwise; comparison is case-insensitive because Broker Load's own
     * column names are.
     *
     * <p>Note this is the {@code COLUMNS} list only, never the {@code COLUMNS FROM PATH} names:
     * Broker Load appends path columns after the file's fields, and so does FILES
     * ({@code getSchemaFromPath} runs after the declared schema), so they stay out of the
     * positional list on both sides and are carried by {@code columns_from_path} instead.
     */
    private static List<String> resolveSharedCsvFileFields(BrokerLoadScanContext context)
            throws StarRocksException {
        List<String> shared = null;
        for (BrokerFileGroup fileGroup : context.fileGroups()) {
            List<String> declared = fileGroup.getFileFieldNames();
            // Absent reads as null from a DataDescription-built group and as an empty list from a
            // TableFunctionTable-built one; both mean "no COLUMNS list", so both take the default.
            List<String> layout = declared != null && !declared.isEmpty()
                    ? declared
                    : defaultCsvFileFields(context.targetBaseSchema());
            if (shared == null) {
                shared = layout;
            } else if (!namesEqualIgnoringCase(shared, layout)) {
                throw new StarRocksException(ERROR_PREFIX + "CSV file groups disagree on their column layout: "
                        + shared + " vs " + layout + "; one FILES call declares one positional schema");
            }
        }
        return shared;
    }

    /**
     * The layout Broker Load derives for a file group that declares no {@code COLUMNS} list:
     * {@code Load.initColumns} walks the target's base schema and makes one source field per
     * column, skipping generated and auto-increment columns because neither is read from the
     * file. The CSV scanner then fills those source slots in order, so this list is the file's
     * field order.
     */
    private static List<String> defaultCsvFileFields(List<Column> targetBaseSchema) throws StarRocksException {
        List<String> fileFieldNames = new ArrayList<>();
        for (Column column : targetBaseSchema) {
            if (!column.isGeneratedColumn() && !column.isAutoIncrement()) {
                fileFieldNames.add(column.getName());
            }
        }
        if (fileFieldNames.isEmpty()) {
            throw new StarRocksException(ERROR_PREFIX + "CSV file group declares no COLUMNS list and the scan "
                    + "context carries no target base schema, so its positional field layout is unknown");
        }
        return fileFieldNames;
    }

    /**
     * Renders the FILES {@code schema} string for a positional field layout.
     *
     * <p>A field the sub-query projects is declared with the target column's own name and type,
     * so the projection's identifier matches the declared alias exactly and the BE performs the
     * same text → value conversion the load performs on that slot. Every other field only has to
     * hold its position, so it is declared {@code VARCHAR(65533)}: a string always parses, which
     * keeps a column the sampler never looks at from failing the sub-query.
     */
    private static String buildCsvSchema(List<String> fileFieldNames, Map<String, Column> projectedColumnsByName) {
        StringBuilder schema = new StringBuilder();
        for (String fileFieldName : fileFieldNames) {
            if (schema.length() > 0) {
                schema.append(", ");
            }
            Column projected = projectedColumnsByName.get(fileFieldName);
            String declaredName = projected == null ? fileFieldName : projected.getName();
            String declaredType = projected == null
                    ? StringType.DEFAULT_STRING.toSql()
                    : declaredTypeSql(projected.getType());
            schema.append(SqlUtils.getIdentSql(declaredName)).append(' ').append(declaredType);
        }
        return schema.toString();
    }

    /**
     * A column type as the FILES {@code schema} grammar should read it back. {@code toSql()} emits
     * a bare {@code varchar} / {@code char} for a type whose length was never set, and the schema
     * parser then defaults that to length 1 — which would truncate every sampled value to one
     * character. Widen those to the default string length instead; the sample is decoded against
     * the target column's real type either way.
     */
    private static String declaredTypeSql(Type type) {
        if (type.isScalarType()) {
            ScalarType scalarType = (ScalarType) type;
            if (scalarType.getPrimitiveType().isStringType() && scalarType.getLength() <= 0) {
                return StringType.DEFAULT_STRING.toSql();
            }
        }
        return type.toSql();
    }

    /** Every column the sampling SELECT projects — the sampled keys plus the partition sources. */
    private static Map<String, Column> projectedColumnsByName(SampleRequest request) {
        Map<String, Column> byName = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (Column column : sampledKeyColumns(request)) {
            byName.putIfAbsent(column.getName(), column);
        }
        for (Column column : request.getPartitionSourceColumns()) {
            byName.putIfAbsent(column.getName(), column);
        }
        return byName;
    }

    /** The CSV parsing options of one file group, compared as a unit across groups. */
    private record CsvDialect(String columnSeparator, String rowDelimiter, byte enclose, byte escape,
                              long skipHeader, boolean trimSpace) {
        static CsvDialect of(BrokerFileGroup fileGroup) {
            return new CsvDialect(fileGroup.getColumnSeparator(), fileGroup.getRowDelimiter(),
                    fileGroup.getEnclose(), fileGroup.getEscape(),
                    fileGroup.getSkipHeader(), fileGroup.isTrimspace());
        }
    }

    /** One FILES call parses every file the same way, so every file group must agree. */
    private static CsvDialect resolveSharedCsvDialect(List<BrokerFileGroup> fileGroups) throws StarRocksException {
        CsvDialect shared = null;
        for (BrokerFileGroup fileGroup : fileGroups) {
            CsvDialect dialect = CsvDialect.of(fileGroup);
            if (shared == null) {
                shared = dialect;
            } else if (!shared.equals(dialect)) {
                throw new StarRocksException(ERROR_PREFIX + "CSV file groups disagree on parsing options: "
                        + shared + " vs " + dialect);
            }
        }
        return shared;
    }

    /**
     * Writes the dialect onto the FILES property map. An option left at Broker Load's default is
     * omitted: FILES defaults {@code csv.column_separator} to {@code \t} and
     * {@code csv.row_delimiter} to {@code \n} too, and the remaining options default to "off" on
     * both sides. Delimiters pass through as strings — {@code TableFunctionTable} re-runs
     * {@code Delimiter.convertDelimiter} on them, which is a no-op for an already-decoded
     * delimiter (the same round trip {@code BrokerFileGroup(TableFunctionTable, ...)} makes in the
     * other direction).
     */
    private static void appendCsvDialect(Map<String, String> filesProperties, CsvDialect dialect)
            throws StarRocksException {
        if (dialect.columnSeparator() != null) {
            filesProperties.put(TableFunctionTable.PROPERTY_CSV_COLUMN_SEPARATOR, dialect.columnSeparator());
        }
        if (dialect.rowDelimiter() != null) {
            filesProperties.put(TableFunctionTable.PROPERTY_CSV_ROW_DELIMITER, dialect.rowDelimiter());
        }
        if (dialect.enclose() != 0) {
            filesProperties.put(TableFunctionTable.PROPERTY_CSV_ENCLOSE,
                    asFilesOptionByte(dialect.enclose(), "enclose"));
        }
        if (dialect.escape() != 0) {
            filesProperties.put(TableFunctionTable.PROPERTY_CSV_ESCAPE,
                    asFilesOptionByte(dialect.escape(), "escape"));
        }
        if (dialect.skipHeader() != 0) {
            filesProperties.put(TableFunctionTable.PROPERTY_CSV_SKIP_HEADER, Long.toString(dialect.skipHeader()));
        }
        if (dialect.trimSpace()) {
            filesProperties.put(TableFunctionTable.PROPERTY_CSV_TRIM_SPACE, "true");
        }
    }

    /**
     * Broker Load holds {@code enclose} / {@code escape} as a raw byte while FILES takes them as a
     * string and reads back its first byte. The two agree for ASCII and only for ASCII: a high
     * byte would be re-encoded as a two-byte UTF-8 sequence and FILES would read the lead byte
     * instead, silently splitting fields differently from the load.
     */
    private static String asFilesOptionByte(byte optionByte, String optionName) throws StarRocksException {
        if (optionByte < 0) {
            throw new StarRocksException(String.format(
                    "%sCSV %s byte 0x%02x is outside ASCII and cannot be forwarded to FILES unchanged",
                    ERROR_PREFIX, optionName, optionByte & 0xFF));
        }
        return String.valueOf((char) optionByte);
    }

    private static boolean namesEqualIgnoringCase(List<String> left, List<String> right) {
        if (left.size() != right.size()) {
            return false;
        }
        for (int index = 0; index < left.size(); index++) {
            if (!left.get(index).equalsIgnoreCase(right.get(index))) {
                return false;
            }
        }
        return true;
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
