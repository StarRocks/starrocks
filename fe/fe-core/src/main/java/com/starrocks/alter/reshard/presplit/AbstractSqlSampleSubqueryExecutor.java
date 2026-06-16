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
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Variant;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.util.SqlUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SimpleExecutor;
import com.starrocks.statistic.StatisticUtils;
import com.starrocks.thrift.TResultBatch;
import com.starrocks.thrift.TResultSinkType;
import com.starrocks.warehouse.cngroup.ComputeResource;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Shared scaffolding for data-tier sample sub-query executors that synthesize a
 * {@code SELECT <sort_key> FROM <source>} sub-query, submit it through
 * {@link SimpleExecutor#executeDQL} on the load's compute resource, and decode
 * the JSON result rows into {@link SampleRow} values.
 *
 * <p>Subclasses implement {@link #resolveSampleSpec} to supply the FROM clause
 * SQL, an optional WHERE predicate, the total input byte count, the compute
 * resource, and the projected column identifier lists. Everything else —
 * sampling-rate math, SQL synthesis, BE invocation, JSON row decode — is shared
 * here.
 */
abstract class AbstractSqlSampleSubqueryExecutor implements SampleSubqueryExecutor {

    /** Soft target row count the sampling rate is sized to deliver. */
    static final int TARGET_SAMPLE_ROW_COUNT = 50_000;

    /**
     * Coarse FE-side estimate of an average input row's serialized byte size,
     * used only to convert the FE-known input byte total into a sampling rate
     * targeting {@link #TARGET_SAMPLE_ROW_COUNT} rows. Wrong by a constant
     * factor is fine — the SQL {@code LIMIT} caps over-sampling and the byte
     * cap in {@link ReservoirSampler} bounds FE memory regardless.
     */
    static final long AVERAGE_ROW_BYTES_ESTIMATE = 256L;

    /**
     * Hard ceiling on rows returned to FE, in addition to the rate-derived
     * Bernoulli filter. Two over-shoot paths are guarded: a small input
     * pushes the rate to ~1.0, and a narrow-column input delivers more rows
     * than expected at a given rate. The 4× factor is large enough to absorb
     * both without truncating an unbiased sample at the cap.
     */
    static final int SAMPLE_ROW_HARD_LIMIT = TARGET_SAMPLE_ROW_COUNT * 4;

    /**
     * Submits a built sampling SELECT under the supplied {@link ComputeResource}
     * and returns the BE-side result batches. Constructor-injected to keep the
     * executor unit-testable without bringing up the full FE planner/coordinator
     * stack — production wires this to {@link SimpleExecutor#executeDQL} via a
     * scoped {@link ConnectContext} carrying the load's compute resource.
     *
     * <p>{@code queryTimeoutSeconds} caps the sub-query's wall-clock runtime
     * ({@code 0} = uncapped); the production runner applies it via
     * {@code query_timeout} on the sample context so an over-budget sample is
     * cancelled by the BE. Test stubs return canned batches and ignore it.
     */
    @FunctionalInterface
    interface SampleQueryRunner {
        List<TResultBatch> run(String sampleSql, ComputeResource computeResource, int queryTimeoutSeconds)
                throws StarRocksException;
    }

    /**
     * Inputs the template needs from a concrete subclass. The FROM clause SQL
     * and optional WHERE predicate are expressed as raw SQL fragments; the byte
     * total and compute resource drive sampling-rate computation and sub-query
     * routing; the projected identifier lists define the SELECT projection in
     * sort-key order followed by partition-source order.
     *
     * <p>The projection ident lists and the column lists are paired
     * POSITIONALLY: {@code sortKeyProjectionIdents.get(i)} is the SQL the SELECT
     * emits for {@code sortKeyColumns.get(i)}, and likewise for the partition
     * slice. Bundling both in the spec keeps the SELECT projection and the JSON
     * decode in lock-step even when a subclass remaps which source columns back
     * a target column, so the two halves cannot silently desync. Each ident must
     * already be backtick-quoted (e.g. via {@link SqlUtils#getIdentSql}).
     */
    protected record SampleSpec(
            String fromClauseSql,
            String whereClauseSqlOrNull,
            long totalInputBytes,
            ComputeResource computeResource,
            List<String> sortKeyProjectionIdents,
            List<String> partitionProjectionIdents,
            List<Column> sortKeyColumns,
            List<Column> partitionSourceColumns) {
        public SampleSpec {
            Objects.requireNonNull(fromClauseSql, "fromClauseSql");
            Objects.requireNonNull(computeResource, "computeResource");
            Objects.requireNonNull(sortKeyProjectionIdents, "sortKeyProjectionIdents");
            Objects.requireNonNull(partitionProjectionIdents, "partitionProjectionIdents");
            Objects.requireNonNull(sortKeyColumns, "sortKeyColumns");
            Objects.requireNonNull(partitionSourceColumns, "partitionSourceColumns");
            if (totalInputBytes < 0) {
                throw new IllegalArgumentException("totalInputBytes must be non-negative, was " + totalInputBytes);
            }
        }
    }

    private final String errorPrefix;
    private final SampleQueryRunner sampleQueryRunner;

    AbstractSqlSampleSubqueryExecutor(String errorPrefix, String executorName) {
        this(errorPrefix, defaultRunner(executorName));
    }

    @VisibleForTesting
    AbstractSqlSampleSubqueryExecutor(String errorPrefix, SampleQueryRunner sampleQueryRunner) {
        this.errorPrefix = Objects.requireNonNull(errorPrefix, "errorPrefix");
        this.sampleQueryRunner = Objects.requireNonNull(sampleQueryRunner, "sampleQueryRunner");
    }

    /**
     * Builds the production sub-query runner backed by a {@link SimpleExecutor}
     * named {@code executorName}, so each load path's sample sub-query carries
     * its own audit / profile attribution.
     */
    private static SampleQueryRunner defaultRunner(String executorName) {
        Objects.requireNonNull(executorName, "executorName");
        SimpleExecutor simpleExecutor = new SimpleExecutor(executorName, TResultSinkType.HTTP_PROTOCAL);
        return (sampleSql, computeResource, queryTimeoutSeconds) ->
                runViaSimpleExecutor(simpleExecutor, sampleSql, computeResource, queryTimeoutSeconds);
    }

    /**
     * Translate the load's scan context into the FROM clause, optional WHERE
     * predicate, byte total, compute resource, and projected identifier lists
     * needed to synthesize and execute the sampling SELECT. Implementations
     * throw {@link StarRocksException} for any source shape they cannot handle.
     */
    protected abstract SampleSpec resolveSampleSpec(SampleRequest request) throws StarRocksException;

    @Override
    public final SampleExecution execute(SampleRequest request) throws StarRocksException {
        SampleSpec spec = resolveSampleSpec(request);
        double samplingRate = pickSamplingRate(spec.totalInputBytes());
        int rowLimit = pickRowLimit(request.getSampleByteLimit());
        String sampleSql = buildSampleSql(
                spec.fromClauseSql(), spec.whereClauseSqlOrNull(),
                spec.sortKeyProjectionIdents(), spec.partitionProjectionIdents(),
                samplingRate, rowLimit, request.getSeed());
        List<TResultBatch> resultBatches = runSampleQuery(
                sampleSql, spec.computeResource(), request.getQueryTimeoutSeconds());
        Iterator<SampleRow> rowIterator =
                decodeRows(resultBatches, spec.sortKeyColumns(), spec.partitionSourceColumns()).iterator();
        return new SampleExecution(rowIterator, new Estimates(spec.totalInputBytes(), 0L));
    }

    static double pickSamplingRate(long totalFileBytes) {
        if (totalFileBytes <= 0L) {
            return 1.0;
        }
        double rate = (double) TARGET_SAMPLE_ROW_COUNT * AVERAGE_ROW_BYTES_ESTIMATE / totalFileBytes;
        return Math.min(1.0, rate);
    }

    /**
     * Caps the SQL-side {@code LIMIT} at whichever is smaller: the per-feature
     * hard cap, or the FE-memory cap implied by {@code sampleByteLimit}. The
     * byte cap matters because {@code SimpleExecutor.executeDQL} materializes
     * every result batch on the FE heap before this executor decodes — without
     * this cap, a rate over-estimate against narrow rows can balloon FE memory
     * before {@link ReservoirSampler}'s soft byte cap engages.
     */
    static int pickRowLimit(long sampleByteLimit) {
        long rowLimitFromBytes = sampleByteLimit / AVERAGE_ROW_BYTES_ESTIMATE;
        return (int) Math.min(SAMPLE_ROW_HARD_LIMIT, Math.max(1L, rowLimitFromBytes));
    }

    /**
     * Builds the sampling SELECT from the supplied FROM clause, optional WHERE
     * predicate, projected identifier lists, sampling rate, row limit, and seed.
     * Projects every sort-key identifier followed by every partition-source
     * identifier. The {@code ORDER BY rand(seed XOR 0x5...)} before {@code LIMIT}
     * re-shuffles the {@code WHERE}-survivors so an over-rate truncation does not
     * bias toward earlier files in scan order. When {@code whereClauseSqlOrNull}
     * is null, only the Bernoulli rand filter is emitted in the WHERE clause.
     */
    @VisibleForTesting
    static String buildSampleSql(
            String fromClauseSql, String whereClauseSqlOrNull,
            List<String> sortKeyProjectionIdents, List<String> partitionProjectionIdents,
            double samplingRate, int rowLimit, long seed) {
        List<String> projected = new ArrayList<>(
                sortKeyProjectionIdents.size() + partitionProjectionIdents.size());
        projected.addAll(sortKeyProjectionIdents);
        projected.addAll(partitionProjectionIdents);
        String projection = String.join(", ", projected);
        long orderShuffleSeed = seed ^ 0x5A5A5A5A5A5A5A5AL;
        String randFilter = "rand(" + seed + ") < " + Double.toString(samplingRate);
        String whereClause = whereClauseSqlOrNull == null
                ? randFilter
                : "(" + whereClauseSqlOrNull + ") AND " + randFilter;
        return String.format(
                "SELECT %s FROM %s WHERE %s ORDER BY rand(%d) LIMIT %d",
                projection, fromClauseSql, whereClause, orderShuffleSeed, rowLimit);
    }

    private List<TResultBatch> runSampleQuery(
            String sampleSql, ComputeResource computeResource, int queryTimeoutSeconds) throws StarRocksException {
        try {
            return sampleQueryRunner.run(sampleSql, computeResource, queryTimeoutSeconds);
        } catch (RuntimeException runtimeFailure) {
            throw new StarRocksException(
                    errorPrefix + "sample sub-query failed: " + runtimeFailure.getMessage(), runtimeFailure);
        }
    }

    /**
     * Production sub-query runner: builds a {@link ConnectContext} pinned to
     * the load's {@link ComputeResource} so the sample executes on the same
     * warehouse the user load will use, not the statistics-default one
     * {@link SimpleExecutor#executeDQL(String)} would build implicitly. Thread-
     * local context handling follows the same prior-context save/restore
     * pattern {@code SimpleExecutor.executeDQL(String)} uses internally.
     */
    private static List<TResultBatch> runViaSimpleExecutor(
            SimpleExecutor simpleExecutor, String sampleSql, ComputeResource computeResource,
            int queryTimeoutSeconds) {
        ConnectContext priorContext = ConnectContext.get();
        ConnectContext sampleContext = configureSampleContext(
                StatisticUtils.buildConnectContext(), computeResource, queryTimeoutSeconds);
        sampleContext.setThreadLocalInfo();
        try {
            return simpleExecutor.executeDQL(sampleSql, sampleContext);
        } finally {
            ConnectContext.remove();
            if (priorContext != null) {
                priorContext.setThreadLocalInfo();
            }
        }
    }

    /**
     * Aligns a freshly built {@link ConnectContext} with the load's compute
     * resource. The warehouse id MUST be set before
     * {@code setCurrentComputeResource}: {@code ConnectContext.getCurrentComputeResource}
     * discards and re-acquires the resource if its {@code getWarehouseId()}
     * disagrees with the context's {@code currentWarehouseId}, so omitting
     * the warehouse-id alignment silently routes the sub-query to the
     * statistics-default warehouse instead of the load's.
     *
     * <p>{@code queryTimeoutSeconds > 0} sets {@code query_timeout} on the
     * sample session so the BE cancels an over-budget sample (the data-tier
     * pipeline derives this from the remaining pre-submit budget); {@code 0}
     * leaves the built context's default timeout untouched. This is the seam
     * that makes the pre-submit deadline hard — {@code executeDQL} reads the
     * cap from {@code SessionVariable.toThrift()}, not from any SQL hint.
     */
    @VisibleForTesting
    static ConnectContext configureSampleContext(
            ConnectContext context, ComputeResource computeResource, int queryTimeoutSeconds) {
        // setCurrentWarehouseId delegates to setCurrentWarehouse, which REPLACES the session-variable
        // object with a fresh warehouse-defaulted one (re-applying only tracked SET variables). The
        // pre-submit-budget query_timeout is applied via a direct setter (not a tracked SET), so it
        // would be dropped if set before the switch — apply it AFTER, on the final session variable,
        // or an over-budget sample runs to the warehouse/default timeout and blocks the load past the
        // pre-submit budget instead of failing fast and falling back.
        context.setCurrentWarehouseId(computeResource.getWarehouseId());
        context.setCurrentComputeResource(computeResource);
        if (queryTimeoutSeconds > 0) {
            context.getSessionVariable().setQueryTimeoutS(queryTimeoutSeconds);
        }
        context.setNeedQueued(false);
        context.setStartTime();
        return context;
    }

    private List<SampleRow> decodeRows(
            List<TResultBatch> resultBatches,
            List<Column> sortKeyColumns,
            List<Column> partitionSourceColumns) throws StarRocksException {
        List<SampleRow> rows = new ArrayList<>();
        if (resultBatches == null) {
            return rows;
        }
        for (TResultBatch resultBatch : resultBatches) {
            List<ByteBuffer> batchRows = resultBatch.getRows();
            if (batchRows == null) {
                continue;
            }
            for (ByteBuffer rowBuffer : batchRows) {
                rows.add(decodeRow(rowBuffer, sortKeyColumns, partitionSourceColumns));
            }
        }
        return rows;
    }

    /**
     * Decode one HTTP_PROTOCAL JSON row ({@code {"data":[<val0>, ...]}}) into a
     * {@link SampleRow}. The JSON array carries
     * {@code sortKeyColumns.size() + partitionSourceColumns.size()} cells in
     * projection order: the first slice fills the row's sort-key tuple, the
     * trailing slice fills its partition-source tuple. When
     * {@code partitionSourceColumns} is empty the trailing slice is empty and
     * the row collapses to the pre-extension single-tuple shape.
     *
     * <p>Nullable columns accept JSON nulls and decode to
     * {@link com.starrocks.catalog.NullVariant}; non-nullable columns surface a
     * null cell as an executor failure rather than silently dropping the row.
     * Any shape deviation or type-coerce failure becomes a
     * {@link StarRocksException} so the coordinator records SAMPLE_FAILED
     * instead of letting an unchecked exception unwind the load thread.
     */
    private SampleRow decodeRow(
            ByteBuffer rowBuffer,
            List<Column> sortKeyColumns,
            List<Column> partitionSourceColumns) throws StarRocksException {
        int expectedArity = sortKeyColumns.size() + partitionSourceColumns.size();
        JsonArray dataArray = extractDataArray(rowBuffer, expectedArity);
        List<Variant> sortKeyValues = new ArrayList<>(sortKeyColumns.size());
        for (int columnIndex = 0; columnIndex < sortKeyColumns.size(); columnIndex++) {
            sortKeyValues.add(decodeCell(
                    dataArray.get(columnIndex), sortKeyColumns.get(columnIndex), COLUMN_ROLE_SORT_KEY));
        }
        List<Variant> partitionSourceValues = new ArrayList<>(partitionSourceColumns.size());
        for (int columnIndex = 0; columnIndex < partitionSourceColumns.size(); columnIndex++) {
            partitionSourceValues.add(decodeCell(
                    dataArray.get(sortKeyColumns.size() + columnIndex),
                    partitionSourceColumns.get(columnIndex),
                    COLUMN_ROLE_PARTITION_SOURCE));
        }
        return new SampleRow(sortKeyValues, partitionSourceValues);
    }

    /**
     * Parse {@code rowBuffer} as JSON and return its {@code data} array,
     * validating shape (non-empty UTF-8, root is JSON object, has a
     * {@code data} array, arity matches the expected projection count).
     */
    private JsonArray extractDataArray(ByteBuffer rowBuffer, int expectedArity) throws StarRocksException {
        String jsonRow = StandardCharsets.UTF_8.decode(rowBuffer.duplicate()).toString();
        JsonElement root;
        try {
            root = JsonParser.parseString(jsonRow);
        } catch (RuntimeException parseFailure) {
            throw new StarRocksException(
                    errorPrefix + "row decode failed: " + parseFailure.getMessage(), parseFailure);
        }
        if (!root.isJsonObject()) {
            throw new StarRocksException(errorPrefix + "row root is not a JSON object: " + jsonRow);
        }
        JsonElement dataElement = root.getAsJsonObject().get("data");
        if (dataElement == null || !dataElement.isJsonArray()) {
            throw new StarRocksException(errorPrefix + "row is missing a JSON array `data` field: " + jsonRow);
        }
        JsonArray dataArray = dataElement.getAsJsonArray();
        if (dataArray.size() != expectedArity) {
            throw new StarRocksException(errorPrefix + "expected " + expectedArity
                    + " projected column(s) per row but row carried " + dataArray.size());
        }
        return dataArray;
    }

    /**
     * Coerce one JSON cell into a {@link Variant} of {@code column}'s declared
     * type. Null cells produce a typed {@link Variant#nullVariant} for nullable
     * columns (BoundaryPlanner's compareTo orders {@code NullVariant} lower
     * than any non-null value); non-nullable columns reject null as a schema
     * invariant violation. {@code columnRole} names the projection slice the
     * cell belongs to (sort-key or partition-source) so a non-null violation
     * reports the schema an operator should actually inspect.
     */
    private Variant decodeCell(JsonElement valueElement, Column column, String columnRole) throws StarRocksException {
        if (valueElement.isJsonNull()) {
            if (column.isAllowNull()) {
                return Variant.nullVariant(column.getType());
            }
            throw new StarRocksException(errorPrefix + "sample returned a null value for non-nullable "
                    + columnRole + " column " + column.getName());
        }
        try {
            return Variant.of(column.getType(), valueElement.getAsString());
        } catch (RuntimeException variantFailure) {
            throw new StarRocksException(errorPrefix + "failed to coerce sample value for " + columnRole
                    + " column " + column.getName() + " to " + column.getType().toSql() + ": "
                    + variantFailure.getMessage(),
                    variantFailure);
        }
    }

    private static final String COLUMN_ROLE_SORT_KEY = "sort-key";
    private static final String COLUMN_ROLE_PARTITION_SOURCE = "partition-source";

    /**
     * Converts a list of raw column names into backtick-quoted SQL identifiers
     * suitable for embedding in a SELECT projection.
     */
    protected static List<String> identsOf(List<String> columnNames) {
        return columnNames.stream().map(SqlUtils::getIdentSql).collect(Collectors.toList());
    }
}
