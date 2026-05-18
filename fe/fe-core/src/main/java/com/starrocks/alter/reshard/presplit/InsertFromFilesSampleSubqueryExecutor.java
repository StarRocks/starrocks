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
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.TableFunctionTable;
import com.starrocks.catalog.Variant;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.util.SqlUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SimpleExecutor;
import com.starrocks.statistic.StatisticUtils;
import com.starrocks.thrift.TBrokerFileStatus;
import com.starrocks.thrift.TResultBatch;
import com.starrocks.thrift.TResultSinkType;
import com.starrocks.type.Type;
import com.starrocks.warehouse.cngroup.ComputeResource;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Production Tier 2 {@link SampleSubqueryExecutor} for the INSERT-from-FILES
 * path. Issues a SELECT against the same {@code FILES(...)} source the load
 * will scan, with a Bernoulli predicate on the FE-supplied seed, and decodes
 * the resulting rows into sort-key {@link Variant} values.
 *
 * <p>The sub-query reaches BE coordinator scheduling via the same
 * {@link SimpleExecutor} path internal tooling already relies on, so cluster
 * resources, audit logging, and credential redaction are inherited rather
 * than reimplemented. Result rows arrive as JSON {@code {"data":[...]}}
 * envelopes (the HTTP_PROTOCAL sink); each row's single column is fed
 * to {@link Variant#of} using the sort-key column's declared type.
 *
 * <p>Sampling rate is picked from the FE-known input byte total: the executor
 * targets {@link #TARGET_SAMPLE_ROW_COUNT} rows assuming a coarse
 * {@link #AVERAGE_ROW_BYTES_ESTIMATE}. A SQL-side {@code LIMIT} caps actual
 * returns so a rate over-estimate (small files, narrow rows) cannot blow up
 * FE memory; the byte cap inside {@link ReservoirSampler} enforces the soft
 * memory bound a second time on the consumer side.
 *
 * <p>The Broker Load load kind is intentionally NOT covered here — Broker
 * Load's source is built programmatically from {@code BrokerDesc} and is
 * not addressable via a {@code FILES(...)} table function (broker-backed
 * sources go through brokers that the {@code FILES} path does not invoke).
 * That path keeps using {@link PendingSampleSubqueryExecutor} until a future
 * commit lands a Broker-Load-specific sampler.
 */
final class InsertFromFilesSampleSubqueryExecutor implements SampleSubqueryExecutor {

    private static final String EXECUTOR_NAME = "TabletPreSplitTier2InsertFromFiles";

    private static final SimpleExecutor PRODUCTION_SIMPLE_EXECUTOR =
            new SimpleExecutor(EXECUTOR_NAME, TResultSinkType.HTTP_PROTOCAL);

    private static final String ERROR_PREFIX = "INSERT-from-FILES Tier 2 ";

    /** Soft target row count the sampling rate is sized to deliver. */
    private static final int TARGET_SAMPLE_ROW_COUNT = 50_000;

    /**
     * Coarse FE-side estimate of an average input row's serialized byte size,
     * used only to convert the FE-known input byte total into a sampling rate
     * targeting {@link #TARGET_SAMPLE_ROW_COUNT} rows. Wrong by a constant
     * factor is fine — the SQL {@code LIMIT} caps over-sampling and the byte
     * cap in {@link ReservoirSampler} bounds FE memory regardless.
     */
    private static final long AVERAGE_ROW_BYTES_ESTIMATE = 256L;

    /**
     * Hard ceiling on rows returned to FE, in addition to the rate-derived
     * Bernoulli filter. Two over-shoot paths are guarded: a small input
     * pushes the rate to ~1.0, and a narrow-column input delivers more rows
     * than expected at a given rate. The 4× factor is large enough to absorb
     * both without truncating an unbiased sample at the cap.
     */
    private static final int SAMPLE_ROW_HARD_LIMIT = TARGET_SAMPLE_ROW_COUNT * 4;

    /**
     * Submits a built sampling SELECT under the supplied {@link ComputeResource}
     * and returns the BE-side result batches. Constructor-injected to keep the
     * executor unit-testable without bringing up the full FE planner/coordinator
     * stack — production wires this to {@link SimpleExecutor#executeDQL} via a
     * scoped {@link ConnectContext} carrying the load's compute resource.
     */
    @FunctionalInterface
    interface SampleQueryRunner {
        List<TResultBatch> run(String sampleSql, ComputeResource computeResource) throws StarRocksException;
    }

    private final SampleQueryRunner sampleQueryRunner;

    InsertFromFilesSampleSubqueryExecutor() {
        this(InsertFromFilesSampleSubqueryExecutor::runViaSimpleExecutor);
    }

    @VisibleForTesting
    InsertFromFilesSampleSubqueryExecutor(SampleQueryRunner sampleQueryRunner) {
        this.sampleQueryRunner = sampleQueryRunner;
    }

    @Override
    public SampleExecution execute(SampleRequest request) throws StarRocksException {
        InsertFromFilesScanContext context = requireInsertFromFilesContext(request);
        rejectCompositeSortKey(request);
        TableFunctionTable sourceTable = context.sourceTable();
        Column sortKeyColumn = request.getSortKey().get(0);

        long totalFileBytes = sumFileBytes(sourceTable.loadFileList());
        double samplingRate = pickSamplingRate(totalFileBytes);
        int rowLimit = pickRowLimit(request.getSampleByteLimit());

        String sampleSql = buildSampleSql(
                sourceTable, sortKeyColumn, samplingRate, rowLimit, request.getSeed());
        List<TResultBatch> resultBatches = runSampleQuery(sampleSql, context.computeResource());
        Iterator<List<Variant>> rowIterator = decodeRowsAsSingleColumn(resultBatches, sortKeyColumn.getType()).iterator();
        return new SampleExecution(rowIterator, new Estimates(totalFileBytes, 0L));
    }

    private static InsertFromFilesScanContext requireInsertFromFilesContext(SampleRequest request)
            throws StarRocksException {
        ScanContext scanContext = request.getScanContext();
        if (!(scanContext instanceof InsertFromFilesScanContext insertFromFilesContext)) {
            throw new StarRocksException(ERROR_PREFIX + "received a "
                    + scanContext.getClass().getSimpleName() + " — wire only the INSERT-from-FILES load kind here");
        }
        return insertFromFilesContext;
    }

    /**
     * Tier 1's {@code ParquetMetadataSampler.rejectCompositeSortKey} throws
     * {@link Tier1UnavailableException}, which falls back to this Tier 2
     * executor. The current sub-query shape projects a single column, so
     * composite sort keys are not yet supported here either — we surface a
     * clean StarRocksException (mapped to SkipReason.SAMPLE_FAILED) rather
     * than letting the multi-column row decode throw later.
     */
    private static void rejectCompositeSortKey(SampleRequest request) throws StarRocksException {
        if (request.getSortKey().size() > 1) {
            throw new StarRocksException(ERROR_PREFIX
                    + "composite sort keys are not yet supported (size=" + request.getSortKey().size() + ")");
        }
    }

    private static long sumFileBytes(List<TBrokerFileStatus> fileStatuses) {
        long total = 0L;
        for (TBrokerFileStatus fileStatus : fileStatuses) {
            if (!fileStatus.isDir) {
                total += fileStatus.size;
            }
        }
        return total;
    }

    private static double pickSamplingRate(long totalFileBytes) {
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
    private static int pickRowLimit(long sampleByteLimit) {
        long bytesDerivedLimit = sampleByteLimit / AVERAGE_ROW_BYTES_ESTIMATE;
        return (int) Math.min(SAMPLE_ROW_HARD_LIMIT, Math.max(1L, bytesDerivedLimit));
    }

    /**
     * Re-issues the load's original {@code FILES(...)} properties verbatim so
     * the BE scan covers the same files the load will scan. The sort-key
     * column is the only projection (composite sort keys are rejected
     * upstream by {@link #rejectCompositeSortKey}).
     *
     * <p>String literal escaping covers both {@code "} and {@code \} so a
     * crafted property value cannot break out of the double-quoted form and
     * inject SQL into the internal-context sub-query. The {@code ORDER BY
     * rand(seed XOR 0x5...)} before {@code LIMIT} re-shuffles the
     * {@code WHERE}-survivors so an over-rate truncation does not bias
     * toward earlier files in scan order.
     */
    @VisibleForTesting
    static String buildSampleSql(
            TableFunctionTable sourceTable, Column sortKeyColumn,
            double samplingRate, int rowLimit, long seed) {
        String propertiesClause = sourceTable.getProperties().entrySet().stream()
                .map(property -> '"' + escapeDoubleQuoted(property.getKey()) + "\" = \""
                        + escapeDoubleQuoted(property.getValue()) + '"')
                .collect(Collectors.joining(", "));
        long orderShuffleSeed = seed ^ 0x5A5A5A5A5A5A5A5AL;
        return String.format(
                "SELECT %s FROM FILES(%s) WHERE rand(%d) < %s ORDER BY rand(%d) LIMIT %d",
                SqlUtils.getIdentSql(sortKeyColumn.getName()),
                propertiesClause,
                seed,
                Double.toString(samplingRate),
                orderShuffleSeed,
                rowLimit);
    }

    /**
     * Escapes both backslash and double-quote inside a double-quoted SQL
     * string literal. Backslash MUST be escaped first to avoid double-
     * escaping the slashes inserted by the quote escape.
     */
    private static String escapeDoubleQuoted(String value) {
        return value.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    private List<TResultBatch> runSampleQuery(String sampleSql, ComputeResource computeResource)
            throws StarRocksException {
        try {
            return sampleQueryRunner.run(sampleSql, computeResource);
        } catch (RuntimeException runtimeFailure) {
            throw new StarRocksException(
                    ERROR_PREFIX + "sample sub-query failed: " + runtimeFailure.getMessage(), runtimeFailure);
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
    private static List<TResultBatch> runViaSimpleExecutor(String sampleSql, ComputeResource computeResource) {
        ConnectContext priorContext = ConnectContext.get();
        ConnectContext sampleContext = configureSampleContext(StatisticUtils.buildConnectContext(), computeResource);
        sampleContext.setThreadLocalInfo();
        try {
            return PRODUCTION_SIMPLE_EXECUTOR.executeDQL(sampleSql, sampleContext);
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
     */
    @VisibleForTesting
    static ConnectContext configureSampleContext(ConnectContext context, ComputeResource computeResource) {
        context.setCurrentWarehouseId(computeResource.getWarehouseId());
        context.setCurrentComputeResource(computeResource);
        context.setNeedQueued(false);
        context.setStartTime();
        return context;
    }

    private static List<List<Variant>> decodeRowsAsSingleColumn(List<TResultBatch> resultBatches, Type valueType)
            throws StarRocksException {
        List<List<Variant>> rows = new ArrayList<>();
        if (resultBatches == null) {
            return rows;
        }
        for (TResultBatch resultBatch : resultBatches) {
            List<ByteBuffer> batchRows = resultBatch.getRows();
            if (batchRows == null) {
                continue;
            }
            for (ByteBuffer rowBuffer : batchRows) {
                rows.add(decodeSingleColumnRow(rowBuffer, valueType));
            }
        }
        return rows;
    }

    /**
     * Each row arrives as a UTF-8 JSON document of shape
     * {@code {"data":[<single value>],"meta":[...]}}; we read the lone
     * {@code data} cell and feed it to {@link Variant#of}. A null cell would
     * imply a null in the sort key — sort-key columns reject nulls upstream,
     * so we surface this as an executor failure rather than silently dropping
     * the row. Any shape deviation (non-object root, missing/non-array
     * {@code data}, wrong arity, type-coerce failure) becomes a
     * {@link StarRocksException} so the coordinator records SAMPLE_FAILED
     * instead of letting an unchecked exception unwind the load thread.
     */
    private static List<Variant> decodeSingleColumnRow(ByteBuffer rowBuffer, Type valueType) throws StarRocksException {
        String jsonRow = StandardCharsets.UTF_8.decode(rowBuffer.duplicate()).toString();
        JsonElement root;
        try {
            root = JsonParser.parseString(jsonRow);
        } catch (RuntimeException parseFailure) {
            throw new StarRocksException(
                    ERROR_PREFIX + "row decode failed: " + parseFailure.getMessage(), parseFailure);
        }
        if (!root.isJsonObject()) {
            throw new StarRocksException(ERROR_PREFIX + "row root is not a JSON object: " + jsonRow);
        }
        JsonObject rootObject = root.getAsJsonObject();
        JsonElement dataElement = rootObject.get("data");
        if (dataElement == null || !dataElement.isJsonArray()) {
            throw new StarRocksException(ERROR_PREFIX + "row is missing a JSON array `data` field: " + jsonRow);
        }
        JsonArray dataArray = dataElement.getAsJsonArray();
        if (dataArray.size() != 1) {
            throw new StarRocksException(ERROR_PREFIX
                    + "expected one projected column per row but row carried " + dataArray.size());
        }
        JsonElement valueElement = dataArray.get(0);
        if (valueElement.isJsonNull()) {
            throw new StarRocksException(
                    ERROR_PREFIX + "sample returned a null sort-key value — sort keys must be non-null");
        }
        try {
            return List.of(Variant.of(valueType, valueElement.getAsString()));
        } catch (RuntimeException variantFailure) {
            throw new StarRocksException(ERROR_PREFIX + "failed to coerce sample value to "
                    + valueType.toSql() + ": " + variantFailure.getMessage(), variantFailure);
        }
    }
}
