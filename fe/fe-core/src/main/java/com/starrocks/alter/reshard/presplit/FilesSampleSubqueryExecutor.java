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
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Shared scaffolding for Tier 2 sample sub-query executors that translate the
 * load's source into a {@code SELECT <sort_key> FROM FILES(...)} sub-query.
 * Subclasses provide a {@link Source} (FILES property map plus the load's
 * total input byte total and {@link ComputeResource}); everything else —
 * sampling-rate math, SQL synthesis, BE invocation, JSON row decode — is
 * shared.
 *
 * <p>The sub-query is submitted through {@link SimpleExecutor#executeDQL}
 * with a {@link ConnectContext} pinned to the load's compute resource, so
 * cluster resources, audit logging, and credential redaction are inherited
 * rather than reimplemented. Result rows arrive as JSON
 * {@code {"data":[...]}} envelopes (HTTP_PROTOCAL sink); each row's single
 * column is fed to {@link Variant#of} using the sort-key column's declared
 * type.
 */
abstract class FilesSampleSubqueryExecutor implements SampleSubqueryExecutor {

    private static final String EXECUTOR_NAME = "TabletPreSplitTier2FilesSubquery";

    private static final SimpleExecutor PRODUCTION_SIMPLE_EXECUTOR =
            new SimpleExecutor(EXECUTOR_NAME, TResultSinkType.HTTP_PROTOCAL);

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
     */
    @FunctionalInterface
    interface SampleQueryRunner {
        List<TResultBatch> run(String sampleSql, ComputeResource computeResource) throws StarRocksException;
    }

    /** Subclass-supplied FILES sub-query inputs. */
    protected record Source(
            Map<String, String> filesProperties, long totalFileBytes, ComputeResource computeResource) {
        public Source {
            Objects.requireNonNull(filesProperties, "filesProperties");
            Objects.requireNonNull(computeResource, "computeResource");
            if (totalFileBytes < 0) {
                throw new IllegalArgumentException("totalFileBytes must be non-negative, was " + totalFileBytes);
            }
        }
    }

    private final String errorPrefix;
    private final SampleQueryRunner sampleQueryRunner;

    FilesSampleSubqueryExecutor(String errorPrefix) {
        this(errorPrefix, FilesSampleSubqueryExecutor::runViaSimpleExecutor);
    }

    @VisibleForTesting
    FilesSampleSubqueryExecutor(String errorPrefix, SampleQueryRunner sampleQueryRunner) {
        this.errorPrefix = Objects.requireNonNull(errorPrefix, "errorPrefix");
        this.sampleQueryRunner = Objects.requireNonNull(sampleQueryRunner, "sampleQueryRunner");
    }

    /**
     * Translate the load's scan context into FILES-call inputs the shared
     * orchestration can execute. Implementations throw
     * {@link StarRocksException} for any source shape the FILES sub-query
     * cannot honor.
     */
    protected abstract Source resolveSource(SampleRequest request) throws StarRocksException;

    @Override
    public final SampleExecution execute(SampleRequest request) throws StarRocksException {
        Source source = resolveSource(request);
        List<Column> sortKeyColumns = request.getSortKey();

        double samplingRate = pickSamplingRate(source.totalFileBytes());
        int rowLimit = pickRowLimit(request.getSampleByteLimit());

        String sampleSql = buildSampleSql(
                source.filesProperties(), sortKeyColumns, samplingRate, rowLimit, request.getSeed());
        List<TResultBatch> resultBatches = runSampleQuery(sampleSql, source.computeResource());
        Iterator<List<Variant>> rowIterator = decodeRows(resultBatches, sortKeyColumns).iterator();
        return new SampleExecution(rowIterator, new Estimates(source.totalFileBytes(), 0L));
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
        long rowLimitFromBytes = sampleByteLimit / AVERAGE_ROW_BYTES_ESTIMATE;
        return (int) Math.min(SAMPLE_ROW_HARD_LIMIT, Math.max(1L, rowLimitFromBytes));
    }

    /**
     * Builds the sampling SELECT against the supplied FILES properties. Projects
     * every sort-key column so multi-column sort keys produce tuple-valued
     * samples that {@link BoundaryPlanner} can lex-sort and quantile-cut.
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
            Map<String, String> filesProperties, List<Column> sortKeyColumns,
            double samplingRate, int rowLimit, long seed) {
        String projection = sortKeyColumns.stream()
                .map(column -> SqlUtils.getIdentSql(column.getName()))
                .collect(Collectors.joining(", "));
        String propertiesClause = filesProperties.entrySet().stream()
                .map(property -> '"' + escapeDoubleQuoted(property.getKey()) + "\" = \""
                        + escapeDoubleQuoted(property.getValue()) + '"')
                .collect(Collectors.joining(", "));
        long orderShuffleSeed = seed ^ 0x5A5A5A5A5A5A5A5AL;
        return String.format(
                "SELECT %s FROM FILES(%s) WHERE rand(%d) < %s ORDER BY rand(%d) LIMIT %d",
                projection,
                propertiesClause,
                seed,
                Double.toString(samplingRate),
                orderShuffleSeed,
                rowLimit);
    }

    /**
     * Escapes both backslash and double-quote inside a double-quoted SQL
     * string literal. Backslash MUST be escaped first to avoid double-
     * escaping the slashes inserted by the quote escape. Short-circuits
     * the common case (paths, simple credentials) to avoid two full
     * {@code String.replace} copies of multi-MB property values.
     */
    private static String escapeDoubleQuoted(String value) {
        if (value.indexOf('\\') < 0 && value.indexOf('"') < 0) {
            return value;
        }
        return value.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    private List<TResultBatch> runSampleQuery(String sampleSql, ComputeResource computeResource)
            throws StarRocksException {
        try {
            return sampleQueryRunner.run(sampleSql, computeResource);
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

    private List<List<Variant>> decodeRows(List<TResultBatch> resultBatches, List<Column> sortKeyColumns)
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
                rows.add(decodeRow(rowBuffer, sortKeyColumns));
            }
        }
        return rows;
    }

    /**
     * Decode one HTTP_PROTOCAL JSON row ({@code {"data":[<val0>, ...]}}) into
     * a sort-key tuple. Nullable columns accept JSON nulls and decode to
     * {@link com.starrocks.catalog.NullVariant}; non-nullable columns surface
     * a null cell as an executor failure rather than silently dropping the
     * row. Any shape deviation or type-coerce failure becomes a
     * {@link StarRocksException} so the coordinator records SAMPLE_FAILED
     * instead of letting an unchecked exception unwind the load thread.
     */
    private List<Variant> decodeRow(ByteBuffer rowBuffer, List<Column> sortKeyColumns) throws StarRocksException {
        JsonArray dataArray = extractDataArray(rowBuffer, sortKeyColumns.size());
        List<Variant> tupleValues = new ArrayList<>(sortKeyColumns.size());
        for (int columnIndex = 0; columnIndex < sortKeyColumns.size(); columnIndex++) {
            tupleValues.add(decodeCell(dataArray.get(columnIndex), sortKeyColumns.get(columnIndex)));
        }
        return tupleValues;
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
     * invariant violation.
     */
    private Variant decodeCell(JsonElement valueElement, Column column) throws StarRocksException {
        if (valueElement.isJsonNull()) {
            if (column.isAllowNull()) {
                return Variant.nullVariant(column.getType());
            }
            throw new StarRocksException(errorPrefix + "sample returned a null value for non-nullable sort-key column "
                    + column.getName());
        }
        try {
            return Variant.of(column.getType(), valueElement.getAsString());
        } catch (RuntimeException variantFailure) {
            throw new StarRocksException(errorPrefix + "failed to coerce sample value for column "
                    + column.getName() + " to " + column.getType().toSql() + ": " + variantFailure.getMessage(),
                    variantFailure);
        }
    }
}
