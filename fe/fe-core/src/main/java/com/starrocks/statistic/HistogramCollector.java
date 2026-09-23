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

package com.starrocks.statistic;

import com.starrocks.common.Config;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.thrift.TStatisticData;
import com.starrocks.type.Type;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.starrocks.statistic.HistogramStatisticsUtils.batchInsertPrefixSize;
import static com.starrocks.statistic.HistogramStatisticsUtils.buildBatchInsertPrefix;
import static com.starrocks.statistic.HistogramStatisticsUtils.buildMcvJson;
import static com.starrocks.statistic.HistogramStatisticsUtils.createInsertStmt;
import static com.starrocks.statistic.HistogramStatisticsUtils.utf8Length;

/**
 * Collects a histogram per column: query the column's most common values, query its buckets, then
 * write the rows back as one buffered INSERT ... VALUES per
 * {@link Config#histogram_batch_insert_buffer_size} worth of SQL. Anything that differs between
 * native and external tables comes from the {@link HistogramCollectTraits}.
 *
 * <p>Single use: the buffers are instance state, so one collector serves one collect() call.
 */
final class HistogramCollector {
    private static final Logger LOG = LogManager.getLogger(HistogramCollector.class);

    private final HistogramCollectTraits traits;

    private final List<List<Expr>> rowsBuffer = new ArrayList<>();
    private final List<String> sqlBuffer = new ArrayList<>();
    private final List<String> columnsBuffer = new ArrayList<>();
    private final List<String> insertedColumns = new ArrayList<>();

    HistogramCollector(HistogramCollectTraits traits) {
        this.traits = traits;
    }

    void collect(ConnectContext context, AnalyzeStatus analyzeStatus) throws Exception {
        StatisticsCollectJob job = traits.job;
        List<String> columnNames = job.getColumnNames();
        List<Type> columnTypes = job.getColumnTypes();
        String statsTableName = traits.statsTableName();

        long bufferSize = batchInsertPrefixSize(statsTableName);
        long bufferLimit = Math.max(1, Config.histogram_batch_insert_buffer_size);
        int failedColumns = 0;
        Exception lastFailure = null;

        try {
            for (int i = 0; i < columnNames.size(); i++) {
                String columnName = columnNames.get(i);
                Type columnType = columnTypes.get(i);
                List<Expr> row;
                String rowSql;
                long rowSize;
                try {
                    ColumnHistogram histogram = collectColumn(context, analyzeStatus, columnName, columnType);
                    row = traits.buildInsertRow(columnName, histogram.buckets(), histogram.mcvJson());
                    rowSql = traits.buildInsertRowSql(columnName, histogram.buckets(), histogram.mcvJson());
                    rowSize = utf8Length(rowSql) + (sqlBuffer.isEmpty() ? 0 : 2);
                } catch (Exception collectionFailure) {
                    flushOnCollectionFailure(context, analyzeStatus, columnName, collectionFailure);
                    if (!traits.toleratesColumnFailure()) {
                        throw collectionFailure;
                    }
                    // One column's histogram is independent of every other column's, so the rest of the
                    // job is still worth running and what it produces is still worth keeping. The failures
                    // are reported together once the loop ends.
                    failedColumns++;
                    lastFailure = collectionFailure;
                    LOG.warn("Failed to collect {} for column {}, continuing with the remaining columns ({}/{} " +
                                    "failed so far)", traits.statisticsDescription(), columnName, failedColumns,
                            columnNames.size(), collectionFailure);
                    continue;
                }

                if (!rowsBuffer.isEmpty() && bufferSize + rowSize > bufferLimit) {
                    flush(context, analyzeStatus);
                    bufferSize = batchInsertPrefixSize(statsTableName);
                    rowSize = utf8Length(rowSql);
                }

                rowsBuffer.add(row);
                sqlBuffer.add(rowSql);
                columnsBuffer.add(columnName);
                bufferSize += rowSize;
                if (bufferSize >= bufferLimit) {
                    flush(context, analyzeStatus);
                    bufferSize = batchInsertPrefixSize(statsTableName);
                }

                analyzeStatus.setProgress((i + 1) * 99L / columnNames.size());
                GlobalStateMgr.getCurrentState().getAnalyzeMgr().addAnalyzeStatus(analyzeStatus);
            }

            flush(context, analyzeStatus);
        } finally {
            traits.afterCollection(context, insertedColumns);
        }

        if (lastFailure != null) {
            // Nothing survived: there is no partial result to keep, and reporting success would hide a
            // collection that produced no histogram at all.
            if (insertedColumns.isEmpty()) {
                throw lastFailure;
            }
            // Some columns have a histogram and some do not. The job finishes - what it wrote is correct
            // and usable - but says so, and only the columns it actually wrote get metadata (see
            // collectedColumns, which StatisticExecutor uses when committing ExternalHistogramStatsMeta).
            String message = String.format("collect %s job partially failed but tolerated %d/%d, " +
                    "last error is %s", traits.statisticsDescription(), failedColumns, columnNames.size(),
                    lastFailure);
            analyzeStatus.setReason(message);
            LOG.warn(message);
        }

        analyzeStatus.setProgress(100);
        GlobalStateMgr.getCurrentState().getAnalyzeMgr().addAnalyzeStatus(analyzeStatus);
    }

    /** The columns whose histogram row reached storage; only these should get metadata. */
    List<String> collectedColumns() {
        return insertedColumns;
    }

    /**
     * Queries one column's most common values and buckets, waiting out a backend that is momentarily out
     * of memory when the flavour asks for it. A statistics query is tiny, so such a rejection is about
     * other work on that backend and stops being true on its own - see
     * {@link StatisticsCollectJob#awaitBackendMemory} for why the waiting is budgeted.
     */
    private ColumnHistogram collectColumn(ConnectContext context, AnalyzeStatus analyzeStatus, String columnName,
                                          Type columnType) throws Exception {
        StatisticsCollectJob job = traits.job;
        for (int attempt = 0; ; attempt++) {
            try {
                List<TStatisticData> mcv = job.queryStatisticSync(
                        traits.buildMcvQuery(columnName), context, analyzeStatus);
                Map<String, String> mostCommonValues = traits.buildMostCommonValues(mcv);

                String bucketsQuery = traits.buildBucketsQuery(
                        context, analyzeStatus, columnName, columnType, mostCommonValues);
                String buckets = traits.singleResult(
                        job.queryStatisticSync(bucketsQuery, context, analyzeStatus), columnName).histogram;

                return new ColumnHistogram(buckets, buildMcvJson(mostCommonValues));
            } catch (Exception e) {
                if (!traits.waitsOutProcessMemoryPressure()
                        || attempt >= job.maxProcessMemoryRetries()
                        || !StatisticsCollectJob.isProcessMemoryExhausted(e)) {
                    throw e;
                }
                // A KILL or the overall analyze deadline can land while a query is in flight, where it
                // surfaces as that query's failure. Re-check both before waiting, so neither is mistaken
                // for a busy backend and silently retried.
                job.checkCancelled(analyzeStatus);
                job.calculateAndSetRemainingTimeout(context, analyzeStatus);

                long backoffMs = job.retryBackoffMillis(attempt);
                LOG.info("[ExternalStats] backend out of memory, retrying histogram | table={} column={} " +
                        "attempt={} backoffMs={}", table(), columnName, attempt + 1, backoffMs);
                if (!job.awaitBackendMemory(backoffMs)) {
                    throw e;
                }
            }
        }
    }

    private String table() {
        return traits.table == null ? "?" : traits.table.getName();
    }

    /** One column's collected histogram, before it is turned into a buffered row. */
    private record ColumnHistogram(String buckets, String mcvJson) {
    }

    /**
     * Best-effort flush of the rows collected so far, so a failure on one column does not discard
     * the columns already computed. The collection failure stays the primary exception.
     */
    private void flushOnCollectionFailure(ConnectContext context, AnalyzeStatus analyzeStatus, String columnName,
                                          Exception collectionFailure) {
        try {
            flush(context, analyzeStatus);
        } catch (Exception flushFailure) {
            if (flushFailure != collectionFailure) {
                collectionFailure.addSuppressed(flushFailure);
            }
            LOG.warn("Failed to flush buffered {} statistics after collection failed for column {}",
                    traits.statisticsDescription(), columnName, flushFailure);
        }
    }

    private void flush(ConnectContext context, AnalyzeStatus analyzeStatus) throws Exception {
        if (rowsBuffer.isEmpty()) {
            return;
        }

        String statsTableName = traits.statsTableName();
        String sql = buildBatchInsertPrefix(statsTableName) + String.join(", ", sqlBuffer) + ";";
        traits.job.collectStatisticSync(() -> createInsertStmt(statsTableName, rowsBuffer, sql),
                context, analyzeStatus);
        insertedColumns.addAll(columnsBuffer);
        rowsBuffer.clear();
        sqlBuffer.clear();
        columnsBuffer.clear();
    }
}
