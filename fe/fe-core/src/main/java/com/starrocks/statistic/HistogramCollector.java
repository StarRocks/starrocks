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

        try {
            for (int i = 0; i < columnNames.size(); i++) {
                String columnName = columnNames.get(i);
                Type columnType = columnTypes.get(i);
                List<Expr> row;
                String rowSql;
                long rowSize;
                try {
                    List<TStatisticData> mcv = job.queryStatisticSync(
                            traits.buildMcvQuery(columnName), context, analyzeStatus);
                    Map<String, String> mostCommonValues = traits.buildMostCommonValues(mcv);

                    String bucketsQuery = traits.buildBucketsQuery(
                            context, analyzeStatus, columnName, columnType, mostCommonValues);
                    String buckets = traits.singleResult(
                            job.queryStatisticSync(bucketsQuery, context, analyzeStatus), columnName).histogram;

                    String mcvJson = buildMcvJson(mostCommonValues);
                    row = traits.buildInsertRow(columnName, buckets, mcvJson);
                    rowSql = traits.buildInsertRowSql(columnName, buckets, mcvJson);
                    rowSize = utf8Length(rowSql) + (sqlBuffer.isEmpty() ? 0 : 2);
                } catch (Exception collectionFailure) {
                    flushOnCollectionFailure(context, analyzeStatus, columnName, collectionFailure);
                    throw collectionFailure;
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

        analyzeStatus.setProgress(100);
        GlobalStateMgr.getCurrentState().getAnalyzeMgr().addAnalyzeStatus(analyzeStatus);
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
