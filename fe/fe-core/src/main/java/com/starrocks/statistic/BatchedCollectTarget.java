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

import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.type.Type;

import java.util.List;
import java.util.Map;

/**
 * The job-specific half of the batched (query the buckets, then INSERT ... VALUES) strategy.
 * @see BatchedHistogramCollector
 */
interface BatchedCollectTarget extends HistogramCollectTarget {
    /** Statistics table the buffered rows are inserted into. */
    String statsTableName();

    /** Names this flavour in error messages and logs, e.g. "histogram" or "external histogram". */
    String statisticsDescription();

    /**
     * SQL for the query whose single row carries this column's buckets. May itself run
     * intermediate queries (native HLL mode derives bucket boundaries first).
     */
    String buildBatchedHistogramQuery(ConnectContext context, AnalyzeStatus analyzeStatus,
                                      HistogramCollectParams params, String columnName, Type columnType,
                                      Map<String, String> mostCommonValues) throws Exception;

    /** One buffered row, as expressions and as SQL text; the two must stay in lockstep. */
    List<Expr> buildBatchInsertRow(String columnName, String buckets, String mcvJson);

    String buildBatchInsertRowSql(String columnName, String buckets, String mcvJson);

    /**
     * Hook run once the collection finishes, successfully or not, with every column that reached
     * storage. No-op unless the flavour needs post-write cleanup.
     */
    void afterCollection(ConnectContext context, List<String> insertedColumns);
}
