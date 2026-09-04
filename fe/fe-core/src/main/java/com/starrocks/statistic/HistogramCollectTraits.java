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

import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.common.DdlException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.thrift.TStatisticData;
import com.starrocks.type.Type;

import java.util.List;
import java.util.Map;

/**
 * The flavour-specific half of a histogram collection: which statistics table the rows land in, and
 * what the SQL for one column looks like. {@link HistogramCollector} owns everything that does not
 * depend on the flavour - the per-column loop, the insert buffering and the progress accounting.
 *
 * @see NativeHistogramTraits
 * @see ExternalHistogramTraits
 */
abstract class HistogramCollectTraits {
    protected final StatisticsCollectJob job;
    protected final HistogramCollectParams params;
    protected final Database db;
    protected final Table table;
    protected final String catalogName;

    HistogramCollectTraits(StatisticsCollectJob job, HistogramCollectParams params) {
        this.job = job;
        this.params = params;
        this.db = job.getDb();
        this.table = job.getTable();
        this.catalogName = job.getCatalogName();
    }

    /** Statistics table the collected rows are inserted into. */
    abstract String statsTableName();

    /** Names this flavour in error messages and logs, e.g. "histogram" or "external histogram". */
    abstract String statisticsDescription();

    /** SQL for the most-common-values query of one column. */
    abstract String buildMcvQuery(String columnName);

    /**
     * Turn this flavour's raw MCV result into a column-value -&gt; count map. The native MCV query is
     * sampled, so its counts are scaled back up to full-table counts; the external one scans the
     * whole table and passes them through.
     */
    abstract Map<String, String> buildMostCommonValues(List<TStatisticData> mcv);

    /**
     * SQL for the query whose single row carries this column's buckets. May itself run intermediate
     * queries - the native HLL mode derives the bucket boundaries before it can name them.
     */
    abstract String buildBucketsQuery(ConnectContext context, AnalyzeStatus analyzeStatus, String columnName,
                                      Type columnType, Map<String, String> mostCommonValues) throws Exception;

    /** One buffered row, as expressions and as SQL text; the two must stay in lockstep. */
    abstract List<Expr> buildInsertRow(String columnName, String buckets, String mcvJson);

    abstract String buildInsertRowSql(String columnName, String buckets, String mcvJson);

    /**
     * Hook run once the collection finishes, successfully or not, with every column whose row
     * reached storage. Only the external flavour needs it, to drop the rows it superseded.
     */
    void afterCollection(ConnectContext context, List<String> insertedColumns) {
    }

    /** Fails unless the bucket query returned exactly the one row it is supposed to. */
    TStatisticData singleResult(List<TStatisticData> results, String columnName) throws DdlException {
        return HistogramStatisticsUtils.getSingleHistogramResult(results, columnName, statisticsDescription());
    }
}
