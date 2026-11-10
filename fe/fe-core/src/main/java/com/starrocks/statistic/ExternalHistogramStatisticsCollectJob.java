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
import com.starrocks.qe.ConnectContext;
import com.starrocks.type.Type;

import java.util.List;
import java.util.Map;

public class ExternalHistogramStatisticsCollectJob extends StatisticsCollectJob {
    private final String catalogName;

    // Columns whose histogram row actually reached storage. Null until collect() has run; a job that
    // tolerated a failed column collected fewer columns than it declared, and only these may be recorded
    // as having a histogram (see StatisticExecutor#collectStatistics).
    private List<String> collectedColumns = null;

    public ExternalHistogramStatisticsCollectJob(String catalogName, Database db, Table table, List<String> columnNames,
                                                 List<Type> columnTypes, StatsConstants.AnalyzeType type,
                                                 StatsConstants.ScheduleType scheduleType,
                                                 Map<String, String> properties) {
        super(db, table, columnNames, columnTypes, type, scheduleType, properties);
        this.catalogName = catalogName;
    }

    @Override
    public String getCatalogName() {
        return catalogName;
    }

    @Override
    public String getName() {
        return "ExternalHistogram";
    }

    @Override
    public void collect(ConnectContext context, AnalyzeStatus analyzeStatus) throws Exception {
        context.getSessionVariable().setNewPlanerAggStage(1);

        HistogramCollector collector =
                new HistogramCollector(new ExternalHistogramTraits(this, new HistogramCollectParams(properties)));
        try {
            collector.collect(context, analyzeStatus);
        } finally {
            collectedColumns = List.copyOf(collector.collectedColumns());
        }
    }

    /**
     * The columns this job actually produced a histogram for, which is every declared column unless a
     * column's collection failed and was tolerated (see {@link HistogramCollectTraits#toleratesColumnFailure}).
     * Recording a histogram for a column that has none would leave metadata pointing at nothing.
     */
    public List<String> getCollectedColumns() {
        return collectedColumns == null ? columnNames : collectedColumns;
    }
}
