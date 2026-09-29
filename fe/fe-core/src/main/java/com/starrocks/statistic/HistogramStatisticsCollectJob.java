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
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.qe.ConnectContext;
import com.starrocks.type.Type;

import java.util.List;
import java.util.Map;

public class HistogramStatisticsCollectJob extends StatisticsCollectJob {
    public HistogramStatisticsCollectJob(Database db, Table table, List<String> columnNames, List<Type> columnTypes,
                                         StatsConstants.ScheduleType scheduleType, Map<String, String> properties) {
        super(db, table, columnNames, columnTypes, StatsConstants.AnalyzeType.HISTOGRAM, scheduleType, properties);
    }

    @Override
    public void collect(ConnectContext context, AnalyzeStatus analyzeStatus) throws Exception {
        context.getSessionVariable().setNewPlanerAggStage(1);

        if (table.isTemporaryTable()) {
            context.setSessionId(((OlapTable) table).getSessionId());
        }

        new HistogramCollector(new NativeHistogramTraits(this, new HistogramCollectParams(properties)))
                .collect(context, analyzeStatus);
    }

    @Override
    public String getName() {
        return "Histogram";
    }
}
