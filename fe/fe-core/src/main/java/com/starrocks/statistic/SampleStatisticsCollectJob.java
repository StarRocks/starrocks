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
import com.starrocks.catalog.Type;
import com.starrocks.common.Config;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.statistic.sample.ColumnSampleManager;
import com.starrocks.statistic.sample.ColumnStats;
import com.starrocks.statistic.sample.SampleInfo;
import com.starrocks.statistic.sample.TabletSampleManager;

import java.util.List;
import java.util.Map;

public class SampleStatisticsCollectJob extends StatisticsCollectJob {

    public SampleStatisticsCollectJob(Database db, Table table, List<String> columnNames,
                                      StatsConstants.AnalyzeType type, StatsConstants.ScheduleType scheduleType,
                                      Map<String, String> properties) {
        super(db, table, columnNames, type, scheduleType, properties);
    }

    public SampleStatisticsCollectJob(Database db, Table table, List<String> columnNames, List<Type> columnTypes,
                                      StatsConstants.AnalyzeType type, StatsConstants.ScheduleType scheduleType,
                                      Map<String, String> properties) {
        super(db, table, columnNames, columnTypes, type, scheduleType, properties);
    }

    @Override
    public void collect(ConnectContext context, AnalyzeStatus analyzeStatus) throws Exception {
        TabletSampleManager tabletSampleManager = TabletSampleManager.init(properties, table);
        SampleInfo sampleInfo = tabletSampleManager.generateSampleInfo();

        ColumnSampleManager columnSampleManager = ColumnSampleManager.init(columnNames, columnTypes, table,
                sampleInfo);

        // sample complex type column stats
        if (!columnSampleManager.getComplexTypeStats().isEmpty()) {
            String complexTypeColsTask = sampleInfo.generateComplexTypeColumnTask(table.getId(),
                    db.getId(), table.getName(), db.getFullName(), columnSampleManager.getComplexTypeStats());
            context.getSessionVariable().setExprChildrenLimit(
                    Math.max(Config.expr_children_limit, columnSampleManager.getComplexTypeStats().size()));
            collectStatisticSync(complexTypeColsTask, context);
        }

        List<List<ColumnStats>> columnStatsBatchList = columnSampleManager.splitPrimitiveTypeStats();
        if (columnStatsBatchList.size() == 0) {
            analyzeStatus.setProgress(100);
            GlobalStateMgr.getCurrentState().getAnalyzeMgr().addAnalyzeStatus(analyzeStatus);
            return;
        }
        context.getSessionVariable().setEnableAnalyzePhasePruneColumns(true);

        // sample primitive type column stats
        int finishedTaskNum = 0;
        int totalTaskNum = columnStatsBatchList.size();
        double recordStagePoint = 0.2;
        for (List<ColumnStats> columnStatsBatch : columnStatsBatchList) {
            String primitiveTypeColsTask = sampleInfo.generatePrimitiveTypeColumnTask(table.getId(),
                    db.getId(), table.getName(), db.getFullName(), columnStatsBatch, tabletSampleManager);
            context.getSessionVariable().setExprChildrenLimit(
                    Math.max(Config.expr_children_limit, sampleInfo.getMaxSampleTabletNum()));
            collectStatisticSync(primitiveTypeColsTask, context);

            double progress = finishedTaskNum * 1.0 / totalTaskNum;
            if (progress >= recordStagePoint) {
                recordStagePoint += 0.2;
                analyzeStatus.setProgress((long) Math.ceil(progress * 100));
                GlobalStateMgr.getCurrentState().getAnalyzeMgr().addAnalyzeStatus(analyzeStatus);
            }
        }
    }

    @Override
    public String getName() {
        return "Sample";
    }

}
