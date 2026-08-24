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
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TStatisticData;
import com.starrocks.type.Type;

import java.util.List;
import java.util.Map;

import static com.starrocks.statistic.HistogramStatisticsUtils.buildMostCommonValues;

/**
 * Collects histograms one column at a time, each as a single INSERT ... SELECT computed entirely
 * by the backend. Note that the MCV query runs on a bare StatisticExecutor rather than through
 * StatisticsCollectJob#queryStatisticSync, so it deliberately skips that method's cancellation
 * check, timeout recalculation and session-variable setup - the batched strategy does not.
 *
 * @see BatchedHistogramCollector
 */
final class LegacyHistogramCollector {
    private final LegacyStatsCollectionUtils target;
    private final HistogramCollectParams params;

    LegacyHistogramCollector(LegacyStatsCollectionUtils target, HistogramCollectParams params) {
        this.target = target;
        this.params = params;
    }

    void collect(ConnectContext context, AnalyzeStatus analyzeStatus) throws Exception {
        StatisticsCollectJob job = target.job();
        List<String> columnNames = job.getColumnNames();
        List<Type> columnTypes = job.getColumnTypes();

        long finishedSQLNum = 0;
        long totalCollectSQL = columnNames.size();
        for (int i = 0; i < columnNames.size(); i++) {
            String columnName = columnNames.get(i);
            Type columnType = columnTypes.get(i);

            StatisticExecutor statisticExecutor = new StatisticExecutor();
            List<TStatisticData> mcv =
                    statisticExecutor.queryMCV(context, target.buildMcvQuery(params, columnName));
            Map<String, String> mostCommonValues =
                    buildMostCommonValues(mcv, target.mcvCountScaleRatio(params));

            String sql = target.buildSqlCmd(
                    context, statisticExecutor, params, columnName, columnType, mostCommonValues);
            job.collectStatisticSync(sql, context, analyzeStatus);
            target.afterColumnInserted(context, statisticExecutor, columnName);

            finishedSQLNum++;
            analyzeStatus.setProgress(finishedSQLNum * 100 / totalCollectSQL);
            GlobalStateMgr.getCurrentState().getAnalyzeMgr().addAnalyzeStatus(analyzeStatus);
        }
    }
}
