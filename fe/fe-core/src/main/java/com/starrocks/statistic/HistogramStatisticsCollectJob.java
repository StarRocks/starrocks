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
import com.starrocks.common.Config;
import com.starrocks.qe.ConnectContext;
import com.starrocks.type.Type;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;

public class HistogramStatisticsCollectJob extends StatisticsCollectJob {
    private static final Logger LOG = LogManager.getLogger(HistogramStatisticsCollectJob.class);

    public HistogramStatisticsCollectJob(Database db, Table table, List<String> columnNames, List<Type> columnTypes,
                                         StatsConstants.ScheduleType scheduleType, Map<String, String> properties) {
        super(db, table, columnNames, columnTypes, StatsConstants.AnalyzeType.HISTOGRAM, scheduleType, properties);
    }

    @Override
    public void collect(ConnectContext context, AnalyzeStatus analyzeStatus) throws Exception {
        context.getSessionVariable().setNewPlanerAggStage(1);

        HistogramCollectParams params = new HistogramCollectParams(properties);
        // Derived once per collection so an invalid mode is reported once, not once per column.
        StatsConstants.HistogramCollectBucketNdvMode ndvMode = parseBucketNdvMode(properties);

        if (table.isTemporaryTable()) {
            context.setSessionId(((OlapTable) table).getSessionId());
        }

        if (Config.enable_batch_insert_histogram_statistics && columnNames.size() > 1) {
            new BatchedHistogramCollector(new HistogramBatchedTarget(this, ndvMode), params)
                    .collect(context, analyzeStatus);
        } else {
            new LegacyHistogramCollector(new HistogramLegacyTarget(this, ndvMode), params)
                    .collect(context, analyzeStatus);
        }
    }

    // The key is always present: AnalyzeStmtAnalyzer fills it in with computeIfAbsent while analyzing the
    // ANALYZE statement, long before a job is built, so a missing key means a caller that skipped analysis.
    static StatsConstants.HistogramCollectBucketNdvMode parseBucketNdvMode(Map<String, String> properties) {
        String mode = properties.get(StatsConstants.HISTOGRAM_COLLECT_BUCKET_NDV_MODE);
        if (mode.equalsIgnoreCase("none")) {
            return StatsConstants.HistogramCollectBucketNdvMode.NONE;
        } else if (mode.equalsIgnoreCase("sample")) {
            return StatsConstants.HistogramCollectBucketNdvMode.SAMPLE;
        } else if (mode.equalsIgnoreCase("hll")) {
            return StatsConstants.HistogramCollectBucketNdvMode.HLL;
        } else {
            LOG.warn("Invalid histogram collect bucket ndv mode {}.", mode);
            return StatsConstants.HistogramCollectBucketNdvMode.NONE;
        }
    }

    @Override
    public String getName() {
        return "Histogram";
    }
}
