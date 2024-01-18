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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/metric/MetricCalculator.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.metric;

import com.starrocks.common.Config;
import com.starrocks.qe.QueryDetail;
import com.starrocks.qe.QueryDetailQueue;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.TimerTask;

/*
 * MetricCalculator will collect and calculate some certain metrics at a fix rate,
 * such QPS, and save the result for users to get.
 */
public class MetricCalculator extends TimerTask {
    private long lastTs = -1;
    private long lastQueryCounter = -1;
    private long lastRequestCounter = -1;
    private long lastQueryErrCounter = -1;
    private long lastQueryEventTime = -1;

    @Override
    public void run() {
        update();
    }

    private void update() {
        long currentTs = System.currentTimeMillis();
        if (lastTs == -1) {
            lastTs = currentTs;
            lastQueryCounter = MetricRepo.COUNTER_QUERY_ALL.getValue();
            lastRequestCounter = MetricRepo.COUNTER_REQUEST_ALL.getValue();
            lastQueryErrCounter = MetricRepo.COUNTER_QUERY_ERR.getValue();
            lastQueryEventTime = System.currentTimeMillis() * 1000000;
            return;
        }

        long interval = (currentTs - lastTs) / 1000 + 1;

        // qps
        long currentQueryCounter = MetricRepo.COUNTER_QUERY_ALL.getValue();
        double qps = (double) (currentQueryCounter - lastQueryCounter) / interval;
        MetricRepo.GAUGE_QUERY_PER_SECOND.setValue(qps < 0 ? 0.0 : qps);
        lastQueryCounter = currentQueryCounter;

        // rps
        long currentRequestCounter = MetricRepo.COUNTER_REQUEST_ALL.getValue();
        double rps = (double) (currentRequestCounter - lastRequestCounter) / interval;
        MetricRepo.GAUGE_REQUEST_PER_SECOND.setValue(rps < 0 ? 0.0 : rps);
        lastRequestCounter = currentRequestCounter;

        // err rate
        long currentErrCounter = MetricRepo.COUNTER_QUERY_ERR.getValue();
        double errRate = (double) (currentErrCounter - lastQueryErrCounter) / interval;
        MetricRepo.GAUGE_QUERY_ERR_RATE.setValue(errRate < 0 ? 0.0 : errRate);
        lastQueryErrCounter = currentErrCounter;

        lastTs = currentTs;

        // max tablet compaction score of all backends
        if (RunMode.isSharedDataMode()) {
            MetricRepo.GAUGE_MAX_TABLET_COMPACTION_SCORE.setValue(
                    (long) GlobalStateMgr.getCurrentState().getCompactionMgr().getMaxCompactionScore());
        } else {
            long maxCompactionScore = 0;
            List<Metric> compactionScoreMetrics = MetricRepo.getMetricsByName(MetricRepo.TABLET_MAX_COMPACTION_SCORE);
            for (Metric metric : compactionScoreMetrics) {
                if (((GaugeMetric<Long>) metric).getValue() > maxCompactionScore) {
                    maxCompactionScore = ((GaugeMetric<Long>) metric).getValue();
                }
            }
            MetricRepo.GAUGE_MAX_TABLET_COMPACTION_SCORE.setValue(maxCompactionScore);
        }

        // query latency
        List<QueryDetail> queryList = QueryDetailQueue.getQueryDetailsAfterTime(lastQueryEventTime);
        List<Long> latencyList = new ArrayList<>();
        double latencySum = 0L;
        for (QueryDetail queryDetail : queryList) {
            if (queryDetail.isQuery() && queryDetail.getState() == QueryDetail.QueryMemState.FINISHED) {
                latencyList.add(queryDetail.getLatency());
                latencySum += queryDetail.getLatency();
            }
        }
        if (queryList.size() > 0) {
            lastQueryEventTime = queryList.get(queryList.size() - 1).getEventTime();
        }
        if (latencyList.size() > 0) {
            MetricRepo.GAUGE_QUERY_LATENCY_MEAN.setValue(latencySum / latencyList.size());

            latencyList.sort(Comparator.naturalOrder());

            int index = (int) Math.round((latencyList.size() - 1) * 0.5);
            MetricRepo.GAUGE_QUERY_LATENCY_MEDIAN.setValue((double) latencyList.get(index));
            index = (int) Math.round((latencyList.size() - 1) * 0.75);
            MetricRepo.GAUGE_QUERY_LATENCY_P75.setValue((double) latencyList.get(index));
            index = (int) Math.round((latencyList.size() - 1) * 0.90);
            MetricRepo.GAUGE_QUERY_LATENCY_P90.setValue((double) latencyList.get(index));
            index = (int) Math.round((latencyList.size() - 1) * 0.95);
            MetricRepo.GAUGE_QUERY_LATENCY_P95.setValue((double) latencyList.get(index));
            index = (int) Math.round((latencyList.size() - 1) * 0.99);
            MetricRepo.GAUGE_QUERY_LATENCY_P99.setValue((double) latencyList.get(index));
            index = (int) Math.round((latencyList.size() - 1) * 0.999);
            MetricRepo.GAUGE_QUERY_LATENCY_P999.setValue((double) latencyList.get(index));
        } else {
            MetricRepo.GAUGE_QUERY_LATENCY_MEAN.setValue(0.0);
            MetricRepo.GAUGE_QUERY_LATENCY_MEDIAN.setValue(0.0);
            MetricRepo.GAUGE_QUERY_LATENCY_P75.setValue(0.0);
            MetricRepo.GAUGE_QUERY_LATENCY_P90.setValue(0.0);
            MetricRepo.GAUGE_QUERY_LATENCY_P95.setValue(0.0);
            MetricRepo.GAUGE_QUERY_LATENCY_P99.setValue(0.0);
            MetricRepo.GAUGE_QUERY_LATENCY_P999.setValue(0.0);
        }

        if (Config.enable_routine_load_lag_metrics)  {
            MetricRepo.updateRoutineLoadProcessMetrics();
        }

        MetricRepo.GAUGE_SAFE_MODE.setValue(GlobalStateMgr.getCurrentState().isSafeMode() ? 1 : 0);
    }
}
