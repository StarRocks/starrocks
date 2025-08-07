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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.TimerTask;

/*
 * MetricCalculator will collect and calculate some certain metrics at a fix rate,
 * such QPS, and save the result for users to get.
 */
public class MetricCalculator extends TimerTask {
    private static final Logger LOG = LogManager.getLogger(MetricCalculator.class);

    private long lastTs = -1;
    private long lastQueryCounter = -1;
    private long lastRequestCounter = -1;
    private long lastQueryErrCounter = -1;
    private long lastQueryInternalErrCounter = -1;
    private long lastQueryAnalysisErrCounter = -1;
    private long lastQueryTimeOutCounter = -1;

    private long lastRootQueryCounter = -1;
    private long lastRootRequestCounter = -1;
    private long lastRootQueryErrCounter = -1;
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
            lastQueryInternalErrCounter = MetricRepo.COUNTER_QUERY_INTERNAL_ERR.getValue();
            lastQueryAnalysisErrCounter = MetricRepo.COUNTER_QUERY_ANALYSIS_ERR.getValue();
            lastQueryTimeOutCounter = MetricRepo.COUNTER_QUERY_TIMEOUT.getValue();

            lastRootQueryCounter = MetricRepo.COUNTER_ROOT_QUERY_ALL.getValue();
            lastRootRequestCounter = MetricRepo.COUNTER_ROOT_REQUEST_ALL.getValue();
            lastRootQueryErrCounter = MetricRepo.COUNTER_ROOT_QUERY_ERR.getValue();

            lastQueryEventTime = System.currentTimeMillis() * 1000000;
            return;
        }

        long interval = (currentTs - lastTs) / 1000 + 1;

        // qps
        long currentQueryCounter = MetricRepo.COUNTER_QUERY_ALL.getValue();
        double qps = (double) (currentQueryCounter - lastQueryCounter) / interval;
        MetricRepo.GAUGE_QUERY_PER_SECOND.setValue(qps < 0 ? 0.0 : qps);
        lastQueryCounter = currentQueryCounter;

        // root qps
        long currentRootQueryCounter = MetricRepo.COUNTER_ROOT_QUERY_ALL.getValue();
        double rootQps = (double) (currentRootQueryCounter - lastRootQueryCounter) / interval;
        MetricRepo.GAUGE_ROOT_QUERY_PER_SECOND.setValue(rootQps < 0 ? 0.0 : rootQps);
        lastRootQueryCounter = currentRootQueryCounter;

        // rps
        long currentRequestCounter = MetricRepo.COUNTER_REQUEST_ALL.getValue();
        double rps = (double) (currentRequestCounter - lastRequestCounter) / interval;
        MetricRepo.GAUGE_REQUEST_PER_SECOND.setValue(rps < 0 ? 0.0 : rps);
        lastRequestCounter = currentRequestCounter;

        // root rps
        long currentRootRequestCounter = MetricRepo.COUNTER_ROOT_REQUEST_ALL.getValue();
        double rootRps = (double) (currentRootRequestCounter - lastRootRequestCounter) / interval;
        MetricRepo.GAUGE_ROOT_REQUEST_PER_SECOND.setValue(rootRps < 0 ? 0.0 : rootRps);
        lastRootRequestCounter = currentRootRequestCounter;

        // err rate
        long currentErrCounter = MetricRepo.COUNTER_QUERY_ERR.getValue();
        double errRate = (double) (currentErrCounter - lastQueryErrCounter) / interval;
        MetricRepo.GAUGE_QUERY_ERR_RATE.setValue(errRate < 0 ? 0.0 : errRate);
        lastQueryErrCounter = currentErrCounter;

        // root err rate
        long currentRootErrCounter = MetricRepo.COUNTER_ROOT_QUERY_ERR.getValue();
        double rootErrRate = (double) (currentRootErrCounter - lastRootQueryErrCounter) / interval;
        MetricRepo.GAUGE_ROOT_QUERY_ERR_RATE.setValue(rootErrRate < 0 ? 0.0 : rootErrRate);
        lastRootQueryErrCounter = currentRootErrCounter;

        // internal err rate
        long currentInternalErrCounter = MetricRepo.COUNTER_QUERY_INTERNAL_ERR.getValue();
        double internalErrRate = (double) (currentInternalErrCounter - lastQueryInternalErrCounter) / interval;
        MetricRepo.GAUGE_QUERY_INTERNAL_ERR_RATE.setValue(errRate < 0 ? 0.0 : internalErrRate);
        lastQueryInternalErrCounter = currentErrCounter;

        // analysis error rate
        long currentAnalysisErrCounter = MetricRepo.COUNTER_QUERY_ANALYSIS_ERR.getValue();
        double analysisErrRate = (double) (currentAnalysisErrCounter - lastQueryAnalysisErrCounter) / interval;
        MetricRepo.GAUGE_QUERY_ANALYSIS_ERR_RATE.setValue(errRate < 0 ? 0.0 : analysisErrRate);
        lastQueryAnalysisErrCounter = currentErrCounter;

        // query timeout rate
        long currentTimeoutErrCounter = MetricRepo.COUNTER_QUERY_TIMEOUT.getValue();
        double timeoutErrRate = (double) (currentTimeoutErrCounter - lastQueryTimeOutCounter) / interval;
        MetricRepo.GAUGE_QUERY_TIMEOUT_RATE.setValue(errRate < 0 ? 0.0 : timeoutErrRate);
        lastQueryTimeOutCounter = currentErrCounter;

        lastTs = currentTs;

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

        if (Config.memory_tracker_enable)  {
            MetricRepo.updateMemoryUsageMetrics();
        }

        if (Config.emr_serverless_warehouse_mapping_metrics_enable) {
            try {
                MetricRepo.updateWarehouseMetrics();
            } catch (Exception e) {
                LOG.warn("update warehouse metric calculator failed", e);
            }
        }

        MetricRepo.GAUGE_SAFE_MODE.setValue(GlobalStateMgr.getCurrentState().isSafeMode() ? 1 : 0);
    }
}
