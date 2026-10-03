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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/http/rest/HealthAction.java

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

package com.starrocks.http.rest;

import com.starrocks.common.Config;
import com.starrocks.http.ActionController;
import com.starrocks.http.BaseRequest;
import com.starrocks.http.BaseResponse;
import com.starrocks.http.IllegalArgException;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.GracefulExitFlag;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.atomic.AtomicLong;

public class HealthAction extends RestBaseAction {
    private static final Logger LOG = LogManager.getLogger(HealthAction.class);

    private static final AtomicLong PROBE_COUNT = new AtomicLong(0);
    private static final AtomicLong LAST_SUMMARY_TS = new AtomicLong(0);
    private static final long SUMMARY_INTERVAL_MS = 60_000L;
    // Graceful-exit probe logging: first probe is always logged (marks the moment Load Balancer
    // starts polling a 500), subsequent probes are sampled at SUMMARY_INTERVAL_MS.
    private static final AtomicLong GRACEFUL_PROBE_COUNT = new AtomicLong(0);
    private static final AtomicLong LAST_GRACEFUL_LOG_TS = new AtomicLong(0);

    public HealthAction(ActionController controller) {
        super(controller);
    }

    public static void registerAction(ActionController controller)
            throws IllegalArgException {
        controller.registerHandler(HttpMethod.GET, "/api/health", new HealthAction(controller));
    }

    // Liveness probe. Historically anonymous; gated for backward compatibility so it
    // requires Basic auth (AuthN-only, no privilege check) only when the operator opts
    // in via `enable_http_auth`.
    @Override
    public boolean needAuth() {
        return Config.enable_http_auth;
    }

    @Override
    protected void executeWithoutPassword(BaseRequest request, BaseResponse response) {
        if (GracefulExitFlag.isGracefulExit()) {
            long first = GRACEFUL_PROBE_COUNT.getAndIncrement();
            long now = System.currentTimeMillis();
            long last = LAST_GRACEFUL_LOG_TS.get();
            // First probe (first==0, TS still 0) atomically seeds the timestamp via CAS so the
            // second probe (now - 0 >= interval) does not log again immediately. The sampling
            // branch additionally requires last != 0 so a concurrent second probe that reads the
            // pre-seed value 0 cannot take the sampling path and log spuriously.
            if ((first == 0 && LAST_GRACEFUL_LOG_TS.compareAndSet(0, now))
                    || (last != 0 && now - last >= SUMMARY_INTERVAL_MS && LAST_GRACEFUL_LOG_TS.compareAndSet(last, now))) {
                LOG.info("health probe from {} graceful {} acceptNew {} totalGracefulProbes {}",
                        request.getHostString(), GracefulExitFlag.isGracefulExit(),
                        GracefulExitFlag.shouldAcceptNewRequest(), first + 1);
            }
            sendResult(request, response, HttpResponseStatus.INTERNAL_SERVER_ERROR);
        } else {
            summary();
            response.setContentType("application/json");

            RestResult result = new RestResult();
            result.addResultEntry("total_backend_num",
                    GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getTotalBackendNumber());
            result.addResultEntry("online_backend_num",
                    GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getAliveBackendNumber());
            sendResult(request, response, result);
        }
    }

    // Aggregate non-graceful probes and emit one summary per minute.
    private void summary() {
        PROBE_COUNT.incrementAndGet();
        long now = System.currentTimeMillis();
        long last = LAST_SUMMARY_TS.get();
        if (now - last >= SUMMARY_INTERVAL_MS && LAST_SUMMARY_TS.compareAndSet(last, now)) {
            long probeCount = PROBE_COUNT.getAndSet(0);
            if (probeCount > 0) {
                LOG.info("health-probe summary: {} requests", probeCount);
            }
        }
    }

    @Override
    public boolean supportAsyncHandler() {
        // Health Action need to be handled synchronously
        return false;
    }
}
