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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/http/rest/MetricsAction.java

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

import com.google.common.base.Strings;
import com.starrocks.common.DdlException;
import com.starrocks.http.ActionController;
import com.starrocks.http.BaseRequest;
import com.starrocks.http.BaseResponse;
import com.starrocks.http.IllegalArgException;
import com.starrocks.http.UnauthorizedException;
import com.starrocks.metric.JsonMetricVisitor;
import com.starrocks.metric.MetricRepo;
import com.starrocks.metric.MetricVisitor;
import com.starrocks.metric.PrometheusMetricVisitor;
import com.starrocks.metric.SimpleCoreMetricVisitor;
import com.starrocks.mysql.privilege.PrivPredicate;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.UserIdentity;
import io.netty.handler.codec.http.HttpMethod;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

//fehost:port/metrics
//fehost:port/metrics?type=core
//fehost:port/metrics?type=json
public class MetricsAction extends RestBaseAction {

    private static final Logger LOG = LogManager.getLogger(MetricsAction.class);
    private static final String TYPE_PARAM = "type";
    // `with_table_metrics=minified` : without tables that have empty values
    // `with_table_metrics=all` : with all table metrics
    protected static final String WITH_TABLE_METRICS_PARAM = "with_table_metrics";
    protected static final String WITH_TABLE_METRICS_MINIFIED = "minified";
    protected static final String WITH_TABLE_METRICS_ALL = "all";
    public static final String API_PATH = "/metrics";

    public MetricsAction(ActionController controller) {
        super(controller);
    }

<<<<<<< HEAD
=======
    public static final class RequestParams {
        // Whether to collect per table metrics
        private final boolean collectTableMetrics;
        // Whether to collect per table metrics in minified mode, Ignore some heavy metrics if true
        private final boolean minifyTableMetrics;
        // Whether to collect per materialized view metrics
        private final boolean collectMVMetrics;
        // Whether to collect per materialized view metrics in minified mode, Ignore some heavy metrics if true
        private final boolean minifyMVMetrics;

        public RequestParams(boolean collectTableMetrics, boolean minifyTableMetrics,
                      boolean collectMVMetrics, boolean minifyMVMetrics) {
            this.collectTableMetrics = collectTableMetrics;
            this.minifyTableMetrics = minifyTableMetrics;
            this.collectMVMetrics = collectMVMetrics;
            this.minifyMVMetrics = minifyMVMetrics;
        }

        public boolean isCollectTableMetrics() {
            return collectTableMetrics;
        }

        public boolean isMinifyTableMetrics() {
            return minifyTableMetrics;
        }

        public boolean isCollectMVMetrics() {
            return collectMVMetrics;
        }

        public boolean isMinifyMVMetrics() {
            return minifyMVMetrics;
        }
    }

>>>>>>> 96e287c149 ([Enhancement] remove db lock in MetricRepo (#34558))
    public static void registerAction(ActionController controller) throws IllegalArgException {
        controller.registerHandler(HttpMethod.GET, API_PATH, new MetricsAction(controller));
    }

    @Override
    public void execute(BaseRequest request, BaseResponse response) throws DdlException {
        String type = request.getSingleParameter(TYPE_PARAM);
        MetricVisitor visitor = null;
        if (!Strings.isNullOrEmpty(type) && type.equalsIgnoreCase("core")) {
            visitor = new SimpleCoreMetricVisitor("starrocks_fe");
        } else if (!Strings.isNullOrEmpty(type) && type.equalsIgnoreCase("json")) {
            visitor = new JsonMetricVisitor("starrocks_fe");
        } else {
            visitor = new PrometheusMetricVisitor("starrocks_fe");
        }
        boolean collectTableMetrics = false;
        boolean minifyTableMetrics = true;
        String withTableMetrics = request.getSingleParameter(WITH_TABLE_METRICS_PARAM);
        UserIdentity currentUser = null;
        if (WITH_TABLE_METRICS_MINIFIED.equalsIgnoreCase(withTableMetrics) ||
                WITH_TABLE_METRICS_ALL.equalsIgnoreCase(withTableMetrics)) {
            try {
                ActionAuthorizationInfo authInfo = getAuthorizationInfo(request);
                currentUser = checkPassword(authInfo);
                if (GlobalStateMgr.getCurrentState().isUsingNewPrivilege()) {
                    checkUserOwnsAdminRole(currentUser);
                } else {
                    checkGlobalAuth(currentUser, PrivPredicate.ADMIN);
                }
                collectTableMetrics = true;
                if (WITH_TABLE_METRICS_ALL.equalsIgnoreCase(withTableMetrics)) {
                    minifyTableMetrics = false;
                }
            } catch (UnauthorizedException e) {
                LOG.warn("Auth failure when getting table level metrics, current user: {}, error msg: {}",
                        currentUser, e.getMessage(), e);
            }
        }
        response.setContentType("text/plain");
        response.getContent().append(MetricRepo.getMetric(visitor, collectTableMetrics, minifyTableMetrics));
        sendResult(request, response);
    }
}
