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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/qe/QeProcessorImpl.java

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

package com.starrocks.qe;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.starrocks.catalog.MvId;
import com.starrocks.common.Config;
import com.starrocks.common.UserException;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.memory.MemoryTrackable;
import com.starrocks.qe.scheduler.Coordinator;
import com.starrocks.thrift.TBatchReportExecStatusParams;
import com.starrocks.thrift.TBatchReportExecStatusResult;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TReportAuditStatisticsParams;
import com.starrocks.thrift.TReportAuditStatisticsResult;
import com.starrocks.thrift.TReportExecStatusParams;
import com.starrocks.thrift.TReportExecStatusResult;
import com.starrocks.thrift.TStatus;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.thrift.TUniqueId;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.util.SizeEstimator;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static com.starrocks.mysql.MysqlCommand.COM_STMT_EXECUTE;

public final class QeProcessorImpl implements QeProcessor, MemoryTrackable {

    private static final Logger LOG = LogManager.getLogger(QeProcessorImpl.class);
    private static final long ONE_MINUTE = 60 * 1000L;
    private final Map<TUniqueId, QueryInfo> coordinatorMap = Maps.newConcurrentMap();
    private final Map<TUniqueId, Long> monitorQueryMap = Maps.newConcurrentMap();
    private final AtomicLong lastCheckTime = new AtomicLong();

    public static final QeProcessor INSTANCE;

    static {
        INSTANCE = new QeProcessorImpl();
    }

    private QeProcessorImpl() {
    }

    @Override
    public Coordinator getCoordinator(TUniqueId queryId) {
        QueryInfo queryInfo = coordinatorMap.get(queryId);
        if (queryInfo != null) {
            return queryInfo.getCoord();
        }
        return null;
    }

    @Override
    public List<Coordinator> getCoordinators() {
        return coordinatorMap.values().stream()
                .map(QueryInfo::getCoord)
                .collect(Collectors.toList());
    }

    @Override
    public void registerQuery(TUniqueId queryId, Coordinator coord) throws UserException {
        registerQuery(queryId, new QueryInfo(coord));
    }

    @Override
    public void registerQuery(TUniqueId queryId, QueryInfo info) throws UserException {
        if (needLogRegisterAndUnregisterQueryId(info)) {
            LOG.info("register query id = {}", DebugUtil.printId(queryId));
        }
        final QueryInfo result = coordinatorMap.putIfAbsent(queryId, info);
        if (result != null) {
            throw new UserException("queryId " + queryId + " already exists");
        }
        scanMonitorQueries();
    }

    private void scanMonitorQueries() {
        long now = System.currentTimeMillis();
        long lastCheckTime = this.lastCheckTime.get();
        if (now - lastCheckTime > ONE_MINUTE && this.lastCheckTime.compareAndSet(lastCheckTime, now)) {
            for (Map.Entry<TUniqueId, Long> entry : monitorQueryMap.entrySet()) {
                if (now > entry.getValue()) {
                    LOG.warn("monitor expired, query id = {}", DebugUtil.printId(entry.getKey()));
                    unregisterQuery(entry.getKey());
                    monitorQueryMap.remove(entry.getKey());
                }
            }
        }
    }

    @Override
    public void monitorQuery(TUniqueId queryId, long expireTime) {
        monitorQueryMap.put(queryId, expireTime);
    }

    @Override
    public void unMonitorQuery(TUniqueId queryId) {
        monitorQueryMap.remove(queryId);
    }

    @Override
    public void unregisterQuery(TUniqueId queryId) {
        QueryInfo info = coordinatorMap.remove(queryId);
        if (info != null) {
            if (info.getCoord() != null) {
                info.getCoord().onFinished();
            }
            if (needLogRegisterAndUnregisterQueryId(info)) {
                LOG.info("deregister query id = {}", DebugUtil.printId(queryId));
            }
        }
    }

    @Override
    public Map<String, QueryStatisticsItem> getQueryStatistics() {
        final Map<String, QueryStatisticsItem> querySet = Maps.newHashMap();
        for (Map.Entry<TUniqueId, QueryInfo> entry : coordinatorMap.entrySet()) {
            final QueryInfo info = entry.getValue();
            final ConnectContext context = info.getConnectContext();
            if (info.sql == null || context == null) {
                continue;
            }
            final String queryIdStr = DebugUtil.printId(info.getConnectContext().getExecutionId());
            final QueryStatisticsItem item = new QueryStatisticsItem.Builder()
                    .queryId(queryIdStr)
                    .executionId(info.getConnectContext().getExecutionId())
                    .queryStartTime(info.getStartExecTime())
                    .sql(info.getSql())
                    .user(context.getQualifiedUser())
                    .connId(String.valueOf(context.getConnectionId()))
                    .db(context.getDatabase())
                    .fragmentInstanceInfos(info.getCoord().getFragmentInstanceInfos())
                    .profile(info.getCoord().getQueryProfile()).build();
            querySet.put(queryIdStr, item);
        }
        return querySet;
    }

    @Override
    public TReportExecStatusResult reportExecStatus(TReportExecStatusParams params, TNetworkAddress beAddr) {
        if (LOG.isDebugEnabled() && params.isSetProfile()) {
            LOG.debug("ReportExecStatus(): fragment_instance_id={}, query_id={}, backend num: {}, ip: {}",
                    DebugUtil.printId(params.fragment_instance_id), DebugUtil.printId(params.query_id),
                    params.backend_num, beAddr);
            LOG.debug("params: {}", params);
        }
        final TReportExecStatusResult result = new TReportExecStatusResult();
        final QueryInfo info = coordinatorMap.get(params.query_id);
        if (info == null) {
            LOG.info("ReportExecStatus() failed, query does not exist, fragment_instance_id={}, query_id={},",
                    DebugUtil.printId(params.fragment_instance_id), DebugUtil.printId(params.query_id));
            result.setStatus(new TStatus(TStatusCode.NOT_FOUND));
            result.status.addToError_msgs("query id " + DebugUtil.printId(params.query_id) + " not found");
            return result;
        }
        // TODO(murphy) update exec status in FE
        if (info.isMVJob) {
            result.setStatus(new TStatus(TStatusCode.OK));
            return result;
        }
        try {
            info.getCoord().updateFragmentExecStatus(params);
        } catch (Exception e) {
            LOG.warn("ReportExecStatus() failed, fragment_instance_id={}, query_id={}, error: {}",
                    DebugUtil.printId(params.fragment_instance_id), DebugUtil.printId(params.query_id), e.getMessage());
            LOG.warn("stack:", e);
            result.setStatus(new TStatus(TStatusCode.INTERNAL_ERROR));
            result.status.addToError_msgs(e.getMessage());
            return result;
        }
        result.setStatus(new TStatus(TStatusCode.OK));
        return result;
    }

    @Override
    public TReportAuditStatisticsResult reportAuditStatistics(TReportAuditStatisticsParams params,
                                                              TNetworkAddress beAddr) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("reportAuditStatistics(): fragment_instance_id={}, query_id={}, ip: {}",
                    DebugUtil.printId(params.fragment_instance_id), DebugUtil.printId(params.query_id), beAddr);
            LOG.debug("params: {}", params);
        }
        final TReportAuditStatisticsResult result = new TReportAuditStatisticsResult();
        final QueryInfo info = coordinatorMap.get(params.query_id);
        if (info == null) {
            LOG.info("reportAuditStatistics() failed, query does not exist, fragment_instance_id={}, query_id={},",
                    DebugUtil.printId(params.fragment_instance_id), DebugUtil.printId(params.query_id));
            result.setStatus(new TStatus(TStatusCode.NOT_FOUND));
            result.status.addToError_msgs("query id " + DebugUtil.printId(params.query_id) + " not found");
            return result;
        }
        try {
            info.getCoord().updateAuditStatistics(params);
        } catch (Exception e) {
            LOG.warn("reportAuditStatistics() failed, fragment_instance_id={}, query_id={}, error: {}",
                    DebugUtil.printId(params.fragment_instance_id), DebugUtil.printId(params.query_id), e.getMessage(),
                    e);
            result.setStatus(new TStatus(TStatusCode.INTERNAL_ERROR));
            result.status.addToError_msgs(e.getMessage());
            return result;
        }
        result.setStatus(new TStatus(TStatusCode.OK));
        return result;
    }

    @Override
    public TBatchReportExecStatusResult batchReportExecStatus(TBatchReportExecStatusParams paramsList,
                                                              TNetworkAddress beAddr) {
        TBatchReportExecStatusResult resultList = new TBatchReportExecStatusResult();
        Iterator<TReportExecStatusParams> iters = paramsList.getParams_listIterator();
        while (iters.hasNext()) {
            TReportExecStatusParams params = iters.next();
            if (LOG.isDebugEnabled() && params.isSetProfile()) {
                LOG.debug("ReportExecStatus(): fragment_instance_id={}, query_id={}, backend num: {}, ip: {}",
                        DebugUtil.printId(params.fragment_instance_id), DebugUtil.printId(params.query_id),
                        params.backend_num, beAddr);
                LOG.debug("params: {}", params);
            }
            TReportExecStatusResult result = new TReportExecStatusResult();
            final QueryInfo info = coordinatorMap.get(params.query_id);
            if (info == null) {
                LOG.info("ReportExecStatus() failed, query does not exist, fragment_instance_id={}, query_id={},",
                        DebugUtil.printId(params.fragment_instance_id), DebugUtil.printId(params.query_id));
                result.setStatus(new TStatus(TStatusCode.NOT_FOUND));
                result.status.addToError_msgs("query id " + DebugUtil.printId(params.query_id) + " not found");
                resultList.addToStatus_list(result.getStatus());
                continue;
            }
            try {
                info.getCoord().updateFragmentExecStatus(params);
            } catch (Exception e) {
                LOG.warn("ReportExecStatus() failed, fragment_instance_id={}, query_id={}, error: {}",
                        DebugUtil.printId(params.fragment_instance_id), DebugUtil.printId(params.query_id),
                        e.getMessage());
                LOG.warn("stack:", e);
                result.setStatus(new TStatus(TStatusCode.INTERNAL_ERROR));
                result.status.addToError_msgs(e.getMessage());
                resultList.addToStatus_list(result.getStatus());
                continue;
            }
            result.setStatus(new TStatus(TStatusCode.OK));
            resultList.addToStatus_list(result.getStatus());
        }

        return resultList;
    }

    @Override
    public long getCoordinatorCount() {
        return coordinatorMap.size();
    }

    @Override
    public long estimateSize() {
        return SizeEstimator.estimate(coordinatorMap) + SizeEstimator.estimate(monitorQueryMap);
    }

    @Override
    public Map<String, Long> estimateCount() {
        return ImmutableMap.of("QueryInfo", (long) coordinatorMap.size());
    }

    public static final class QueryInfo {
        private final ConnectContext connectContext;
        private final Coordinator coord;
        private final String sql;
        private final long startExecTime;

        private boolean isMVJob = false;

        // from Export, Pull load, Insert 
        public QueryInfo(Coordinator coord) {
            this(null, null, coord);
        }

        // from query
        public QueryInfo(ConnectContext connectContext, String sql, Coordinator coord) {
            this.connectContext = connectContext;
            this.coord = coord;
            this.sql = sql;
            this.startExecTime = System.currentTimeMillis();
        }

        // TODO: report exec status for MV job
        public static QueryInfo fromMVJob(MvId mvId, ConnectContext connectContext) {
            QueryInfo res = new QueryInfo(connectContext, null, null);
            res.isMVJob = true;
            return res;
        }

        public ConnectContext getConnectContext() {
            return connectContext;
        }

        public Coordinator getCoord() {
            return coord;
        }

        public String getSql() {
            return sql;
        }

        public long getStartExecTime() {
            return startExecTime;
        }
    }

    private static boolean needLogRegisterAndUnregisterQueryId(QueryInfo inf) {
        ConnectContext context = inf.getConnectContext();
        return Config.log_register_and_unregister_query_id &&
                context != null &&
                (context.getCommand() != COM_STMT_EXECUTE ||
                        context.getSessionVariable().isAuditExecuteStmt());
    }
}
