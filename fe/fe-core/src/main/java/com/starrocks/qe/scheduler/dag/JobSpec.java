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

package com.starrocks.qe.scheduler.dag;

import com.google.common.base.Preconditions;
import com.starrocks.analysis.DescriptorTable;
import com.starrocks.catalog.ResourceGroupClassifier;
import com.starrocks.common.util.CompressionUtils;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.load.loadv2.BulkLoadJob;
import com.starrocks.planner.PlanFragment;
import com.starrocks.planner.ScanNode;
import com.starrocks.planner.SchemaScanNode;
import com.starrocks.planner.StreamLoadPlanner;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.GlobalVariable;
import com.starrocks.qe.SessionVariable;
import com.starrocks.qe.scheduler.slot.SlotProvider;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.sql.LoadPlanner;
import com.starrocks.thrift.TCompressionType;
import com.starrocks.thrift.TDescriptorTable;
import com.starrocks.thrift.TExecPlanFragmentParams;
import com.starrocks.thrift.TLoadJobType;
import com.starrocks.thrift.TQueryGlobals;
import com.starrocks.thrift.TQueryOptions;
import com.starrocks.thrift.TQueryType;
import com.starrocks.thrift.TUniqueId;
import com.starrocks.thrift.TWorkGroup;
import org.apache.commons.lang3.StringUtils;

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.starrocks.qe.CoordinatorPreprocessor.genQueryGlobals;
import static com.starrocks.qe.CoordinatorPreprocessor.prepareResourceGroup;

public class JobSpec {

    private static final long UNINITIALIZED_LOAD_JOB_ID = -1;
    private long loadJobId = UNINITIALIZED_LOAD_JOB_ID;

    private TUniqueId queryId;

    private List<PlanFragment> fragments;
    private List<ScanNode> scanNodes;
    /**
     * copied from TQueryExecRequest; constant across all fragments
     */
    private TDescriptorTable descTable;

    private ConnectContext connectContext;
    private boolean enablePipeline;
    private boolean enableStreamPipeline;
    private boolean isBlockQuery;

    private boolean needReport;

    /**
     * Why we use query global?
     * When `NOW()` function is in sql, we need only one now(),
     * but, we execute `NOW()` distributed.
     * So we make a query global value here to make one `now()` value in one query process.
     */
    private TQueryGlobals queryGlobals;
    private TQueryOptions queryOptions;
    private TWorkGroup resourceGroup;

    private long warehouseId = WarehouseManager.DEFAULT_WAREHOUSE_ID;

    public long getWarehouseId() {
        return warehouseId;
    }

    private String planProtocol;

    private boolean enableQueue = false;
    private boolean needQueued = false;
    private boolean enableGroupLevelQueue = false;

    private boolean incrementalScanRanges = false;

    private boolean isSyncStreamLoad = false;

    public static class Factory {
        private Factory() {
        }

        public static JobSpec fromQuerySpec(ConnectContext context,
                                            List<PlanFragment> fragments,
                                            List<ScanNode> scanNodes,
                                            TDescriptorTable descTable,
                                            TQueryType queryType) {
            TQueryOptions queryOptions = context.getSessionVariable().toThrift();
            queryOptions.setQuery_type(queryType);
            queryOptions.setQuery_timeout(context.getExecTimeout());

            TQueryGlobals queryGlobals = genQueryGlobals(context.getStartTimeInstant(),
                    context.getSessionVariable().getTimeZone());
            if (context.getLastQueryId() != null) {
                queryGlobals.setLast_query_id(context.getLastQueryId().toString());
            }
            queryGlobals.setConnector_scan_node_number(scanNodes.stream().filter(x -> x.isRunningAsConnectorOperator()).count());

            return new Builder()
                    .queryId(context.getExecutionId())
                    .fragments(fragments)
                    .scanNodes(scanNodes)
                    .descTable(descTable)
                    .enableStreamPipeline(false)
                    .isBlockQuery(false)
                    .needReport(context.getSessionVariable().isEnableProfile() ||
                            context.getSessionVariable().isEnableBigQueryProfile())
                    .queryGlobals(queryGlobals)
                    .queryOptions(queryOptions)
                    .commonProperties(context)
                    .warehouseId(context.getCurrentWarehouseId())
                    .setPlanProtocol(context.getSessionVariable().getThriftPlanProtocol())
                    .build();
        }

        public static JobSpec fromMVMaintenanceJobSpec(ConnectContext context,
                                                       List<PlanFragment> fragments,
                                                       List<ScanNode> scanNodes,
                                                       TDescriptorTable descTable) {
            TQueryOptions queryOptions = context.getSessionVariable().toThrift();

            TQueryGlobals queryGlobals = genQueryGlobals(context.getStartTimeInstant(),
                    context.getSessionVariable().getTimeZone());
            if (context.getLastQueryId() != null) {
                queryGlobals.setLast_query_id(context.getLastQueryId().toString());
            }
            queryGlobals.setConnector_scan_node_number(scanNodes.stream().filter(x -> x.isRunningAsConnectorOperator()).count());

            return new Builder()
                    .queryId(context.getExecutionId())
                    .fragments(fragments)
                    .scanNodes(scanNodes)
                    .descTable(descTable)
                    .enableStreamPipeline(true)
                    .isBlockQuery(false)
                    .needReport(true)
                    .queryGlobals(queryGlobals)
                    .queryOptions(queryOptions)
                    .commonProperties(context)
                    .build();
        }

        public static JobSpec fromBrokerLoadJobSpec(LoadPlanner loadPlanner) {
            ConnectContext context = loadPlanner.getContext();

            TQueryOptions queryOptions = createBrokerLoadQueryOptions(loadPlanner);

            TQueryGlobals queryGlobals = genQueryGlobals(context.getStartTimeInstant(),
                    context.getSessionVariable().getTimeZone());
            if (context.getLastQueryId() != null) {
                queryGlobals.setLast_query_id(context.getLastQueryId().toString());
            }

            return new Builder()
                    .loadJobId(loadPlanner.getLoadJobId())
                    .queryId(loadPlanner.getLoadId())
                    .fragments(loadPlanner.getFragments())
                    .scanNodes(loadPlanner.getScanNodes())
                    .descTable(loadPlanner.getDescTable().toThrift())
                    .enableStreamPipeline(false)
                    .isBlockQuery(true)
                    .needReport(true)
                    .queryGlobals(queryGlobals)
                    .queryOptions(queryOptions)
                    .commonProperties(context)
                    .warehouseId(loadPlanner.getWarehouseId())
                    .build();
        }

        public static JobSpec fromStreamLoadJobSpec(LoadPlanner loadPlanner) {
            JobSpec jobSpec = fromBrokerLoadJobSpec(loadPlanner);
            jobSpec.getQueryOptions().setLoad_job_type((TLoadJobType.STREAM_LOAD));
            return jobSpec;
        }

        public static JobSpec fromBrokerExportSpec(ConnectContext context,
                                                   Long loadJobId, TUniqueId queryId,
                                                   DescriptorTable descTable,
                                                   List<PlanFragment> fragments,
                                                   List<ScanNode> scanNodes, String timezone,
                                                   long startTime,
                                                   Map<String, String> sessionVariables,
                                                   long execMemLimit) {
            TQueryOptions queryOptions = new TQueryOptions();
            setSessionVariablesToLoadQueryOptions(queryOptions, sessionVariables);
            queryOptions.setMem_limit(execMemLimit);

            TQueryGlobals queryGlobals = genQueryGlobals(Instant.ofEpochMilli(startTime), timezone);

            return new JobSpec.Builder()
                    .loadJobId(loadJobId)
                    .queryId(queryId)
                    .fragments(fragments)
                    .scanNodes(scanNodes)
                    .descTable(descTable.toThrift())
                    .enableStreamPipeline(false)
                    .isBlockQuery(true)
                    .needReport(true)
                    .queryGlobals(queryGlobals)
                    .queryOptions(queryOptions)
                    .warehouseId(context.getCurrentWarehouseId())
                    .commonProperties(context)
                    .build();
        }

        public static JobSpec fromRefreshDictionaryCacheSpec(ConnectContext context,
                                                             TUniqueId queryId,
                                                             DescriptorTable descTable,
                                                             List<PlanFragment> fragments,
                                                             List<ScanNode> scanNodes) {
            TQueryOptions queryOptions = context.getSessionVariable().toThrift();
            TQueryGlobals queryGlobals = genQueryGlobals(context.getStartTimeInstant(),
                    context.getSessionVariable().getTimeZone());

            return new JobSpec.Builder()
                    .queryId(queryId)
                    .fragments(fragments)
                    .scanNodes(scanNodes)
                    .descTable(descTable.toThrift())
                    .enableStreamPipeline(false)
                    .isBlockQuery(false)
                    .needReport(false)
                    .queryGlobals(queryGlobals)
                    .queryOptions(queryOptions)
                    .commonProperties(context)
                    .build();
        }

        public static JobSpec fromNonPipelineBrokerLoadJobSpec(ConnectContext context,
                                                               Long loadJobId, TUniqueId queryId,
                                                               DescriptorTable descTable,
                                                               List<PlanFragment> fragments,
                                                               List<ScanNode> scanNodes,
                                                               String timezone,
                                                               long startTime,
                                                               Map<String, String> sessionVariables,
                                                               long execMemLimit,
                                                               long warehouseId) {
            TQueryOptions queryOptions = new TQueryOptions();
            setSessionVariablesToLoadQueryOptions(queryOptions, sessionVariables);
            queryOptions.setQuery_type(TQueryType.LOAD);
            /*
             * For broker load job, user only need to set mem limit by 'exec_mem_limit' property.
             * And the variable 'load_mem_limit' does not make any effect.
             * However, in order to ensure the consistency of semantics when executing on the BE side,
             * and to prevent subsequent modification from incorrectly setting the load_mem_limit,
             * here we use exec_mem_limit to directly override the load_mem_limit property.
             */
            queryOptions.setMem_limit(execMemLimit);
            queryOptions.setLoad_mem_limit(execMemLimit);

            TQueryGlobals queryGlobals = genQueryGlobals(Instant.ofEpochMilli(startTime), timezone);

            return new Builder()
                    .loadJobId(loadJobId)
                    .queryId(queryId)
                    .fragments(fragments)
                    .scanNodes(scanNodes)
                    .descTable(descTable.toThrift())
                    .enableStreamPipeline(false)
                    .isBlockQuery(true)
                    .needReport(true)
                    .queryGlobals(queryGlobals)
                    .queryOptions(queryOptions)
                    .commonProperties(context)
                    .warehouseId(warehouseId)
                    .build();
        }

        public static JobSpec fromSyncStreamLoadSpec(StreamLoadPlanner planner) {
            TExecPlanFragmentParams params = planner.getExecPlanFragmentParams();
            TUniqueId queryId = params.getParams().getFragment_instance_id();

            return new Builder()
                    .queryId(queryId)
                    .fragments(Collections.emptyList())
                    .scanNodes(null)
                    .descTable(null)
                    .enableStreamPipeline(false)
                    .isBlockQuery(true)
                    .needReport(true)
                    .queryGlobals(null)
                    .queryOptions(null)
                    .enablePipeline(false)
                    .resourceGroup(null)
                    .warehouseId(planner.getWarehouseId())
                    .setSyncStreamLoad()
                    .build();
        }

        public static JobSpec mockJobSpec(ConnectContext context,
                                          List<PlanFragment> fragments,
                                          List<ScanNode> scanNodes) {
            TQueryOptions queryOptions = context.getSessionVariable().toThrift();

            TQueryGlobals queryGlobals = genQueryGlobals(context.getStartTimeInstant(),
                    context.getSessionVariable().getTimeZone());
            if (context.getLastQueryId() != null) {
                queryGlobals.setLast_query_id(context.getLastQueryId().toString());
            }

            return new Builder()
                    .queryId(context.getExecutionId())
                    .fragments(fragments)
                    .scanNodes(scanNodes)
                    .descTable(null)
                    .enableStreamPipeline(false)
                    .isBlockQuery(false)
                    .needReport(false)
                    .queryGlobals(queryGlobals)
                    .queryOptions(queryOptions)
                    .enablePipeline(context.getSessionVariable().isEnablePipelineEngine())
                    .resourceGroup(null)
                    .build();
        }

        private static TQueryOptions createBrokerLoadQueryOptions(LoadPlanner loadPlanner) {
            ConnectContext context = loadPlanner.getContext();
            TQueryOptions queryOptions = context.getSessionVariable().toThrift();

            queryOptions
                    .setQuery_type(TQueryType.LOAD)
                    .setQuery_timeout((int) loadPlanner.getTimeout())
                    .setLoad_mem_limit(loadPlanner.getLoadMemLimit());

            // Don't set it explicit when zero. otherwise backend will take limit as zero.
            long execMemLimit = loadPlanner.getExecMemLimit();
            if (execMemLimit > 0) {
                queryOptions.setMem_limit(execMemLimit);
                queryOptions.setQuery_mem_limit(execMemLimit);
            }

            setSessionVariablesToLoadQueryOptions(queryOptions, loadPlanner.getSessionVariables());

            return queryOptions;
        }

        private static void setSessionVariablesToLoadQueryOptions(TQueryOptions queryOptions,
                                                                  Map<String, String> sessionVariables) {
            if (sessionVariables == null) {
                return;
            }

            if (sessionVariables.containsKey(SessionVariable.LOAD_TRANSMISSION_COMPRESSION_TYPE)) {
                final TCompressionType loadCompressionType = CompressionUtils.findTCompressionByName(
                        sessionVariables.get(SessionVariable.LOAD_TRANSMISSION_COMPRESSION_TYPE));
                if (loadCompressionType != null) {
                    queryOptions.setLoad_transmission_compression_type(loadCompressionType);
                }
            }
            if (sessionVariables.containsKey(BulkLoadJob.LOG_REJECTED_RECORD_NUM_SESSION_VARIABLE_KEY)) {
                queryOptions.setLog_rejected_record_num(
                        Long.parseLong(sessionVariables.get(BulkLoadJob.LOG_REJECTED_RECORD_NUM_SESSION_VARIABLE_KEY)));
            }
        }
    }

    private JobSpec() {
    }

    @Override
    public String toString() {
        return "jobSpecrmation{" +
                "loadJobId=" + loadJobId +
                ", queryId=" + DebugUtil.printId(queryId) +
                ", enablePipeline=" + enablePipeline +
                ", enableStreamPipeline=" + enableStreamPipeline +
                ", isBlockQuery=" + isBlockQuery +
                ", resourceGroup=" + resourceGroup +
                ", warehouseId=" + warehouseId +
                '}';
    }

    public boolean isLoadType() {
        return queryOptions.getQuery_type() == TQueryType.LOAD;
    }

    public long getLoadJobId() {
        return loadJobId;
    }

    public void setLoadJobId(long loadJobId) {
        this.loadJobId = loadJobId;
    }

    public TLoadJobType getLoadJobType() {
        return queryOptions.getLoad_job_type();
    }

    public boolean isSetLoadJobId() {
        return loadJobId != UNINITIALIZED_LOAD_JOB_ID;
    }

    public TUniqueId getQueryId() {
        return queryId;
    }

    public void setQueryId(TUniqueId queryId) {
        this.queryId = queryId;
    }

    public List<PlanFragment> getFragments() {
        return fragments;
    }

    public List<ScanNode> getScanNodes() {
        return scanNodes;
    }

    public TDescriptorTable getDescTable() {
        return descTable;
    }

    public boolean isEnablePipeline() {
        return enablePipeline;
    }

    public boolean isEnableStreamPipeline() {
        return enableStreamPipeline;
    }

    public TQueryGlobals getQueryGlobals() {
        return queryGlobals;
    }

    public TQueryOptions getQueryOptions() {
        return queryOptions;
    }

    public void setLoadJobType(TLoadJobType type) {
        queryOptions.setLoad_job_type(type);
    }

    public long getStartTimeMs() {
        return this.queryGlobals.getTimestamp_ms();
    }

    public void setQueryTimeout(int timeoutSecond) {
        this.queryOptions.setQuery_timeout(timeoutSecond);
    }

    public TWorkGroup getResourceGroup() {
        return resourceGroup;
    }

    public boolean isBlockQuery() {
        return isBlockQuery;
    }

    public boolean isNeedReport() {
        return needReport;
    }

    public boolean isStatisticsJob() {
        return connectContext.isStatisticsJob();
    }

    public boolean isEnableQueue() {
        return enableQueue;
    }

    public boolean isNeedQueued() {
        return needQueued;
    }

    public boolean isEnableGroupLevelQueue() {
        return enableGroupLevelQueue;
    }

    public boolean isStreamLoad() {
        return queryOptions.getLoad_job_type() == TLoadJobType.STREAM_LOAD;
    }

    public boolean isBrokerLoad() {
        return queryOptions.getLoad_job_type() == TLoadJobType.BROKER;
    }

    public String getPlanProtocol() {
        return planProtocol;
    }

    public boolean isIncrementalScanRanges() {
        return incrementalScanRanges;
    }

    public void setIncrementalScanRanges(boolean v) {
        incrementalScanRanges = v;
    }

    public void reset() {
        fragments.forEach(PlanFragment::reset);
    }

    public SlotProvider getSlotProvider() {
        if (!isNeedQueued() || !isEnableQueue()) {
            return GlobalStateMgr.getCurrentState().getLocalSlotProvider();
        } else {
            return GlobalStateMgr.getCurrentState().getGlobalSlotProvider();
        }
    }

    public boolean hasOlapTableSink() {
        for (PlanFragment fragment : fragments) {
            if (fragment.hasOlapTableSink()) {
                return true;
            }
        }
        return isSyncStreamLoad;
    }

    public static class Builder {
        private final JobSpec instance = new JobSpec();

        public JobSpec build() {
            return instance;
        }

        public Builder commonProperties(ConnectContext context) {
            TWorkGroup newResourceGroup = prepareResourceGroup(
                    context, ResourceGroupClassifier.QueryType.fromTQueryType(instance.queryOptions.getQuery_type()));
            this.resourceGroup(newResourceGroup);

            this.enablePipeline(isEnablePipeline(context, instance.fragments));
            instance.connectContext = context;

            instance.enableQueue = isEnableQueue(context);
            instance.needQueued = needCheckQueue();
            instance.enableGroupLevelQueue = instance.enableQueue && GlobalVariable.isEnableGroupLevelQueryQueue();

            return this;
        }

        public Builder loadJobId(long loadJobId) {
            instance.loadJobId = loadJobId;
            return this;
        }

        public Builder queryId(TUniqueId queryId) {
            instance.queryId = Preconditions.checkNotNull(queryId);
            return this;
        }

        public Builder fragments(List<PlanFragment> fragments) {
            instance.fragments = fragments;
            return this;
        }

        public Builder scanNodes(List<ScanNode> scanNodes) {
            instance.scanNodes = scanNodes;
            return this;
        }

        public Builder descTable(TDescriptorTable descTable) {
            if (descTable != null) {
                descTable.setIs_cached(false);
            }
            instance.descTable = descTable;
            return this;
        }

        public Builder enableStreamPipeline(boolean enableStreamPipeline) {
            instance.enableStreamPipeline = enableStreamPipeline;
            return this;
        }

        public Builder isBlockQuery(boolean isBlockQuery) {
            instance.isBlockQuery = isBlockQuery;
            return this;
        }

        public Builder queryGlobals(TQueryGlobals queryGlobals) {
            instance.queryGlobals = queryGlobals;
            return this;
        }

        public Builder queryOptions(TQueryOptions queryOptions) {
            instance.queryOptions = queryOptions;
            return this;
        }

        private Builder enablePipeline(boolean enablePipeline) {
            instance.enablePipeline = enablePipeline;
            return this;
        }

        private Builder resourceGroup(TWorkGroup resourceGroup) {
            instance.resourceGroup = resourceGroup;
            return this;
        }

        private Builder warehouseId(long warehouseId) {
            instance.warehouseId = warehouseId;
            return this;
        }

        private Builder needReport(boolean needReport) {
            instance.needReport = needReport;
            return this;
        }

        private Builder setPlanProtocol(String planProtocol) {
            instance.planProtocol = StringUtils.lowerCase(planProtocol);
            return this;
        }

        private Builder setSyncStreamLoad() {
            instance.isSyncStreamLoad = true;
            return this;
        }

        /**
         * Whether it can use pipeline engine.
         *
         * @param connectContext It is null for broker broker export.
         * @param fragments      All the fragments need to execute.
         * @return true if enabling pipeline in the session variable and all the fragments can use pipeline,
         * otherwise false.
         */
        private static boolean isEnablePipeline(ConnectContext connectContext, List<PlanFragment> fragments) {
            return connectContext != null &&
                    connectContext.getSessionVariable().isEnablePipelineEngine() &&
                    fragments.stream().allMatch(PlanFragment::canUsePipeline);
        }

        private boolean isEnableQueue(ConnectContext connectContext) {
            if (connectContext != null && connectContext.getSessionVariable() != null &&
                    !connectContext.getSessionVariable().isEnableQueryQueue()) {
                return false;
            }
            if (instance.isStatisticsJob()) {
                return GlobalVariable.isEnableQueryQueueStatistic();
            }

            if (instance.isLoadType()) {
                return GlobalVariable.isEnableQueryQueueLoad();
            }

            return GlobalVariable.isEnableQueryQueueSelect();
        }

        private boolean needCheckQueue() {
            if (!instance.connectContext.isNeedQueued()) {
                return false;
            }

            // The queries only using schema meta will never been queued, because a MySQL client will
            // query schema meta after the connection is established.
            boolean notNeed =
                    instance.scanNodes.isEmpty() || instance.scanNodes.stream().allMatch(SchemaScanNode.class::isInstance);
            return !notNeed;
        }
    }

}
