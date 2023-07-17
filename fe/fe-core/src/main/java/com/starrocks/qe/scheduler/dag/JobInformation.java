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
import com.starrocks.planner.StreamLoadPlanner;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.CoordinatorPreprocessor;
import com.starrocks.qe.SessionVariable;
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

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.starrocks.qe.CoordinatorPreprocessor.genQueryGlobals;
import static com.starrocks.qe.CoordinatorPreprocessor.prepareResourceGroup;

public class JobInformation {

    public static final long UNINITIALIZED_LOAD_JOB_ID = -1;
    private long loadJobId;

    private TUniqueId queryId;

    private final List<PlanFragment> fragments;
    private final List<ScanNode> scanNodes;
    // copied from TQueryExecRequest; constant across all fragments
    private final TDescriptorTable descTable;

    private final boolean enablePipeline;
    private final boolean enableStreamPipeline;
    private final boolean isBlockQuery;
    private final boolean enableTraceLog;

    // Why we use query global?
    // When `NOW()` function is in sql, we need only one now(),
    // but, we execute `NOW()` distributed.
    // So we make a query global value here to make one `now()` value in one query process.
    private final TQueryGlobals queryGlobals;
    private final TQueryOptions queryOptions;
    private final TWorkGroup resourceGroup;

    public JobInformation(Builder builder) {
        this.loadJobId = builder.loadJobId;

        this.queryId = builder.queryId;

        this.fragments = builder.fragments;
        this.scanNodes = builder.scanNodes;
        this.descTable = builder.descTable.setIs_cached(false);

        this.enablePipeline = builder.enablePipeline;
        this.enableStreamPipeline = builder.enableStreamPipeline;
        this.isBlockQuery = builder.isBlockQuery;
        this.enableTraceLog = builder.enableTraceLog;

        this.queryGlobals = builder.queryGlobals;
        this.queryOptions = builder.queryOptions;
        this.resourceGroup = builder.resourceGroup;
    }

    @Override
    public String toString() {
        return "JobInformation{" +
                "loadJobId=" + loadJobId +
                ", queryId=" + DebugUtil.printId(queryId) +
                ", enablePipeline=" + enablePipeline +
                ", enableStreamPipeline=" + enableStreamPipeline +
                ", isBlockQuery=" + isBlockQuery +
                ", resourceGroup=" + resourceGroup +
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

    public TWorkGroup getResourceGroup() {
        return resourceGroup;
    }

    public boolean isBlockQuery() {
        return isBlockQuery;
    }

    public boolean isStreamLoad() {
        return queryOptions.getLoad_job_type() == TLoadJobType.STREAM_LOAD;
    }

    public boolean isEnableTraceLog() {
        return enableTraceLog;
    }

    public void reset() {
        fragments.forEach(PlanFragment::reset);
    }

    public static class Factory {
        public static JobInformation createQueryJobInformation(ConnectContext context,
                                                               List<PlanFragment> fragments,
                                                               List<ScanNode> scanNodes,
                                                               TDescriptorTable descTable,
                                                               TQueryType queryType) {
            TQueryOptions queryOptions = context.getSessionVariable().toThrift();
            queryOptions.setQuery_type(queryType);

            TQueryGlobals queryGlobals = createQueryGlobals(
                    context,
                    context.getStartTime(),
                    context.getSessionVariable().getTimeZone());

            TWorkGroup resourceGroup = prepareResourceGroup(
                    context, ResourceGroupClassifier.QueryType.fromTQueryType(queryOptions.getQuery_type()));

            return new JobInformation.Builder()
                    .loadJobId(UNINITIALIZED_LOAD_JOB_ID)
                    .queryId(context.getExecutionId())
                    .fragments(fragments)
                    .scanNodes(scanNodes)
                    .descTable(descTable)
                    .enablePipeline(isEnablePipeline(context, fragments))
                    .enableStreamPipeline(false)
                    .enableTraceLog(context.getSessionVariable().isEnableSchedulerTraceLog())
                    .isBlockQuery(false)
                    .queryGlobals(queryGlobals)
                    .queryOptions(queryOptions)
                    .resourceGroup(resourceGroup)
                    .build();
        }

        public static JobInformation createBrokerLoadJobInformation(LoadPlanner loadPlanner) {
            ConnectContext context = loadPlanner.getContext();
            List<PlanFragment> fragments = loadPlanner.getFragments();
            TQueryOptions queryOptions = createBrokerLoadQueryOptions(loadPlanner);
            TQueryGlobals queryGlobals = createQueryGlobals(
                    context,
                    context.getStartTime(),
                    context.getSessionVariable().getTimeZone());
            TWorkGroup resourceGroup = prepareResourceGroup(
                    context, ResourceGroupClassifier.QueryType.fromTQueryType(queryOptions.getQuery_type()));

            return new JobInformation.Builder()
                    .loadJobId(loadPlanner.getLoadJobId())
                    .queryId(loadPlanner.getLoadId())
                    .fragments(fragments)
                    .scanNodes(loadPlanner.getScanNodes())
                    .descTable(loadPlanner.getDescTable().toThrift())
                    .enablePipeline(isEnablePipeline(context, fragments))
                    .enableStreamPipeline(false)
                    .enableTraceLog(context.getSessionVariable().isEnableSchedulerTraceLog())
                    .isBlockQuery(true)
                    .queryGlobals(queryGlobals)
                    .queryOptions(queryOptions)
                    .resourceGroup(resourceGroup)
                    .build();
        }

        public static JobInformation createStreamLoadJobInformation(LoadPlanner loadPlanner) {
            JobInformation jobInformation = createBrokerLoadJobInformation(loadPlanner);
            jobInformation.getQueryOptions().setLoad_job_type((TLoadJobType.STREAM_LOAD));
            return jobInformation;
        }

        public static JobInformation createFakeJobInformation(StreamLoadPlanner planner) {
            ConnectContext context = planner.getConnectContext();
            boolean enableTraceLog = context.getSessionVariable().isEnableSchedulerTraceLog();

            TExecPlanFragmentParams params = planner.getExecPlanFragmentParams();
            TUniqueId queryId = params.getParams().getFragment_instance_id();

            return new Builder()
                    .loadJobId(UNINITIALIZED_LOAD_JOB_ID)
                    .queryId(queryId)
                    .fragments(Collections.emptyList())
                    .scanNodes(null)
                    .descTable(null)
                    .enablePipeline(false)
                    .enableStreamPipeline(false)
                    .enableTraceLog(enableTraceLog)
                    .isBlockQuery(true)
                    .queryGlobals(null)
                    .queryOptions(null)
                    .resourceGroup(null)
                    .build();
        }

        public static JobInformation createBrokerExportScheduler(ConnectContext context,
                                                                 Long loadJobId, TUniqueId queryId,
                                                                 DescriptorTable descTable,
                                                                 List<PlanFragment> fragments,
                                                                 List<ScanNode> scanNodes, String timezone, long startTime,
                                                                 Map<String, String> sessionVariables, long execMemLimit) {
            TQueryOptions queryOptions = new TQueryOptions();
            setSessionVariablesToQueryOptions(queryOptions, sessionVariables);
            queryOptions.setMem_limit(execMemLimit);

            TQueryGlobals queryGlobals = CoordinatorPreprocessor.genQueryGlobals(startTime, timezone);
            TWorkGroup resourceGroup = prepareResourceGroup(
                    context, ResourceGroupClassifier.QueryType.fromTQueryType(queryOptions.getQuery_type()));

            return new JobInformation.Builder()
                    .loadJobId(loadJobId)
                    .queryId(queryId)
                    .fragments(fragments)
                    .scanNodes(scanNodes)
                    .descTable(descTable.toThrift())
                    .enablePipeline(isEnablePipeline(context, fragments))
                    .enableStreamPipeline(false)
                    .isBlockQuery(true)
                    .enableTraceLog(context.getSessionVariable().isEnableSchedulerTraceLog())
                    .queryGlobals(queryGlobals)
                    .queryOptions(queryOptions)
                    .resourceGroup(resourceGroup)
                    .build();
        }

        public static JobInformation createNonPipelineBrokerLoadJobInformation(ConnectContext context,
                                                                               Long loadJobId, TUniqueId queryId,
                                                                               DescriptorTable descTable,
                                                                               List<PlanFragment> fragments,
                                                                               List<ScanNode> scanNodes, String timezone,
                                                                               long startTime,
                                                                               Map<String, String> sessionVariables,
                                                                               long execMemLimit) {
            TQueryOptions queryOptions = new TQueryOptions();
            setSessionVariablesToQueryOptions(queryOptions, sessionVariables);
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

            TQueryGlobals queryGlobals = CoordinatorPreprocessor.genQueryGlobals(startTime, timezone);
            TWorkGroup resourceGroup = prepareResourceGroup(
                    context, ResourceGroupClassifier.QueryType.fromTQueryType(queryOptions.getQuery_type()));

            return new JobInformation.Builder()
                    .loadJobId(loadJobId)
                    .queryId(queryId)
                    .fragments(fragments)
                    .scanNodes(scanNodes)
                    .descTable(descTable.toThrift())
                    .enablePipeline(isEnablePipeline(context, fragments))
                    .enableStreamPipeline(false)
                    .isBlockQuery(true)
                    .enableTraceLog(context.getSessionVariable().isEnableSchedulerTraceLog())
                    .queryGlobals(queryGlobals)
                    .queryOptions(queryOptions)
                    .resourceGroup(resourceGroup)
                    .build();
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

        private static TQueryGlobals createQueryGlobals(ConnectContext context, long startTime, String timezone) {
            TQueryGlobals queryGlobals = genQueryGlobals(startTime, timezone);
            if (context.getLastQueryId() != null) {
                queryGlobals.setLast_query_id(context.getLastQueryId().toString());
            }
            return queryGlobals;
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

            setSessionVariablesToQueryOptions(queryOptions, loadPlanner.getSessionVariables());

            return queryOptions;
        }

        private static void setSessionVariablesToQueryOptions(TQueryOptions queryOptions, Map<String, String> sessionVariables) {
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

    public static class Builder {
        private long loadJobId;

        private TUniqueId queryId;

        private List<PlanFragment> fragments;
        private List<ScanNode> scanNodes;
        private TDescriptorTable descTable;

        private boolean enablePipeline;
        private boolean enableStreamPipeline;
        private boolean isBlockQuery;

        private boolean enableTraceLog;

        private TQueryGlobals queryGlobals;
        private TQueryOptions queryOptions;
        private TWorkGroup resourceGroup;

        public JobInformation build() {
            return new JobInformation(this);
        }

        public Builder loadJobId(long loadJobId) {
            this.loadJobId = loadJobId;
            return this;
        }

        public Builder queryId(TUniqueId queryId) {
            this.queryId = Preconditions.checkNotNull(queryId);
            return this;
        }

        public Builder fragments(List<PlanFragment> fragments) {
            this.fragments = fragments;
            return this;
        }

        public Builder scanNodes(List<ScanNode> scanNodes) {
            this.scanNodes = scanNodes;
            return this;
        }

        public Builder descTable(TDescriptorTable descTable) {
            this.descTable = descTable;
            return this;
        }

        public Builder enablePipeline(boolean enablePipeline) {
            this.enablePipeline = enablePipeline;
            return this;
        }

        public Builder enableStreamPipeline(boolean enableStreamPipeline) {
            this.enableStreamPipeline = enableStreamPipeline;
            return this;
        }

        public Builder isBlockQuery(boolean isBlockQuery) {
            this.isBlockQuery = isBlockQuery;
            return this;
        }

        public Builder enableTraceLog(boolean enableTraceLog) {
            this.enableTraceLog = enableTraceLog;
            return this;
        }

        public Builder queryGlobals(TQueryGlobals queryGlobals) {
            this.queryGlobals = queryGlobals;
            return this;
        }

        public Builder queryOptions(TQueryOptions queryOptions) {
            this.queryOptions = queryOptions;
            return this;
        }

        public Builder resourceGroup(TWorkGroup resourceGroup) {
            this.resourceGroup = resourceGroup;
            return this;
        }
    }

}
