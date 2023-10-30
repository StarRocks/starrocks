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
import org.apache.commons.lang3.StringUtils;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.starrocks.qe.CoordinatorPreprocessor.genQueryGlobals;
import static com.starrocks.qe.CoordinatorPreprocessor.prepareResourceGroup;

public class JobSpec {

    private static final long UNINITIALIZED_LOAD_JOB_ID = -1;
    private long loadJobId;

    private TUniqueId queryId;

    private final List<PlanFragment> fragments;
    private final List<ScanNode> scanNodes;
    /**
     * copied from TQueryExecRequest; constant across all fragments
     */
    private final TDescriptorTable descTable;

    private final ConnectContext connectContext;
    private final boolean enablePipeline;
    private final boolean enableStreamPipeline;
    private final boolean isBlockQuery;

    private final boolean needReport;

    /**
     * Why we use query global?
     * When `NOW()` function is in sql, we need only one now(),
     * but, we execute `NOW()` distributed.
     * So we make a query global value here to make one `now()` value in one query process.
     */
    private final TQueryGlobals queryGlobals;
    private final TQueryOptions queryOptions;
    private final TWorkGroup resourceGroup;

    private final String planProtocol;

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

            TQueryGlobals queryGlobals = genQueryGlobals(context.getStartTime(),
                    context.getSessionVariable().getTimeZone());
            if (context.getLastQueryId() != null) {
                queryGlobals.setLast_query_id(context.getLastQueryId().toString());
            }

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
                    .setPlanProtocol(context.getSessionVariable().getThriftPlanProtocol())
                    .build();
        }

        public static JobSpec fromMVMaintenanceJobSpec(ConnectContext context,
                                                       List<PlanFragment> fragments,
                                                       List<ScanNode> scanNodes,
                                                       TDescriptorTable descTable) {
            TQueryOptions queryOptions = context.getSessionVariable().toThrift();

            TQueryGlobals queryGlobals = genQueryGlobals(context.getStartTime(),
                    context.getSessionVariable().getTimeZone());
            if (context.getLastQueryId() != null) {
                queryGlobals.setLast_query_id(context.getLastQueryId().toString());
            }

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

            TQueryGlobals queryGlobals = genQueryGlobals(context.getStartTime(),
                    context.getSessionVariable().getTimeZone());
            if (context.getLastQueryId() != null) {
                queryGlobals.setLast_query_id(context.getLastQueryId().toString());
            }

            return new JobSpec.Builder()
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

            TQueryGlobals queryGlobals = genQueryGlobals(startTime, timezone);

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
                                                               long execMemLimit) {
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

            TQueryGlobals queryGlobals = genQueryGlobals(startTime, timezone);

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
                    .commonProperties(context)
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
                    .build();
        }

        public static JobSpec mockJobSpec(ConnectContext context,
                                          List<PlanFragment> fragments,
                                          List<ScanNode> scanNodes) {
            TQueryOptions queryOptions = context.getSessionVariable().toThrift();

            TQueryGlobals queryGlobals = genQueryGlobals(context.getStartTime(),
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
                    .enablePipeline(true)
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

    private JobSpec(Builder builder) {
        this.loadJobId = builder.loadJobId;

        this.queryId = builder.queryId;

        this.fragments = builder.fragments;
        this.scanNodes = builder.scanNodes;
        this.descTable = builder.descTable;

        this.enablePipeline = builder.enablePipeline;
        this.enableStreamPipeline = builder.enableStreamPipeline;
        this.isBlockQuery = builder.isBlockQuery;
        this.needReport = builder.needReport;
        this.connectContext = builder.connectContext;

        this.queryGlobals = builder.queryGlobals;
        this.queryOptions = builder.queryOptions;
        this.resourceGroup = builder.resourceGroup;
        this.planProtocol = builder.planProtocol;
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

    public boolean isNeedQueued() {
        return connectContext.isNeedQueued();
    }

    public boolean isStreamLoad() {
        return queryOptions.getLoad_job_type() == TLoadJobType.STREAM_LOAD;
    }

    public String getPlanProtocol() {
        return planProtocol;
    }

    public void reset() {
        fragments.forEach(PlanFragment::reset);
    }

    public static class Builder {
        private long loadJobId = UNINITIALIZED_LOAD_JOB_ID;

        private TUniqueId queryId;
        private List<PlanFragment> fragments;
        private List<ScanNode> scanNodes;
        private TDescriptorTable descTable;

        private boolean enablePipeline;
        private boolean enableStreamPipeline;
        private boolean isBlockQuery;
        private boolean needReport;
        private ConnectContext connectContext;

        private TQueryGlobals queryGlobals;
        private TQueryOptions queryOptions;
        private TWorkGroup resourceGroup;
        private String planProtocol;

        public JobSpec build() {
            return new JobSpec(this);
        }

        public Builder commonProperties(ConnectContext context) {
            TWorkGroup newResourceGroup = prepareResourceGroup(
                    context, ResourceGroupClassifier.QueryType.fromTQueryType(queryOptions.getQuery_type()));
            this.resourceGroup(newResourceGroup);

            this.enablePipeline(isEnablePipeline(context, fragments));
            this.connectContext = context;

            return this;
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
            if (descTable != null) {
                descTable.setIs_cached(false);
            }
            this.descTable = descTable;
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

        public Builder queryGlobals(TQueryGlobals queryGlobals) {
            this.queryGlobals = queryGlobals;
            return this;
        }

        public Builder queryOptions(TQueryOptions queryOptions) {
            this.queryOptions = queryOptions;
            return this;
        }

        private Builder enablePipeline(boolean enablePipeline) {
            this.enablePipeline = enablePipeline;
            return this;
        }

        private Builder resourceGroup(TWorkGroup resourceGroup) {
            this.resourceGroup = resourceGroup;
            return this;
        }

        private Builder needReport(boolean needReport) {
            this.needReport = needReport;
            return this;
        }

        private Builder setPlanProtocol(String planProtocol) {
            this.planProtocol = StringUtils.lowerCase(planProtocol);
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
    }

}
