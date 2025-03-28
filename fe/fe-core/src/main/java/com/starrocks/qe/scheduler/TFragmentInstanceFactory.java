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

package com.starrocks.qe.scheduler;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.planner.ExportSink;
import com.starrocks.planner.MultiCastPlanFragment;
import com.starrocks.planner.PlanFragment;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.qe.scheduler.dag.ExecutionDAG;
import com.starrocks.qe.scheduler.dag.ExecutionFragment;
import com.starrocks.qe.scheduler.dag.FragmentInstance;
import com.starrocks.qe.scheduler.dag.JobSpec;
import com.starrocks.sql.common.QueryDebugOptions;
import com.starrocks.thrift.InternalServiceVersion;
import com.starrocks.thrift.TAdaptiveDopParam;
import com.starrocks.thrift.TDescriptorTable;
import com.starrocks.thrift.TExecPlanFragmentParams;
import com.starrocks.thrift.TFunctionVersion;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TPlanFragmentDestination;
import com.starrocks.thrift.TPlanFragmentExecParams;
import com.starrocks.thrift.TPredicateTreeParams;
import com.starrocks.thrift.TQueryOptions;
import com.starrocks.thrift.TQueryQueueOptions;
import org.apache.commons.collections4.CollectionUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

public class TFragmentInstanceFactory {
    private final ConnectContext context;
    private final JobSpec jobSpec;
    private final ExecutionDAG executionDAG;
    private final TNetworkAddress coordAddress;

    public TFragmentInstanceFactory(ConnectContext context,
                                    JobSpec jobSpec,
                                    ExecutionDAG executionDAG,
                                    TNetworkAddress coordAddress) {
        this.jobSpec = jobSpec;
        this.executionDAG = executionDAG;
        this.context = context;
        this.coordAddress = coordAddress;
    }

    public List<TExecPlanFragmentParams> create(ExecutionFragment fragment,
                                                List<FragmentInstance> instances,
                                                TDescriptorTable descTable,
                                                int accTabletSinkDop,
                                                int totalTableSinkDop) {
        // if pipeline is enable and current fragment contain olap table sink, in fe we will
        // calculate the number of all tablet sinks in advance and assign them to each fragment instance
        boolean enablePipelineTableSinkDop = jobSpec.isEnablePipeline() && fragment.getPlanFragment().hasTableSink();

        List<TExecPlanFragmentParams> res = new ArrayList<>(instances.size());
        for (FragmentInstance instance : instances) {
            res.add(create(instance, descTable, accTabletSinkDop, totalTableSinkDop));

            if (enablePipelineTableSinkDop) {
                accTabletSinkDop += instance.getTableSinkDop();
            }
        }

        return res;
    }

    public TExecPlanFragmentParams create(FragmentInstance instance,
                                          TDescriptorTable descTable,
                                          int accTabletSinkDop,
                                          int totalTableSinkDop) {
        TExecPlanFragmentParams result = new TExecPlanFragmentParams();

        toThriftFromCommonParams(result, instance.getExecFragment(), descTable, totalTableSinkDop);
        toThriftForUniqueParams(result, instance, accTabletSinkDop);

        return result;
    }

    public TExecPlanFragmentParams createIncrementalScanRanges(FragmentInstance instance) {
        TExecPlanFragmentParams result = new TExecPlanFragmentParams();
        result.setProtocol_version(InternalServiceVersion.V1);
        result.setParams(new TPlanFragmentExecParams());
        result.params.setQuery_id(jobSpec.getQueryId());
        result.params.setFragment_instance_id(instance.getInstanceId());
        result.params.setPer_node_scan_ranges(instance.getNode2ScanRanges());
        result.params.setNode_to_per_driver_seq_scan_ranges(instance.getNode2DriverSeqToScanRanges());
        result.params.setPer_exch_num_senders(new HashMap<>());
        return result;
    }

    public void toThriftFromCommonParams(TExecPlanFragmentParams result,
                                         ExecutionFragment execFragment,
                                         TDescriptorTable descTable,
                                         int totalTableSinkDop) {
        // TODO(lzh): move to a more proper place.
        execFragment.setLayoutInfosForRuntimeFilters();

        PlanFragment fragment = execFragment.getPlanFragment();

        boolean isEnablePipeline = jobSpec.isEnablePipeline();
        boolean isEnablePipelineTableSinkDop = isEnablePipeline && fragment.hasTableSink();

        result.setProtocol_version(InternalServiceVersion.V1);
        result.setFragment(fragment.toThrift());
        result.setDesc_tbl(descTable);
        result.setFunc_version(TFunctionVersion.RUNTIME_FILTER_SERIALIZE_VERSION_3.getValue());
        result.setCoord(coordAddress);

        result.setParams(new TPlanFragmentExecParams());
        result.params.setUse_vectorized(true);
        result.params.setQuery_id(jobSpec.getQueryId());
        result.params.setDestinations(execFragment.getDestinations());
        if (isEnablePipelineTableSinkDop) {
            result.params.setNum_senders(totalTableSinkDop);
        } else {
            result.params.setNum_senders(execFragment.getInstances().size());
        }
        result.setIs_stream_pipeline(jobSpec.isEnableStreamPipeline());
        result.params.setPer_exch_num_senders(execFragment.getNumSendersPerExchange());
        if (execFragment.getRuntimeFilterParams().isSetRuntime_filter_builder_number()) {
            result.params.setRuntime_filter_params(execFragment.getRuntimeFilterParams());
        }
        result.params.setSend_query_statistics_with_every_batch(fragment.isTransferQueryStatisticsWithEveryBatch());

        result.setQuery_globals(jobSpec.getQueryGlobals());
        if (isEnablePipeline) {
            result.setQuery_options(new TQueryOptions(jobSpec.getQueryOptions()));
        } else {
            result.setQuery_options(jobSpec.getQueryOptions());
        }
        // For broker load, the ConnectContext.get() is null
        if (context != null) {
            SessionVariable sessionVariable = context.getSessionVariable();
            final List<QueryDebugOptions.ExecDebugOption> execDebugOptions =
                    sessionVariable.getQueryDebugOptions().getExecDebugOptions();

            if (isEnablePipeline) {
                result.setIs_pipeline(true);
                result.getQuery_options().setBatch_size(sessionVariable.getChunkSize());
                result.setEnable_shared_scan(sessionVariable.isEnableSharedScan());
                result.params.setEnable_exchange_pass_through(sessionVariable.isEnableExchangePassThrough());
                result.params.setEnable_exchange_perf(sessionVariable.isEnableExchangePerf());
                for (QueryDebugOptions.ExecDebugOption option : execDebugOptions) {
                    result.params.addToExec_debug_options(option.toThirft());
                }

                result.setEnable_resource_group(true);
                if (jobSpec.getResourceGroup() != null) {
                    result.setWorkgroup(jobSpec.getResourceGroup());
                }
                if (fragment.isUseRuntimeAdaptiveDop()) {
                    result.setAdaptive_dop_param(new TAdaptiveDopParam());
                    result.adaptive_dop_param.setMax_block_rows_per_driver_seq(
                            sessionVariable.getAdaptiveDopMaxBlockRowsPerDriverSeq());
                    result.adaptive_dop_param.setMax_output_amplification_factor(
                            sessionVariable.getAdaptiveDopMaxOutputAmplificationFactor());
                }
                if (jobSpec.isEnableQueue()) {
                    TQueryQueueOptions queryQueueOptions = new TQueryQueueOptions();
                    queryQueueOptions.setEnable_global_query_queue(jobSpec.isEnableQueue());
                    queryQueueOptions.setEnable_group_level_query_queue(jobSpec.isEnableGroupLevelQueue());

                    TQueryOptions queryOptions = result.getQuery_options();
                    queryOptions.setQuery_queue_options(queryQueueOptions);
                }

                result.setPred_tree_params(new TPredicateTreeParams());
                result.pred_tree_params.setEnable_or(sessionVariable.isEnablePushdownOrPredicate());
                result.pred_tree_params.setEnable_show_in_profile(sessionVariable.isEnableShowPredicateTreeInProfile());

                if (CollectionUtils.isNotEmpty(fragment.getCollectExecStatsIds())) {
                    result.setExec_stats_node_ids(fragment.getCollectExecStatsIds());
                }
            }
        }
    }

    private void toThriftForUniqueParams(TExecPlanFragmentParams result,
                                         FragmentInstance instance,
                                         int accTabletSinkDop) {
        ExecutionFragment execFragment = instance.getExecFragment();
        PlanFragment fragment = execFragment.getPlanFragment();

        boolean isEnablePipeline = jobSpec.isEnablePipeline();
        boolean isEnablePipelineTableSinkDop = isEnablePipeline && fragment.hasTableSink();

        result.setBackend_num(instance.getIndexInJob());
        if (isEnablePipeline) {
            result.setPipeline_dop(instance.getPipelineDop());
            result.setGroup_execution_scan_dop(instance.getGroupExecutionScanDop());
        }

        // Add instance number in file name prefix when export job.
        if (fragment.getSink() instanceof ExportSink) {
            ExportSink exportSink = (ExportSink) fragment.getSink();
            if (exportSink.getFileNamePrefix() != null) {
                exportSink.setFileNamePrefix(exportSink.getFileNamePrefix() + instance.getIndexInFragment() + "_");
            }
        }

        // For MultiCastDataFragment, output only send to local, and the instance is keep
        // same with MultiCastDataFragment.
        if (fragment instanceof MultiCastPlanFragment) {
            List<List<TPlanFragmentDestination>> multiFragmentDestinations =
                    result.getFragment().getOutput_sink().getMulti_cast_stream_sink().getDestinations();
            List<List<TPlanFragmentDestination>> newDestinations = Lists.newArrayList();
            for (List<TPlanFragmentDestination> destinations : multiFragmentDestinations) {
                Preconditions.checkState(execFragment.getInstances().size() == destinations.size());
                TPlanFragmentDestination ndes = destinations.get(instance.getIndexInFragment());

                Preconditions.checkState(ndes.getDeprecated_server().equals(instance.getWorker().getAddress()));
                newDestinations.add(Collections.singletonList(ndes));
            }

            result.getFragment().getOutput_sink().getMulti_cast_stream_sink()
                    .setDestinations(newDestinations);
        }

        result.params.setInstances_number(executionDAG.getNumInstancesOfWorkerId(instance.getWorkerId()));
        result.params.setFragment_instance_id(instance.getInstanceId());
        result.params.setPer_node_scan_ranges(instance.getNode2ScanRanges());
        result.params.setNode_to_per_driver_seq_scan_ranges(instance.getNode2DriverSeqToScanRanges());
        result.params.setReport_when_finish(execFragment.isNeedReportFragmentFinish());

        if (isEnablePipelineTableSinkDop) {
            result.params.setSender_id(accTabletSinkDop);
            result.params.setPipeline_sink_dop(instance.getTableSinkDop());
        } else {
            result.params.setSender_id(instance.getIndexInFragment());
        }
    }
}
