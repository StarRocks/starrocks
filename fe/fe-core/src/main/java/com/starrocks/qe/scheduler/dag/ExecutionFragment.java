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
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.planner.ExportSink;
import com.starrocks.planner.MultiCastPlanFragment;
import com.starrocks.planner.PlanFragment;
import com.starrocks.planner.RuntimeFilterDescription;
import com.starrocks.qe.ColocatedBackendSelector;
import com.starrocks.qe.CoordinatorPreprocessor;
import com.starrocks.qe.FragmentScanRangeAssignment;
import com.starrocks.qe.SessionVariable;
import com.starrocks.thrift.InternalServiceVersion;
import com.starrocks.thrift.TAdaptiveDopParam;
import com.starrocks.thrift.TDescriptorTable;
import com.starrocks.thrift.TEsScanRange;
import com.starrocks.thrift.TExecPlanFragmentParams;
import com.starrocks.thrift.TFunctionVersion;
import com.starrocks.thrift.THdfsScanRange;
import com.starrocks.thrift.TInternalScanRange;
import com.starrocks.thrift.TPlanFragmentDestination;
import com.starrocks.thrift.TPlanFragmentExecParams;
import com.starrocks.thrift.TQueryOptions;
import com.starrocks.thrift.TRuntimeFilterParams;
import com.starrocks.thrift.TScanRangeParams;
import com.starrocks.thrift.TUniqueId;

import java.util.List;
import java.util.Map;
import java.util.Set;

// execution parameters for a single fragment,
// per-fragment can have multiple FInstanceExecParam,
// used to assemble TPlanFragmentExecParas
public class ExecutionFragment {
    public PlanFragment fragment;
    public List<TPlanFragmentDestination> destinations = Lists.newArrayList();
    public Map<Integer, Integer> perExchNumSenders = Maps.newHashMap();

    public List<FragmentInstance> instanceExecParams = Lists.newArrayList();
    public FragmentScanRangeAssignment scanRangeAssignment = new FragmentScanRangeAssignment();
    private ColocatedBackendSelector.Assignment colocatedAssignment = null;
    TRuntimeFilterParams runtimeFilterParams = new TRuntimeFilterParams();
    public boolean bucketSeqToInstanceForFilterIsSet = false;

    public ExecutionFragment(PlanFragment fragment) {
        this.fragment = fragment;
    }

    void setBucketSeqToInstanceForRuntimeFilters() {
        if (bucketSeqToInstanceForFilterIsSet) {
            return;
        }
        bucketSeqToInstanceForFilterIsSet = true;
        List<Integer> seqToInstance = fragmentIdToSeqToInstanceMap.get(fragment.getFragmentId());
        if (seqToInstance == null || seqToInstance.isEmpty()) {
            return;
        }
        for (RuntimeFilterDescription rf : fragment.getBuildRuntimeFilters().values()) {
            if (!rf.isColocateOrBucketShuffle()) {
                continue;
            }
            rf.setBucketSeqToInstance(seqToInstance);
        }
    }

    /**
     * Set the common fields of all the fragment instances to the destination common thrift params.
     *
     * @param commonParams           The destination common thrift params.
     * @param workerId               The destination id to delivery these instances.
     * @param descTable              The descriptor table, empty for the non-first instance
     *                               when enable pipeline and disable multi fragments in one request.
     * @param isEnablePipelineEngine Whether enable pipeline engine.
     */
    private void toThriftForCommonParams(TExecPlanFragmentParams commonParams,
                                         long workerId, TDescriptorTable descTable,
                                         boolean isEnablePipelineEngine, int tableSinkTotalDop,
                                         boolean isEnableStreamPipeline) {
        boolean enablePipelineTableSinkDop = isEnablePipelineEngine &&
                (fragment.hasOlapTableSink() || fragment.hasIcebergTableSink());
        commonParams.setProtocol_version(InternalServiceVersion.V1);
        commonParams.setFragment(fragment.toThrift());
        commonParams.setDesc_tbl(descTable);
        commonParams.setFunc_version(TFunctionVersion.RUNTIME_FILTER_SERIALIZE_VERSION_2.getValue());
        commonParams.setCoord(coordAddress);

        commonParams.setParams(new TPlanFragmentExecParams());
        commonParams.params.setUse_vectorized(true);
        commonParams.params.setQuery_id(jobSpec.getQueryId());
        commonParams.params.setInstances_number(workerIdToNumInstances.get(workerId));
        commonParams.params.setDestinations(destinations);
        if (enablePipelineTableSinkDop) {
            commonParams.params.setNum_senders(tableSinkTotalDop);
        } else {
            commonParams.params.setNum_senders(instanceExecParams.size());
        }
        commonParams.setIs_stream_pipeline(isEnableStreamPipeline);
        commonParams.params.setPer_exch_num_senders(perExchNumSenders);
        if (runtimeFilterParams.isSetRuntime_filter_builder_number()) {
            commonParams.params.setRuntime_filter_params(runtimeFilterParams);
        }
        commonParams.params.setSend_query_statistics_with_every_batch(
                fragment.isTransferQueryStatisticsWithEveryBatch());

        commonParams.setQuery_globals(jobSpec.getQueryGlobals());
        if (isEnablePipelineEngine) {
            commonParams.setQuery_options(new TQueryOptions(jobSpec.getQueryOptions()));
        } else {
            commonParams.setQuery_options(jobSpec.getQueryOptions());
        }
        // For broker load, the ConnectContext.get() is null
        if (connectContext != null) {
            SessionVariable sessionVariable = connectContext.getSessionVariable();

            if (isEnablePipelineEngine) {
                commonParams.setIs_pipeline(true);
                commonParams.getQuery_options().setBatch_size(SessionVariable.PIPELINE_BATCH_SIZE);
                commonParams.setEnable_shared_scan(sessionVariable.isEnableSharedScan());
                commonParams.params.setEnable_exchange_pass_through(sessionVariable.isEnableExchangePassThrough());
                commonParams.params.setEnable_exchange_perf(sessionVariable.isEnableExchangePerf());

                commonParams.setEnable_resource_group(true);
                if (jobSpec.getResourceGroup() != null) {
                    commonParams.setWorkgroup(jobSpec.getResourceGroup());
                }
                if (fragment.isUseRuntimeAdaptiveDop()) {
                    commonParams.setAdaptive_dop_param(new TAdaptiveDopParam());
                    commonParams.adaptive_dop_param.setMax_block_rows_per_driver_seq(
                            sessionVariable.getAdaptiveDopMaxBlockRowsPerDriverSeq());
                    commonParams.adaptive_dop_param.setMax_output_amplification_factor(
                            sessionVariable.getAdaptiveDopMaxOutputAmplificationFactor());
                }
            }

        }
    }

    /**
     * Set the unique fields for a fragment instance to the destination unique thrift params, including:
     * - backend_num
     * - pipeline_dop (used when isEnablePipelineEngine is true)
     * - params.fragment_instance_id
     * - params.sender_id
     * - params.per_node_scan_ranges
     * - fragment.output_sink (only for MultiCastDataStreamSink and ExportSink)
     *
     * @param uniqueParams         The destination unique thrift params.
     * @param fragmentIndex        The index of this instance in this.instanceExecParams.
     * @param instanceExecParam    The instance param.
     * @param enablePipelineEngine Whether enable pipeline engine.
     */
    private void toThriftForUniqueParams(TExecPlanFragmentParams uniqueParams, int fragmentIndex,
                                         CoordinatorPreprocessor.FragmentInstance instanceExecParam, boolean enablePipelineEngine,
                                         int accTabletSinkDop, int curTableSinkDop) {
        // if pipeline is enable and current fragment contain olap table sink, in fe we will
        // calculate the number of all tablet sinks in advance and assign them to each fragment instance
        boolean enablePipelineTableSinkDop = enablePipelineEngine &&
                (fragment.hasOlapTableSink() || fragment.hasIcebergTableSink());

        uniqueParams.setProtocol_version(InternalServiceVersion.V1);
        uniqueParams.setBackend_num(instanceExecParam.backendNum);
        if (enablePipelineEngine) {
            if (instanceExecParam.isSetPipelineDop()) {
                uniqueParams.setPipeline_dop(instanceExecParam.pipelineDop);
            } else {
                uniqueParams.setPipeline_dop(fragment.getPipelineDop());
            }
        }

        /// Set thrift fragment with the unique fields.

        // Add instance number in file name prefix when export job.
        if (fragment.getSink() instanceof ExportSink) {
            ExportSink exportSink = (ExportSink) fragment.getSink();
            if (exportSink.getFileNamePrefix() != null) {
                exportSink.setFileNamePrefix(exportSink.getFileNamePrefix() + fragmentIndex + "_");
            }
        }
        if (!uniqueParams.isSetFragment()) {
            uniqueParams.setFragment(fragment.toThriftForUniqueFields());
        }
        /*
         * For MultiCastDataFragment, output only send to local, and the instance is keep
         * same with MultiCastDataFragment
         * */
        if (fragment instanceof MultiCastPlanFragment) {
            List<List<TPlanFragmentDestination>> multiFragmentDestinations =
                    uniqueParams.getFragment().getOutput_sink().getMulti_cast_stream_sink().getDestinations();
            List<List<TPlanFragmentDestination>> newDestinations = Lists.newArrayList();
            for (List<TPlanFragmentDestination> destinations : multiFragmentDestinations) {
                Preconditions.checkState(instanceExecParams.size() == destinations.size());
                TPlanFragmentDestination ndes = destinations.get(fragmentIndex);

                newDestinations.add(Lists.newArrayList(ndes));
            }

            uniqueParams.getFragment().getOutput_sink().getMulti_cast_stream_sink()
                    .setDestinations(newDestinations);
        }

        if (!uniqueParams.isSetParams()) {
            uniqueParams.setParams(new TPlanFragmentExecParams());
        }
        uniqueParams.params.setFragment_instance_id(instanceExecParam.instanceId);

        Map<Integer, List<TScanRangeParams>> scanRanges = instanceExecParam.perNodeScanRanges;
        if (scanRanges == null) {
            scanRanges = Maps.newHashMap();
        }
        uniqueParams.params.setPer_node_scan_ranges(scanRanges);
        uniqueParams.params.setNode_to_per_driver_seq_scan_ranges(instanceExecParam.nodeToPerDriverSeqScanRanges);

        if (enablePipelineTableSinkDop) {
            uniqueParams.params.setSender_id(accTabletSinkDop);
            uniqueParams.params.setPipeline_sink_dop(curTableSinkDop);
        } else {
            uniqueParams.params.setSender_id(fragmentIndex);
        }
    }

    public List<TExecPlanFragmentParams> toThrift(Set<TUniqueId> inFlightInstanceIds,
                                                  TDescriptorTable descTable,
                                                  boolean enablePipelineEngine,
                                                  int accTabletSinkDop,
                                                  int tableSinkTotalDop,
                                                  boolean isEnableStreamPipeline) {
        setBucketSeqToInstanceForRuntimeFilters();

        List<TExecPlanFragmentParams> paramsList = Lists.newArrayList();
        for (int i = 0; i < instanceExecParams.size(); ++i) {
            final CoordinatorPreprocessor.FragmentInstance instanceExecParam = instanceExecParams.get(i);
            if (!inFlightInstanceIds.contains(instanceExecParam.instanceId)) {
                continue;
            }
            int curTableSinkDop = instanceExecParam.getTableSinkDop();
            TExecPlanFragmentParams params = new TExecPlanFragmentParams();

            toThriftForCommonParams(params, instanceExecParam.getWorkerId(), descTable, enablePipelineEngine,
                    tableSinkTotalDop, isEnableStreamPipeline);
            toThriftForUniqueParams(params, i, instanceExecParam, enablePipelineEngine,
                    accTabletSinkDop, curTableSinkDop);

            paramsList.add(params);
            accTabletSinkDop += curTableSinkDop;
        }
        return paramsList;
    }

    // Append range information
    // [tablet_id(version),tablet_id(version)]
    public void appendScanRange(StringBuilder sb, List<TScanRangeParams> params) {
        sb.append("range=[");
        int idx = 0;
        for (TScanRangeParams range : params) {
            TInternalScanRange internalScanRange = range.getScan_range().getInternal_scan_range();
            if (internalScanRange != null) {
                if (idx++ != 0) {
                    sb.append(",");
                }
                sb.append("{tid=").append(internalScanRange.getTablet_id())
                        .append(",ver=").append(internalScanRange.getVersion()).append("}");
            }
            TEsScanRange esScanRange = range.getScan_range().getEs_scan_range();
            if (esScanRange != null) {
                sb.append("{ index=").append(esScanRange.getIndex())
                        .append(", shardid=").append(esScanRange.getShard_id())
                        .append("}");
            }
            THdfsScanRange hdfsScanRange = range.getScan_range().getHdfs_scan_range();
            if (hdfsScanRange != null) {
                sb.append("{relative_path=").append(hdfsScanRange.getRelative_path())
                        .append(", offset=").append(hdfsScanRange.getOffset())
                        .append(", length=").append(hdfsScanRange.getLength())
                        .append("}");
            }
        }
        sb.append("]");
    }

    public void appendTo(StringBuilder sb) {
        // append fragment
        sb.append("{plan=");
        fragment.getPlanRoot().appendTrace(sb);
        sb.append(",instance=[");
        // append instance
        for (int i = 0; i < instanceExecParams.size(); ++i) {
            if (i != 0) {
                sb.append(",");
            }

            CoordinatorPreprocessor.FragmentInstance instanceExecParam = instanceExecParams.get(i);

            Map<Integer, List<TScanRangeParams>> scanRanges =
                    scanRangeAssignment.get(instanceExecParam.getWorkerId());
            sb.append("{");
            sb.append("id=").append(DebugUtil.printId(instanceExecParams.get(i).instanceId));
            sb.append(",host=").append(getAddress(instanceExecParam.getWorkerId()));
            if (scanRanges == null) {
                sb.append("}");
                continue;
            }
            sb.append(",range=[");
            int eIdx = 0;
            for (Map.Entry<Integer, List<TScanRangeParams>> entry : scanRanges.entrySet()) {
                if (eIdx++ != 0) {
                    sb.append(",");
                }
                sb.append("id").append(entry.getKey()).append(",");
                appendScanRange(sb, entry.getValue());
            }
            sb.append("]");
            sb.append("}");
        }
        sb.append("]"); // end of instances
        sb.append("}");
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        appendTo(sb);
        return sb.toString();
    }
}
