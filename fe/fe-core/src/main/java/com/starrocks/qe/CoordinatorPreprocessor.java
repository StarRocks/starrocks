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

package com.starrocks.qe;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.starrocks.catalog.ResourceGroup;
import com.starrocks.catalog.ResourceGroupClassifier;
import com.starrocks.common.conf.Config;
import com.starrocks.common.exception.UserException;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.planner.DataPartition;
import com.starrocks.planner.DataSink;
import com.starrocks.planner.PlanFragment;
import com.starrocks.planner.PlanFragmentId;
import com.starrocks.planner.ResultSink;
import com.starrocks.planner.ScanNode;
import com.starrocks.planner.TableFunctionTableSink;
import com.starrocks.qe.scheduler.DefaultWorkerProvider;
import com.starrocks.qe.scheduler.TFragmentInstanceFactory;
import com.starrocks.qe.scheduler.WorkerProvider;
import com.starrocks.qe.scheduler.assignment.FragmentAssignmentStrategyFactory;
import com.starrocks.qe.scheduler.dag.ExecutionDAG;
import com.starrocks.qe.scheduler.dag.ExecutionFragment;
import com.starrocks.qe.scheduler.dag.JobSpec;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.service.FrontendOptions;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.statistic.StatisticUtils;
import com.starrocks.system.ComputeNode;
import com.starrocks.thrift.TDescriptorTable;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TQueryGlobals;
import com.starrocks.thrift.TUniqueId;
import com.starrocks.thrift.TWorkGroup;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public class CoordinatorPreprocessor {
    private static final Logger LOG = LogManager.getLogger(CoordinatorPreprocessor.class);
    private static final String LOCAL_IP = FrontendOptions.getLocalHostAddress();
    public static final int BUCKET_ABSENT = 2147483647;

    static final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private final TNetworkAddress coordAddress;
    private final ConnectContext connectContext;

    private final JobSpec jobSpec;
    private final ExecutionDAG executionDAG;

    private final WorkerProvider.Factory workerProviderFactory = new DefaultWorkerProvider.Factory();
    private WorkerProvider workerProvider;

    private final FragmentAssignmentStrategyFactory fragmentAssignmentStrategyFactory;

    public CoordinatorPreprocessor(ConnectContext context, JobSpec jobSpec) {
        this.coordAddress = new TNetworkAddress(LOCAL_IP, Config.rpc_port);

        this.connectContext = Preconditions.checkNotNull(context);
        this.jobSpec = jobSpec;
        this.executionDAG = ExecutionDAG.build(jobSpec);

        SessionVariable sessionVariable = connectContext.getSessionVariable();
        this.workerProvider = workerProviderFactory.captureAvailableWorkers(GlobalStateMgr.getCurrentSystemInfo(),
                sessionVariable.isPreferComputeNode(), sessionVariable.getUseComputeNodes());

        this.fragmentAssignmentStrategyFactory = new FragmentAssignmentStrategyFactory(connectContext, jobSpec, executionDAG);

    }

    @VisibleForTesting
    CoordinatorPreprocessor(List<PlanFragment> fragments, List<ScanNode> scanNodes) {
        this.coordAddress = new TNetworkAddress(LOCAL_IP, Config.rpc_port);

        this.connectContext = StatisticUtils.buildConnectContext();
        this.jobSpec = JobSpec.Factory.mockJobSpec(connectContext, fragments, scanNodes);
        this.executionDAG = ExecutionDAG.build(jobSpec);

        SessionVariable sessionVariable = connectContext.getSessionVariable();
        this.workerProvider = workerProviderFactory.captureAvailableWorkers(GlobalStateMgr.getCurrentSystemInfo(),
                sessionVariable.isPreferComputeNode(), sessionVariable.getUseComputeNodes());

        Map<PlanFragmentId, PlanFragment> fragmentMap =
                fragments.stream().collect(Collectors.toMap(PlanFragment::getFragmentId, Function.identity()));
        for (ScanNode scan : scanNodes) {
            PlanFragmentId id = scan.getFragmentId();
            PlanFragment fragment = fragmentMap.get(id);
            if (fragment == null) {
                // Fake a fragment for this node
                fragment = new PlanFragment(id, scan, DataPartition.RANDOM);
                executionDAG.attachFragments(Collections.singletonList(fragment));
            }
        }

        this.fragmentAssignmentStrategyFactory = new FragmentAssignmentStrategyFactory(connectContext, jobSpec, executionDAG);
    }

    public static TQueryGlobals genQueryGlobals(Instant startTime, String timezone) {
        TQueryGlobals queryGlobals = new TQueryGlobals();
        String nowString = DATE_FORMAT.format(startTime.atZone(ZoneId.of(timezone)));
        queryGlobals.setNow_string(nowString);
        queryGlobals.setTimestamp_ms(startTime.toEpochMilli());
        queryGlobals.setTimestamp_us(startTime.getEpochSecond() * 1000000 + startTime.getNano() / 1000);
        if (timezone.equals("CST")) {
            queryGlobals.setTime_zone(TimeUtils.DEFAULT_TIME_ZONE);
        } else {
            queryGlobals.setTime_zone(timezone);
        }
        return queryGlobals;
    }

    public TUniqueId getQueryId() {
        return jobSpec.getQueryId();
    }

    public ConnectContext getConnectContext() {
        return connectContext;
    }

    public ExecutionDAG getExecutionDAG() {
        return executionDAG;
    }

    public TDescriptorTable getDescriptorTable() {
        return jobSpec.getDescTable();
    }

    public List<ExecutionFragment> getFragmentsInPreorder() {
        return executionDAG.getFragmentsInPreorder();
    }

    public TNetworkAddress getCoordAddress() {
        return coordAddress;
    }

    public TNetworkAddress getBrpcAddress(long workerId) {
        return workerProvider.getWorkerById(workerId).getBrpcAddress();
    }

    public TNetworkAddress getBrpcIpAddress(long workerId) {
        return workerProvider.getWorkerById(workerId).getBrpcIpAddress();
    }

    public TNetworkAddress getAddress(long workerId) {
        ComputeNode worker = workerProvider.getWorkerById(workerId);
        return worker.getAddress();
    }

    public WorkerProvider getWorkerProvider() {
        return workerProvider;
    }

    public TWorkGroup getResourceGroup() {
        return jobSpec.getResourceGroup();
    }

    public void prepareExec() throws Exception {
        resetExec();
        computeFragmentInstances();
        traceInstance();
    }

    /**
     * Reset state of all the fragments set in Coordinator, when retrying the same query with the fragments.
     */
    private void resetExec() {
        SessionVariable sessionVariable = connectContext.getSessionVariable();
        workerProvider = workerProviderFactory.captureAvailableWorkers(GlobalStateMgr.getCurrentSystemInfo(),
                sessionVariable.isPreferComputeNode(), sessionVariable.getUseComputeNodes());

        jobSpec.getFragments().forEach(PlanFragment::reset);
    }

    private void traceInstance() {
        if (LOG.isDebugEnabled()) {
            // TODO(zc): add a switch to close this function
            StringBuilder sb = new StringBuilder();
            int idx = 0;
            sb.append("query id=").append(DebugUtil.printId(jobSpec.getQueryId())).append(",");
            sb.append("fragment=[");
            for (ExecutionFragment execFragment : executionDAG.getFragmentsInPreorder()) {
                if (idx++ != 0) {
                    sb.append(",");
                }
                sb.append(execFragment.getFragmentId());
                execFragment.appendTo(sb);
            }
            sb.append("]");
            LOG.debug(sb.toString());
        }
    }

    @VisibleForTesting
    FragmentScanRangeAssignment getFragmentScanRangeAssignment(PlanFragmentId fragmentId) {
        return executionDAG.getFragment(fragmentId).getScanRangeAssignment();
    }

    @VisibleForTesting
    void computeFragmentInstances() throws UserException {
        for (ExecutionFragment execFragment : executionDAG.getFragmentsInPostorder()) {
            fragmentAssignmentStrategyFactory.create(execFragment, workerProvider).assignFragmentToWorker(execFragment);
        }
        if (LOG.isDebugEnabled()) {
            executionDAG.getFragmentsInPreorder().forEach(
                    execFragment -> LOG.debug("fragment {} has instances {}", execFragment.getPlanFragment().getFragmentId(),
                            execFragment.getInstances().size()));
        }

        validateExecutionDAG();

        executionDAG.finalizeDAG();
    }

    private void validateExecutionDAG() throws StarRocksPlannerException {
        for (ExecutionFragment execFragment : executionDAG.getFragmentsInPreorder()) {
            DataSink sink = execFragment.getPlanFragment().getSink();
            if (sink instanceof ResultSink && execFragment.getInstances().size() > 1) {
                throw new StarRocksPlannerException("This sql plan has multi result sinks", ErrorType.INTERNAL_ERROR);
            }

            if (sink instanceof TableFunctionTableSink && (((TableFunctionTableSink) sink).isWriteSingleFile())
                    && execFragment.getInstances().size() > 1) {
                throw new StarRocksPlannerException(
                        "This sql plan has multi table function table sinks, but set to write single file",
                        ErrorType.INTERNAL_ERROR);
            }
        }
    }

    public TFragmentInstanceFactory createTFragmentInstanceFactory() {
        return new TFragmentInstanceFactory(connectContext, jobSpec, executionDAG, coordAddress);
    }

    public Map<Integer, TNetworkAddress> getChannelIdToBEHTTPMap() {
        if (jobSpec.isStreamLoad()) {
            return executionDAG.getChannelIdToBEHTTP();
        }
        return null;
    }

    public Map<Integer, TNetworkAddress> getChannelIdToBEPortMap() {
        if (jobSpec.isStreamLoad()) {
            return executionDAG.getChannelIdToBEPort();
        }
        return null;
    }

    public static TWorkGroup prepareResourceGroup(ConnectContext connect, ResourceGroupClassifier.QueryType queryType) {
        if (connect == null || !connect.getSessionVariable().isEnableResourceGroup()) {
            return null;
        }
        SessionVariable sessionVariable = connect.getSessionVariable();
        TWorkGroup resourceGroup = null;

        // 1. try to use the resource group specified by the variable
        if (StringUtils.isNotEmpty(sessionVariable.getResourceGroup())) {
            String rgName = sessionVariable.getResourceGroup();
            resourceGroup = GlobalStateMgr.getCurrentState().getResourceGroupMgr().chooseResourceGroupByName(rgName);
            if (rgName.equalsIgnoreCase(ResourceGroup.DEFAULT_MV_RESOURCE_GROUP_NAME)) {
                ResourceGroup defaultMVResourceGroup = new ResourceGroup();
                defaultMVResourceGroup.setId(ResourceGroup.DEFAULT_MV_WG_ID);
                defaultMVResourceGroup.setName(ResourceGroup.DEFAULT_MV_RESOURCE_GROUP_NAME);
                defaultMVResourceGroup.setVersion(ResourceGroup.DEFAULT_MV_VERSION);
                resourceGroup = defaultMVResourceGroup.toThrift();
            }
        }

        // 2. try to use the resource group specified by workgroup_id
        long workgroupId = connect.getSessionVariable().getResourceGroupId();
        if (resourceGroup == null && workgroupId > 0) {
            resourceGroup = GlobalStateMgr.getCurrentState().getResourceGroupMgr().chooseResourceGroupByID(workgroupId);
        }

        // 3. if the specified resource group not exist try to use the default one
        if (resourceGroup == null) {
            Set<Long> dbIds = connect.getCurrentSqlDbIds();
            resourceGroup = GlobalStateMgr.getCurrentState().getResourceGroupMgr().chooseResourceGroup(
                    connect, queryType, dbIds);
        }

        if (resourceGroup != null) {
            connect.getAuditEventBuilder().setResourceGroup(resourceGroup.getName());
            connect.setResourceGroup(resourceGroup);
        } else {
            connect.getAuditEventBuilder().setResourceGroup(ResourceGroup.DEFAULT_RESOURCE_GROUP_NAME);
        }

        return resourceGroup;
    }

}
