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

package com.starrocks.planner;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.SortInfo;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TRuntimeFilterBuildJoinMode;
import com.starrocks.thrift.TRuntimeFilterBuildType;
import com.starrocks.thrift.TRuntimeFilterDescription;
import com.starrocks.thrift.TRuntimeFilterDestination;
import com.starrocks.thrift.TRuntimeFilterLayout;
import com.starrocks.thrift.TRuntimeFilterLayoutMode;
import com.starrocks.thrift.TUniqueId;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.starrocks.planner.JoinNode.DistributionMode.BROADCAST;
import static com.starrocks.planner.JoinNode.DistributionMode.COLOCATE;
import static com.starrocks.planner.JoinNode.DistributionMode.LOCAL_HASH_BUCKET;
import static com.starrocks.planner.JoinNode.DistributionMode.PARTITIONED;
import static com.starrocks.planner.JoinNode.DistributionMode.REPLICATED;
import static com.starrocks.planner.JoinNode.DistributionMode.SHUFFLE_HASH_BUCKET;

// this class is to describe a runtime filter.
// this class is almost identical to TRuntimeFilterDescription in PlanNodes.thrift
// but comparing to thrift definition, this class has some handy methods and
// `toExplainString()` for explaining sql
public class RuntimeFilterDescription {
    public enum RuntimeFilterType {
        TOPN_FILTER,
        JOIN_FILTER
    }

    private int filterId;
    private int buildPlanNodeId;
    private Expr buildExpr;
    private int exprOrder; // order of expr in eq conjuncts.
    private final Map<Integer, Expr> nodeIdToProbeExpr;
    private boolean hasRemoteTargets;
    private final List<TNetworkAddress> mergeNodes;
    private JoinNode.DistributionMode joinMode;
    private TUniqueId senderFragmentInstanceId;

    private Set<TUniqueId> broadcastGRFSenders;

    private List<TRuntimeFilterDestination> broadcastGRFDestinations;
    private int equalCount;
    private int crossExchangeNodeTimes;
    private boolean equalForNull;

    private long buildCardinality;
    private SessionVariable sessionVariable;

    private boolean onlyLocal;

    private long topn;

    // ExecGroupInfo. used for check build colocate runtime filter
    private boolean isBuildFromColocateGroup = false;
    private int execGroupId = -1;

    private RuntimeFilterType type;

    int numInstances;
    int numDriversPerInstance;
    private List<Integer> bucketSeqToInstance = Lists.newArrayList();
    private List<Integer> bucketSeqToDriverSeq = Lists.newArrayList();
    private List<Integer> bucketSeqToPartition = Lists.newArrayList();
    // partitionByExprs are used for computing partition ids in probe side when
    // join's equal conjuncts size > 1.
    private final Map<Integer, List<Expr>> nodeIdToParitionByExprs = Maps.newHashMap();

    private SortInfo sortInfo;

    public RuntimeFilterDescription(SessionVariable sv) {
        nodeIdToProbeExpr = new HashMap<>();
        mergeNodes = new ArrayList<>();
        filterId = 0;
        exprOrder = 0;
        hasRemoteTargets = false;
        joinMode = JoinNode.DistributionMode.NONE;
        senderFragmentInstanceId = null;
        equalCount = 0;
        crossExchangeNodeTimes = 0;
        buildCardinality = 0;
        equalForNull = false;
        sessionVariable = sv;
        onlyLocal = false;
        type = RuntimeFilterType.JOIN_FILTER;
    }

    public boolean getEqualForNull() {
        return equalForNull;
    }

    public void setEqualForNull(boolean v) {
        equalForNull = v;
    }

    public void setFilterId(int id) {
        filterId = id;
    }

    public int getFilterId() {
        return filterId;
    }

    public void setBuildExpr(Expr expr) {
        buildExpr = expr;
    }

    public void setBuildCardinality(long value) {
        buildCardinality = value;
    }

    public RuntimeFilterType runtimeFilterType() {
        return type;
    }

    public void setRuntimeFilterType(RuntimeFilterType type) {
        this.type = type;
    }

    public SortInfo getSortInfo() {
        return sortInfo;
    }

    public void setSortInfo(SortInfo sortInfo) {
        this.sortInfo = sortInfo;
    }

    public void setTopN(long value) {
        this.topn = value;
    }

    public long getTopN() {
        return this.topn;
    }

    public boolean canProbeUse(PlanNode node, RuntimeFilterPushDownContext rfPushCtx) {
        if (!canAcceptFilter(node, rfPushCtx)) {
            return false;
        }

        if (RuntimeFilterType.TOPN_FILTER.equals(runtimeFilterType()) && node instanceof OlapScanNode) {
            ((OlapScanNode) node).setOrderHint(isAscFilter());
        }
        // if we don't across exchange node, that's to say this is in local fragment instance.
        // we don't need to use adaptive strategy now. we are using a conservative way.
        if (inLocalFragmentInstance()) {
            return true;
        }

        long probeMin = sessionVariable.getGlobalRuntimeFilterProbeMinSize();
        long card = node.getCardinality();
        // The special value 0 means force use this filter
        // Adopts small runtime filter when:
        // 1. for rf generated by BROADCAST/SHUFFLE JOIN, adopts rf if row count < buildMin
        // 2. for rf generated by BUCKET_SHUFFLE/COLOCATE JOIN, adopts rf if row count < buildMin * numAliveBackends.
        long buildMin = sessionVariable.getGlobalRuntimeFilterBuildMinSize();
        if (buildMin > 0 && isColocateOrBucketShuffle()) {
            int numBackends = ConnectContext.get() != null ? ConnectContext.get().getAliveBackendNumber() : 1;
            numBackends = Math.max(1, numBackends);
            buildMin = (Long.MAX_VALUE / numBackends > buildMin) ? buildMin * numBackends : Long.MAX_VALUE;
        }
        if (probeMin == 0 || (buildMin > 0 && buildCardinality <= buildMin)) {
            return true;
        }
        if (card < probeMin) {
            return false;
        }
        long buildCard = Math.max(0, buildCardinality);
        float evaluatedFilterRatio = (buildCard * 1.0f / card);
        float acceptedFilterRatioLB = 1.0f - sessionVariable.getGlobalRuntimeFilterProbeMinSelectivity();
        return evaluatedFilterRatio <= acceptedFilterRatioLB;
    }

    // return true if Node could accept the Filter
    public boolean canAcceptFilter(PlanNode node, RuntimeFilterPushDownContext rfPushCtx) {
        if (RuntimeFilterType.TOPN_FILTER.equals(runtimeFilterType())) {
            if (node instanceof ScanNode) {
                ScanNode scanNode = (ScanNode) node;
                return scanNode.supportTopNRuntimeFilter();
            } else {
                return false;
            }
        }
        // colocate runtime filter couldn't apply to other exec groups
        if (isBuildFromColocateGroup && joinMode.equals(COLOCATE)) {
            int probeExecGroupId = rfPushCtx.getExecGroup(node.getId().asInt()).getGroupId().asInt();
            if (execGroupId != probeExecGroupId) {
                return false;
            }
        }

        return true;
    }

    public boolean isNullLast() {
        if (sortInfo != null) {
            return !sortInfo.getNullsFirst().get(0);
        } else {
            return false;
        }
    }

    public boolean isAscFilter() {
        if (sortInfo != null) {
            return sortInfo.getIsAscOrder().get(0);
        } else {
            return true;
        }
    }

    public void enterExchangeNode() {
        crossExchangeNodeTimes += 1;
    }

    public void exitExchangeNode() {
        crossExchangeNodeTimes -= 1;
    }

    public boolean inLocalFragmentInstance() {
        return crossExchangeNodeTimes == 0;
    }

    public void addProbeExpr(int nodeId, Expr expr) {
        nodeIdToProbeExpr.put(nodeId, expr);
    }

    public Map<Integer, Expr> getNodeIdToProbeExpr() {
        return nodeIdToProbeExpr;
    }

    public void addPartitionByExprsIfNeeded(int nodeId, Expr probeExpr, List<Expr> partitionByExprs) {
        if (partitionByExprs.size() == 0) {
            return;
        }
        // If partition_by_exprs only have one and equals to probeExpr, not set partition_by_exprs:
        //  - to keep compatible with old policies;
        //  - to decrease expr evals for the same expr;
        if (partitionByExprs.size() == 1 && partitionByExprs.get(0).equals(probeExpr)) {
            return;
        }
        nodeIdToParitionByExprs.put(nodeId, partitionByExprs);
    }

    public void setHasRemoteTargets(boolean value) {
        hasRemoteTargets = value;
    }

    public void setEqualCount(int value) {
        equalCount = value;
    }

    public int getEqualCount() {
        return equalCount;
    }

    public boolean isHasRemoteTargets() {
        return hasRemoteTargets;
    }

    public void setExprOrder(int order) {
        exprOrder = order;
    }

    public void setJoinMode(JoinNode.DistributionMode mode) {
        joinMode = mode;
    }

    public boolean isColocateOrBucketShuffle() {
        return joinMode.equals(COLOCATE) ||
                joinMode.equals(LOCAL_HASH_BUCKET);
    }

    public int getBuildPlanNodeId() {
        return buildPlanNodeId;
    }

    public void setBuildPlanNodeId(int buildPlanNodeId) {
        this.buildPlanNodeId = buildPlanNodeId;
    }

    public void setOnlyLocal(boolean onlyLocal) {
        this.onlyLocal = onlyLocal;
    }

    public boolean isLocalApplicable() {
        return joinMode.equals(BROADCAST) ||
                joinMode.equals(COLOCATE) ||
                joinMode.equals(LOCAL_HASH_BUCKET) ||
                joinMode.equals(SHUFFLE_HASH_BUCKET) ||
                joinMode.equals(REPLICATED);
    }

    public boolean isBroadcastJoin() {
        return joinMode.equals(BROADCAST);
    }

    public void setBucketSeqToInstance(List<Integer> bucketSeqToInstance) {
        this.bucketSeqToInstance = bucketSeqToInstance;
    }

    public void setBucketSeqToDriverSeq(List<Integer> bucketSeqToDriverSeq) {
        this.bucketSeqToDriverSeq = bucketSeqToDriverSeq;
    }

    public void setBucketSeqToPartition(List<Integer> bucketSeqToPartition) {
        this.bucketSeqToPartition = bucketSeqToPartition;
    }

    public List<Integer> getBucketSeqToInstance() {
        return this.bucketSeqToInstance;
    }

    public List<Integer> getBucketSeqToPartition() {
        return this.bucketSeqToPartition;
    }

    public void setNumInstances(int numInstances) {
        this.numInstances = numInstances;
    }

    public int getNumInstances() {
        return numInstances;
    }

    public void setNumDriversPerInstance(int numDriversPerInstance) {
        this.numDriversPerInstance = numDriversPerInstance;
    }

    public int getNumDriversPerInstance() {
        return numDriversPerInstance;
    }

    public void setExecGroupInfo(boolean buildFromColocateGroup, int buildExecGroupId) {
        this.isBuildFromColocateGroup = buildFromColocateGroup;
        this.execGroupId = buildExecGroupId;
    }

    public void clearExecGroupInfo() {
        this.isBuildFromColocateGroup = false;
        this.execGroupId = -1;
    }

    public boolean canPushAcrossExchangeNode() {
        if (onlyLocal) {
            return false;
        }
        switch (joinMode) {
            case BROADCAST:
            case PARTITIONED:
            case LOCAL_HASH_BUCKET:
            case SHUFFLE_HASH_BUCKET:
            case COLOCATE:
                return true;
            default:
                return false;
        }
    }

    public void addMergeNode(TNetworkAddress addr) {
        if (mergeNodes.contains(addr)) {
            return;
        }
        mergeNodes.add(addr);
    }

    public void setSenderFragmentInstanceId(TUniqueId value) {
        senderFragmentInstanceId = value;
    }

    public void setBroadcastGRFSenders(Set<TUniqueId> broadcastGRFSenders) {
        this.broadcastGRFSenders = broadcastGRFSenders;
    }

    public void setBroadcastGRFDestinations(List<TRuntimeFilterDestination> broadcastGRFDestinations) {
        this.broadcastGRFDestinations = broadcastGRFDestinations;
    }

    public List<TRuntimeFilterDestination> getBroadcastGRFDestinations() {
        return broadcastGRFDestinations;
    }

    // Only use partition_by_exprs when the grf is remote and joinMode is partitioned.
    private boolean isCanUsePartitionByExprs() {
        return hasRemoteTargets && joinMode != BROADCAST;
    }

    public String toExplainString(int probeNodeId) {
        StringBuilder sb = new StringBuilder();
        sb.append("filter_id = ").append(filterId);
        if (probeNodeId >= 0) {
            sb.append(", probe_expr = (").append(nodeIdToProbeExpr.get(probeNodeId).toSql()).append(")");
            if (isCanUsePartitionByExprs() && nodeIdToParitionByExprs.containsKey(probeNodeId) &&
                    !nodeIdToParitionByExprs.get(probeNodeId).isEmpty()) {
                sb.append(", partition_exprs = (");
                List<Expr> partitionByExprs = nodeIdToParitionByExprs.get(probeNodeId);
                for (int i = 0; i < partitionByExprs.size(); i++) {
                    Expr partitionByExpr = partitionByExprs.get(i);
                    if (i != partitionByExprs.size() - 1) {
                        sb.append(partitionByExpr.toSql() + ",");
                    } else {
                        sb.append(partitionByExpr.toSql());
                    }
                }
                sb.append(")");
            }
        } else {
            sb.append(", build_expr = (").append(buildExpr.toSql()).append(")");
            sb.append(", remote = ").append(hasRemoteTargets);
        }
        return sb.toString();
    }

    private TRuntimeFilterLayoutMode computeLocalLayout() {
        if (sessionVariable.isEnablePipelineLevelMultiPartitionedRf()) {
            if (joinMode == BROADCAST || joinMode == REPLICATED) {
                return TRuntimeFilterLayoutMode.SINGLETON;
            } else if (joinMode == PARTITIONED ||
                    joinMode == SHUFFLE_HASH_BUCKET) {
                return TRuntimeFilterLayoutMode.PIPELINE_SHUFFLE;
            } else if (joinMode == COLOCATE || joinMode == LOCAL_HASH_BUCKET) {
                return (bucketSeqToDriverSeq == null || bucketSeqToDriverSeq.isEmpty()) ?
                        TRuntimeFilterLayoutMode.PIPELINE_BUCKET_LX
                        : TRuntimeFilterLayoutMode.PIPELINE_BUCKET;
            } else {
                return TRuntimeFilterLayoutMode.NONE;
            }
        } else {
            return TRuntimeFilterLayoutMode.SINGLETON;
        }
    }

    private TRuntimeFilterLayoutMode computeGlobalLayout() {
        if (sessionVariable.isEnablePipelineLevelMultiPartitionedRf()) {
            if (joinMode == BROADCAST || joinMode == REPLICATED) {
                return TRuntimeFilterLayoutMode.SINGLETON;
            } else if (joinMode == PARTITIONED || joinMode == SHUFFLE_HASH_BUCKET) {
                return TRuntimeFilterLayoutMode.GLOBAL_SHUFFLE_2L;
            } else if (joinMode == COLOCATE ||
                    joinMode == LOCAL_HASH_BUCKET) {
                return (bucketSeqToDriverSeq == null || bucketSeqToDriverSeq.isEmpty()) ?
                        TRuntimeFilterLayoutMode.GLOBAL_BUCKET_2L_LX
                        : TRuntimeFilterLayoutMode.GLOBAL_BUCKET_2L;
            } else {
                return TRuntimeFilterLayoutMode.NONE;
            }
        } else {
            if (joinMode == BROADCAST || joinMode == REPLICATED) {
                return TRuntimeFilterLayoutMode.SINGLETON;
            } else if (joinMode == PARTITIONED ||
                    joinMode == SHUFFLE_HASH_BUCKET) {
                return TRuntimeFilterLayoutMode.GLOBAL_SHUFFLE_1L;
            } else if (joinMode == COLOCATE ||
                    joinMode == LOCAL_HASH_BUCKET) {
                return TRuntimeFilterLayoutMode.GLOBAL_BUCKET_1L;
            } else {
                return TRuntimeFilterLayoutMode.NONE;
            }
        }
    }

    TRuntimeFilterLayout toLayout() {
        TRuntimeFilterLayout layout = new TRuntimeFilterLayout();
        layout.setFilter_id(filterId);
        layout.setLocal_layout(computeLocalLayout());
        layout.setGlobal_layout(computeGlobalLayout());
        layout.setPipeline_level_multi_partitioned(sessionVariable.isEnablePipelineLevelMultiPartitionedRf());
        layout.setNum_instances(numInstances);
        layout.setNum_drivers_per_instance(numDriversPerInstance);
        if (bucketSeqToInstance != null && !bucketSeqToInstance.isEmpty()) {
            layout.setBucketseq_to_instance(bucketSeqToInstance);
        }
        if (bucketSeqToDriverSeq != null && !bucketSeqToDriverSeq.isEmpty()) {
            layout.setBucketseq_to_driverseq(bucketSeqToDriverSeq);
        }
        if (bucketSeqToPartition != null && !bucketSeqToPartition.isEmpty()) {
            layout.setBucketseq_to_partition(bucketSeqToPartition);
        }
        return layout;
    }

    public TRuntimeFilterDescription toThrift() {
        TRuntimeFilterDescription t = new TRuntimeFilterDescription();
        t.setFilter_id(filterId);
        if (buildExpr != null) {
            t.setBuild_expr(buildExpr.treeToThrift());
        }
        t.setExpr_order(exprOrder);
        for (Map.Entry<Integer, Expr> entry : nodeIdToProbeExpr.entrySet()) {
            t.putToPlan_node_id_to_target_expr(entry.getKey(), entry.getValue().treeToThrift());
        }
        t.setHas_remote_targets(hasRemoteTargets);
        t.setBuild_plan_node_id(buildPlanNodeId);
        if (!mergeNodes.isEmpty()) {
            t.setRuntime_filter_merge_nodes(mergeNodes);
        }
        if (senderFragmentInstanceId != null) {
            t.setSender_finst_id(senderFragmentInstanceId);
        }

        if (broadcastGRFSenders != null && !broadcastGRFSenders.isEmpty()) {
            t.setBroadcast_grf_senders(broadcastGRFSenders.stream().collect(Collectors.toList()));
        }

        if (broadcastGRFDestinations != null && !broadcastGRFDestinations.isEmpty()) {
            t.setBroadcast_grf_destinations(broadcastGRFDestinations);
        }

        t.setLayout(toLayout());

        assert (joinMode != JoinNode.DistributionMode.NONE);
        if (joinMode.equals(BROADCAST)) {
            t.setBuild_join_mode(TRuntimeFilterBuildJoinMode.BORADCAST);
        } else if (joinMode.equals(LOCAL_HASH_BUCKET)) {
            t.setBuild_join_mode(TRuntimeFilterBuildJoinMode.LOCAL_HASH_BUCKET);
        } else if (joinMode.equals(PARTITIONED)) {
            t.setBuild_join_mode(TRuntimeFilterBuildJoinMode.PARTITIONED);
        } else if (joinMode.equals(COLOCATE)) {
            t.setBuild_join_mode(TRuntimeFilterBuildJoinMode.COLOCATE);
        } else if (joinMode.equals(SHUFFLE_HASH_BUCKET)) {
            t.setBuild_join_mode(TRuntimeFilterBuildJoinMode.SHUFFLE_HASH_BUCKET);
        } else if (joinMode.equals(REPLICATED)) {
            t.setBuild_join_mode(TRuntimeFilterBuildJoinMode.REPLICATED);
        }
        if (isCanUsePartitionByExprs()) {
            for (Map.Entry<Integer, List<Expr>> entry : nodeIdToParitionByExprs.entrySet()) {
                if (entry.getValue() != null && !entry.getValue().isEmpty()) {
                    t.putToPlan_node_id_to_partition_by_exprs(entry.getKey(),
                            Expr.treesToThrift(entry.getValue()));
                }
            }
        }

        t.setBuild_from_group_execution(isBuildFromColocateGroup);

        if (RuntimeFilterType.TOPN_FILTER.equals(runtimeFilterType())) {
            t.setFilter_type(TRuntimeFilterBuildType.TOPN_FILTER);
        } else {
            t.setFilter_type(TRuntimeFilterBuildType.JOIN_FILTER);
        }

        return t;
    }

    static List<TRuntimeFilterDescription> toThriftRuntimeFilterDescriptions(List<RuntimeFilterDescription> rfList) {
        List<TRuntimeFilterDescription> result = new ArrayList<>();
        for (RuntimeFilterDescription rf : rfList) {
            result.add(rf.toThrift());
        }
        return result;
    }
}
