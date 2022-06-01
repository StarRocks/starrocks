// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.planner;

import com.google.common.collect.Lists;
import com.starrocks.analysis.Expr;
import com.starrocks.qe.SessionVariable;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TRuntimeFilterBuildJoinMode;
import com.starrocks.thrift.TRuntimeFilterDescription;
import com.starrocks.thrift.TRuntimeFilterDestination;
import com.starrocks.thrift.TUniqueId;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

// this class is to describe a runtime filter.
// this class is almost identical to TRuntimeFilterDescription in PlanNodes.thrift
// but comparing to thrift definition, this class has some handy methods and
// `toExplainString()` for explaining sql
public class RuntimeFilterDescription {
    private int filterId;
    private int buildPlanNodeId;
    private Expr buildExpr;
    private int exprOrder; // order of expr in eq conjuncts.
    private final Map<Integer, Expr> nodeIdToProbeExpr;
    private boolean hasRemoteTargets;
    private final List<TNetworkAddress> mergeNodes;
    private HashJoinNode.DistributionMode joinMode;
    private TUniqueId senderFragmentInstanceId;



    private Set<TUniqueId> broadcastGRFSenders;


    private List<TRuntimeFilterDestination> broadcastGRFDestinations;
    private int equalCount;
    private int crossExchangeNodeTimes;
    private boolean equalForNull;

    private long buildCardinality;
    private SessionVariable sessionVariable;

    private boolean onlyLocal;

    private List<Integer> bucketSeqToInstance = Lists.newArrayList();

    public RuntimeFilterDescription(SessionVariable sv) {
        nodeIdToProbeExpr = new HashMap<>();
        mergeNodes = new ArrayList<>();
        filterId = 0;
        exprOrder = 0;
        hasRemoteTargets = false;
        joinMode = HashJoinNode.DistributionMode.NONE;
        senderFragmentInstanceId = null;
        equalCount = 0;
        crossExchangeNodeTimes = 0;
        buildCardinality = 0;
        equalForNull = false;
        sessionVariable = sv;
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

    public boolean canProbeUse(PlanNode node) {
        // if we don't across exchange node, that's to say this is in local fragment instance.
        // we don't need to use adaptive strategy now. we are using a conservative way.
        if (inLocalFragmentInstance()) {
            return true;
        }

        long card = node.getCardinality();
        if (card < sessionVariable.getGlobalRuntimeFilterProbeMinSize()) {
            return false;
        }
        float sel = (1.0f - buildCardinality * 1.0f / card);
        return !(sel < sessionVariable.getGlobalRuntimeFilterProbeMinSelectivity());
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

    public void setOnlyLocal(boolean value) {
        onlyLocal = value;
    }

    public void setHasRemoteTargets(boolean value) {
        hasRemoteTargets = value;
    }

    public void setEqualCount(int value) {
        equalCount = value;
    }

    public boolean isHasRemoteTargets() {
        return hasRemoteTargets;
    }

    public void setExprOrder(int order) {
        exprOrder = order;
    }

    public void setJoinMode(HashJoinNode.DistributionMode mode) {
        joinMode = mode;
    }

    public boolean isColocate() {
        return joinMode.equals(HashJoinNode.DistributionMode.COLOCATE);
    }

    public int getBuildPlanNodeId() {
        return buildPlanNodeId;
    }

    public void setBuildPlanNodeId(int buildPlanNodeId) {
        this.buildPlanNodeId = buildPlanNodeId;
    }

    public boolean isLocalApplicable() {
        return joinMode.equals(HashJoinNode.DistributionMode.BROADCAST) ||
                joinMode.equals(HashJoinNode.DistributionMode.COLOCATE) ||
                joinMode.equals(HashJoinNode.DistributionMode.LOCAL_HASH_BUCKET) ||
                joinMode.equals(HashJoinNode.DistributionMode.REPLICATED);
    }

    public boolean isBroadcastJoin() {
        return joinMode.equals(HashJoinNode.DistributionMode.BROADCAST);
    }

    public void setBucketSeqToInstance(List<Integer> bucketSeqToInstance) {
        this.bucketSeqToInstance = bucketSeqToInstance;
    }

    public List<Integer> getBucketSeqToInstance() {
        return this.bucketSeqToInstance;
    }

    public boolean canPushAcrossExchangeNode() {
        if (onlyLocal) {
            return false;
        }
        switch (joinMode) {
            case BROADCAST: {
                // Broadcast HJ generates non-shuffle-aware(single-part) rf, so it can be always
                // pushed down across ExchangeNode.
                return true;
            }
            case PARTITIONED:
            case LOCAL_HASH_BUCKET:
            case SHUFFLE_HASH_BUCKET:
            case COLOCATE: {
                // these HJ always generates shuffle-aware(multi-part) rf, at present only rf generated by
                // HJ with only one equalConjunct is allowed to pushed down across ExchangeNode.
                // TODO(by satanson): rf generated by HJ with multiple equalConjuncts needs partition_by_exprs
                //  to choose the right partial component of multi-part global rf.
                return equalCount == 1;
            }
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

    public String toExplainString(int probeNodeId) {
        StringBuilder sb = new StringBuilder();
        sb.append("filter_id = ").append(filterId);
        if (probeNodeId >= 0) {
            sb.append(", probe_expr = (").append(nodeIdToProbeExpr.get(probeNodeId).toSql()).append(")");
        } else {
            sb.append(", build_expr = (").append(buildExpr.toSql()).append(")");
            sb.append(", remote = ").append(hasRemoteTargets);
        }
        return sb.toString();
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

        if (isColocate() && bucketSeqToInstance!=null && !bucketSeqToInstance.isEmpty()) {
            t.setBucketseq_to_instance(bucketSeqToInstance);
        }
        assert (joinMode != HashJoinNode.DistributionMode.NONE);
        if (joinMode.equals(HashJoinNode.DistributionMode.BROADCAST)) {
            t.setBuild_join_mode(TRuntimeFilterBuildJoinMode.BORADCAST);
        } else if (joinMode.equals(HashJoinNode.DistributionMode.LOCAL_HASH_BUCKET)) {
            t.setBuild_join_mode(TRuntimeFilterBuildJoinMode.LOCAL_HASH_BUCKET);
        } else if (joinMode.equals(HashJoinNode.DistributionMode.PARTITIONED)) {
            t.setBuild_join_mode(TRuntimeFilterBuildJoinMode.PARTITIONED);
        } else if (joinMode.equals(HashJoinNode.DistributionMode.COLOCATE)) {
            t.setBuild_join_mode(TRuntimeFilterBuildJoinMode.COLOCATE);
        } else if (joinMode.equals(HashJoinNode.DistributionMode.SHUFFLE_HASH_BUCKET)) {
            t.setBuild_join_mode(TRuntimeFilterBuildJoinMode.SHUFFLE_HASH_BUCKET);
        } else if (joinMode.equals(HashJoinNode.DistributionMode.REPLICATED)) {
            t.setBuild_join_mode(TRuntimeFilterBuildJoinMode.REPLICATED);
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
