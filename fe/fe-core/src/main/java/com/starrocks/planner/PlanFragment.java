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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/planner/PlanFragment.java

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

package com.starrocks.planner;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.analysis.Expr;
import com.starrocks.common.Pair;
import com.starrocks.common.TreeNode;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.statistics.ColumnDict;
import com.starrocks.thrift.TCacheParam;
import com.starrocks.thrift.TDataSink;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.TExpr;
import com.starrocks.thrift.TGlobalDict;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TPartitionType;
import com.starrocks.thrift.TPlanFragment;
import com.starrocks.thrift.TResultSinkType;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections4.MapUtils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * PlanFragments form a tree structure via their ExchangeNodes. A tree of fragments
 * connected in that way forms a plan. The output of a plan is produced by the root
 * fragment and is either the result of the query or an intermediate result
 * needed by a different plan (such as a hash table).
 * <p>
 * Plans are grouped into cohorts based on the consumer of their output: all
 * plans that materialize intermediate results for a particular consumer plan
 * are grouped into a single cohort.
 * <p>
 * A PlanFragment encapsulates the specific tree of execution nodes that
 * are used to produce the output of the plan fragment, as well as output exprs,
 * destination node, etc. If there are no output exprs, the full row that is
 * is produced by the plan root is marked as materialized.
 * <p>
 * A plan fragment can have one or many instances, each of which in turn is executed by
 * an individual node and the output sent to a specific instance of the destination
 * fragment (or, in the case of the root fragment, is materialized in some form).
 * <p>
 * A hash-partitioned plan fragment is the result of one or more hash-partitioning data
 * streams being received by plan nodes in this fragment. In the future, a fragment's
 * data partition could also be hash partitioned based on a scan node that is reading
 * from a physically hash-partitioned table.
 * <p>
 * The sequence of calls is:
 * - c'tor
 * - assemble with getters, etc.
 * - finalize()
 * - toThrift()
 * <p>
 * TODO: the tree of PlanNodes is connected across fragment boundaries, which makes
 * it impossible search for things within a fragment (using TreeNode functions);
 * fix that
 */
public class PlanFragment extends TreeNode<PlanFragment> {
    // id for this plan fragment
    protected final PlanFragmentId fragmentId;

    // root of plan tree executed by this fragment
    protected PlanNode planRoot;

    // exchange node to which this fragment sends its output
    private ExchangeNode destNode;

    // if null, outputs the entire row produced by planRoot
    protected ArrayList<Expr> outputExprs;

    // created in finalize() or set in setSink()
    protected DataSink sink;

    // specification of the partition of the input of this fragment;
    // an UNPARTITIONED fragment is executed on only a single node
    // TODO: improve this comment, "input" is a bit misleading
    protected final DataPartition dataPartition;

    // specification of how the output of this fragment is partitioned (i.e., how
    // it's sent to its destination);
    // if the output is UNPARTITIONED, it is being broadcast
    protected DataPartition outputPartition;

    // Whether query statistics is sent with every batch. In order to get the query
    // statistics correctly when query contains limit, it is necessary to send query 
    // statistics with every batch, or only in close.
    protected boolean transferQueryStatisticsWithEveryBatch;

    // TODO: SubstitutionMap outputSmap;
    // substitution map to remap exprs onto the output of this fragment, to be applied
    // at destination fragment

    // specification of the number of parallel when fragment is executed
    // default value is 1
    protected int parallelExecNum = 1;
    protected int pipelineDop = 1;
    protected boolean dopEstimated = false;

    // Whether to assign scan ranges to each driver sequence of pipeline,
    // for the normal backend assignment (not colocate, bucket, and replicated join).
    protected boolean assignScanRangesPerDriverSeq = false;
    protected boolean withLocalShuffle = false;

    protected final Map<Integer, RuntimeFilterDescription> buildRuntimeFilters = Maps.newTreeMap();
    protected final Map<Integer, RuntimeFilterDescription> probeRuntimeFilters = Maps.newTreeMap();

    protected List<Pair<Integer, ColumnDict>> queryGlobalDicts = Lists.newArrayList();
    protected Map<Integer, Expr> queryGlobalDictExprs;
    protected List<Pair<Integer, ColumnDict>> loadGlobalDicts = Lists.newArrayList();

    private final Set<Integer> runtimeFilterBuildNodeIds = Sets.newHashSet();

    private TCacheParam cacheParam = null;
    private boolean hasOlapTableSink = false;
    private boolean hasIcebergTableSink = false;
    private boolean hasHiveTableSink = false;
    private boolean hasTableFunctionTableSink = false;

    private boolean forceSetTableSinkDop = false;
    private boolean forceAssignScanRangesPerDriverSeq = false;

    private boolean useRuntimeAdaptiveDop = false;

    private boolean isShortCircuit = false;

    /**
     * C'tor for fragment with specific partition; the output is by default broadcast.
     */
    public PlanFragment(PlanFragmentId id, PlanNode root, DataPartition partition) {
        this.fragmentId = id;
        this.dataPartition = partition;
        this.outputPartition = DataPartition.UNPARTITIONED;
        this.transferQueryStatisticsWithEveryBatch = false;
        // when dop adaptation is enabled, parallelExecNum and pipelineDop set to degreeOfParallelism and 1 respectively
        // in default. these values just a hint to help determine numInstances and pipelineDop of a PlanFragment.
        setPlanRoot(root);
        setParallelExecNumIfExists();
        setFragmentInPlanTree(planRoot);
    }

    /**
     * Assigns 'this' as fragment of all PlanNodes in the plan tree rooted at node.
     * Does not traverse the children of ExchangeNodes because those must belong to a
     * different fragment.
     */
    private void setFragmentInPlanTree(PlanNode node) {
        if (node == null) {
            return;
        }
        node.setFragment(this);
        if (node instanceof ExchangeNode) {
            return;
        }
        for (PlanNode child : node.getChildren()) {
            setFragmentInPlanTree(child);
        }
    }

    public boolean canUsePipeline() {
        return getPlanRoot().canUsePipeLine() && getSink().canUsePipeLine();
    }

    public boolean canUseRuntimeAdaptiveDop() {
        return getPlanRoot().canUseRuntimeAdaptiveDop() && getSink().canUseRuntimeAdaptiveDop();
    }

    public void enableAdaptiveDop() {
        useRuntimeAdaptiveDop = true;
        // Constrict DOP as the power of two to make the strategy of decrement DOP easy.
        // After decreasing DOP from old_dop to new_dop, chunks from the i-th input driver is passed
        // to the j-th output driver, where j=i%new_dop.
        pipelineDop = Utils.computeMaxLEPower2(pipelineDop);
    }

    public void disableRuntimeAdaptiveDop() {
        useRuntimeAdaptiveDop = false;
    }

    public boolean isUseRuntimeAdaptiveDop() {
        return useRuntimeAdaptiveDop;
    }

    /**
     * Assign ParallelExecNum by PARALLEL_FRAGMENT_EXEC_INSTANCE_NUM in SessionVariable for synchronous request
     * Assign ParallelExecNum by default value for Asynchronous request
     */
    private void setParallelExecNumIfExists() {
        if (ConnectContext.get() != null) {
            if (ConnectContext.get().getSessionVariable().isEnablePipelineEngine()) {
                this.parallelExecNum = 1;
                this.pipelineDop = ConnectContext.get().getSessionVariable().getDegreeOfParallelism();
            } else {
                this.parallelExecNum = ConnectContext.get().getSessionVariable().getParallelExecInstanceNum();
                this.pipelineDop = 1;
            }
        }
    }

    public void limitMaxPipelineDop(int maxPipelineDop) {
        if (pipelineDop > maxPipelineDop) {
            pipelineDop = maxPipelineDop;
            if (useRuntimeAdaptiveDop) {
                pipelineDop = Utils.computeMaxLEPower2(pipelineDop);
            }
        }
    }

    public ExchangeNode getDestNode() {
        return destNode;
    }

    public DataPartition getOutputPartition() {
        return outputPartition;
    }

    /**
     * Parallel load fragment instance num in single host
     */
    public void setParallelExecNum(int parallelExecNum) {
        this.parallelExecNum = parallelExecNum;
    }

    public int getPipelineDop() {
        return pipelineDop;
    }

    public void setPipelineDop(int dop) {
        this.pipelineDop = dop;
    }

    public boolean hasTableSink() {
        return hasIcebergTableSink() || hasOlapTableSink() || hasHiveTableSink() || hasTableFunctionTableSink();
    }

    public boolean hasOlapTableSink() {
        return this.hasOlapTableSink;
    }

    public void setHasOlapTableSink() {
        this.hasOlapTableSink = true;
    }

    public boolean hasIcebergTableSink() {
        return this.hasIcebergTableSink;
    }

    public void setHasIcebergTableSink() {
        this.hasIcebergTableSink = true;
    }

    public boolean hasHiveTableSink() {
        return this.hasHiveTableSink;
    }

    public void setHasHiveTableSink() {
        this.hasHiveTableSink = true;
    }

    public boolean hasTableFunctionTableSink() {
        return this.hasTableFunctionTableSink;
    }

    public void setHasTableFunctionTableSink() {
        this.hasTableFunctionTableSink = true;
    }

    public boolean forceSetTableSinkDop() {
        return this.forceSetTableSinkDop;
    }

    public void setForceSetTableSinkDop() {
        this.forceSetTableSinkDop = true;
    }

    public boolean isWithLocalShuffle() {
        return withLocalShuffle;
    }

    public void setWithLocalShuffleIfTrue(boolean withLocalShuffle) {
        this.withLocalShuffle |= withLocalShuffle;
    }

    public boolean isAssignScanRangesPerDriverSeq() {
        return assignScanRangesPerDriverSeq;
    }

    public void setAssignScanRangesPerDriverSeq(boolean assignScanRangesPerDriverSeq) {
        this.assignScanRangesPerDriverSeq |= assignScanRangesPerDriverSeq;
    }

    public boolean isForceAssignScanRangesPerDriverSeq() {
        return forceAssignScanRangesPerDriverSeq;
    }

    public void setForceAssignScanRangesPerDriverSeq() {
        this.forceAssignScanRangesPerDriverSeq = true;
    }

    public void computeLocalRfWaitingSet(PlanNode root, boolean clearGlobalRuntimeFilter) {
        root.fillLocalRfWaitingSet(runtimeFilterBuildNodeIds);
        // TopN Filter should't wait
        if (root instanceof RuntimeFilterBuildNode && !(root instanceof SortNode)) {
            runtimeFilterBuildNodeIds.add(root.getId().asInt());
        }
        if (clearGlobalRuntimeFilter) {
            root.clearProbeRuntimeFilters();
            if (root instanceof RuntimeFilterBuildNode) {
                ((RuntimeFilterBuildNode) root).clearBuildRuntimeFilters();
            }
        }
        for (PlanNode child : root.getChildren()) {
            if (child.getFragment() == this) {
                computeLocalRfWaitingSet(child, clearGlobalRuntimeFilter);
            }
        }
    }

    public boolean isDopEstimated() {
        return dopEstimated;
    }

    public void setOutputExprs(List<Expr> outputExprs) {
        this.outputExprs = Expr.cloneList(outputExprs, null);
    }

    /**
     * Finalize plan tree and create stream sink, if needed.
     */
    public void createDataSink(TResultSinkType resultSinkType) {
        if (sink != null) {
            return;
        }
        if (destNode != null) {
            // we're streaming to an exchange node
            DataStreamSink streamSink = new DataStreamSink(destNode.getId());
            streamSink.setPartition(outputPartition);
            streamSink.setMerge(destNode.isMerge());
            streamSink.setFragment(this);
            sink = streamSink;
        } else {
            if (planRoot == null) {
                // only output expr, no FROM clause
                // "select 1 + 2"
                return;
            }
            // add ResultSink
            // we're streaming to an result sink
            sink = new ResultSink(planRoot.getId(), resultSinkType);
        }
    }

    public int getParallelExecNum() {
        return parallelExecNum;
    }

    public TPlanFragment toThrift() {
        TPlanFragment result = new TPlanFragment();
        if (planRoot != null) {
            result.setPlan(planRoot.treeToThrift());
        }
        if (outputExprs != null) {
            result.setOutput_exprs(Expr.treesToThrift(outputExprs));
        }
        if (sink != null) {
            result.setOutput_sink(sink.toThrift());
        }
        result.setPartition(dataPartition.toThrift());

        if (!queryGlobalDicts.isEmpty()) {
            result.setQuery_global_dicts(dictToThrift(queryGlobalDicts));
        }
        if (MapUtils.isNotEmpty(queryGlobalDictExprs)) {
            Preconditions.checkState(!queryGlobalDicts.isEmpty(), "Global dict expression error!");
            Map<Integer, TExpr> exprs = Maps.newHashMap();
            queryGlobalDictExprs.forEach((k, v) -> exprs.put(k, v.treeToThrift()));
            result.setQuery_global_dict_exprs(exprs);
        }
        if (!loadGlobalDicts.isEmpty()) {
            result.setLoad_global_dicts(dictToThrift(loadGlobalDicts));
        }
        if (cacheParam != null) {
            if (ConnectContext.get() != null) {
                SessionVariable sessionVariable = ConnectContext.get().getSessionVariable();
                cacheParam.setForce_populate(sessionVariable.isQueryCacheForcePopulate());
                cacheParam.setEntry_max_bytes(sessionVariable.getQueryCacheEntryMaxBytes());
                cacheParam.setEntry_max_rows(sessionVariable.getQueryCacheEntryMaxRows());
            }
            result.setCache_param(cacheParam);
        }
        return result;
    }

    /**
     * Create thrift fragment with the unique fields, including
     * - output_sink (only for MultiCastDataStreamSink and ExportSink).
     *
     * @return The thrift fragment with the unique fields.
     */
    public TPlanFragment toThriftForUniqueFields() {
        TPlanFragment result = new TPlanFragment();
        // Fill the required field.
        result.setPartition(dataPartition.toThrift());

        if (sink != null && (this instanceof MultiCastPlanFragment || sink instanceof ExportSink)) {
            result.setOutput_sink(sink.toThrift());
        }

        return result;
    }

    public List<TGlobalDict> dictToThrift(List<Pair<Integer, ColumnDict>> dicts) {
        List<TGlobalDict> result = Lists.newArrayList();
        for (Pair<Integer, ColumnDict> dictPair : dicts) {
            TGlobalDict globalDict = new TGlobalDict();
            globalDict.setColumnId(dictPair.first);
            List<ByteBuffer> strings = Lists.newArrayList();
            List<Integer> integers = Lists.newArrayList();
            for (Map.Entry<ByteBuffer, Integer> kv : dictPair.second.getDict().entrySet()) {
                strings.add(kv.getKey());
                integers.add(kv.getValue());
            }
            globalDict.setVersion(dictPair.second.getCollectedVersionTime());
            globalDict.setStrings(strings);
            globalDict.setIds(integers);
            result.add(globalDict);
        }
        return result;
    }

    // normalize dicts of the fragment, it is different from dictToThrift in three points:
    // 1. SlotIds must be replaced by remapped SlotIds;
    // 2. dict should be sorted according to its corresponding remapped SlotIds;
    public List<TGlobalDict> normalizeDicts(List<Pair<Integer, ColumnDict>> dicts, FragmentNormalizer normalizer) {
        List<TGlobalDict> result = Lists.newArrayList();
        // replace slot id with the remapped slot id, sort dicts according to remapped slot ids.
        List<Pair<Integer, ColumnDict>> sortedDicts =
                dicts.stream().map(p -> Pair.create(normalizer.remapSlotId(p.first), p.second))
                        .sorted(Comparator.comparingInt(p -> p.first)).collect(Collectors.toList());

        for (Pair<Integer, ColumnDict> dictPair : sortedDicts) {
            TGlobalDict globalDict = new TGlobalDict();
            globalDict.setColumnId(dictPair.first);
            globalDict.setVersion(dictPair.second.getCollectedVersionTime());
            result.add(globalDict);
        }
        return result;
    }

    public String getExplainString(TExplainLevel explainLevel) {
        StringBuilder str = new StringBuilder();
        Preconditions.checkState(dataPartition != null);
        str.append(" OUTPUT EXPRS:");

        StringBuilder outputBuilder = new StringBuilder();
        if (CollectionUtils.isNotEmpty(outputExprs)) {
            outputBuilder.append(outputExprs.stream().map(Expr::toSql)
                    .collect(Collectors.joining(" | ")));

        }

        str.append(outputBuilder);
        str.append("\n");
        str.append("  PARTITION: ").append(dataPartition.getExplainString(explainLevel)).append("\n");
        if (sink != null) {
            str.append(sink.getExplainString("  ", explainLevel)).append("\n");
        }
        if (planRoot != null) {
            str.append(planRoot.getExplainString("  ", "  ", explainLevel));
        }
        return str.toString();
    }

    public String getVerboseExplain() {
        StringBuilder str = new StringBuilder();
        Preconditions.checkState(dataPartition != null);
        if (CollectionUtils.isNotEmpty(outputExprs)) {
            str.append("  Output Exprs:");
            str.append(outputExprs.stream().map(Expr::toSql)
                    .collect(Collectors.joining(" | ")));
        }
        str.append("\n");
        str.append("  Input Partition: ").append(dataPartition.getExplainString(TExplainLevel.NORMAL));
        if (sink != null) {
            str.append(sink.getVerboseExplain("  ")).append("\n");
        }
        if (MapUtils.isNotEmpty(queryGlobalDictExprs)) {
            str.append("  Global Dict Exprs:\n");
            queryGlobalDictExprs.entrySet().stream()
                    .map(p -> "    " + p.getKey() + ": " + p.getValue().toMySql() + "\n").forEach(str::append);
            str.append("\n");
        }
        if (planRoot != null) {
            str.append(planRoot.getVerboseExplain("  ", "  "));
        }
        return str.toString();
    }

    public String getCostExplain() {
        StringBuilder str = new StringBuilder();
        Preconditions.checkState(dataPartition != null);
        StringBuilder outputBuilder = new StringBuilder();
        if (CollectionUtils.isNotEmpty(outputExprs)) {
            str.append("  Output Exprs:");
            outputBuilder.append(outputExprs.stream().map(Expr::toSql)
                    .collect(Collectors.joining(" | ")));
        }
        str.append(outputBuilder.toString());
        str.append("\n");
        str.append("  Input Partition: ").append(dataPartition.getExplainString(TExplainLevel.NORMAL));
        if (sink != null) {
            str.append(sink.getVerboseExplain("  ")).append("\n");
        }
        if (planRoot != null) {
            str.append(planRoot.getCostExplain("  ", "  "));
        }
        return str.toString();
    }

    /**
     * Returns true if this fragment is partitioned.
     */
    public boolean isPartitioned() {
        return (dataPartition.getType() != TPartitionType.UNPARTITIONED);
    }

    public PlanFragment getDestFragment() {
        if (destNode == null) {
            return null;
        }
        return destNode.getFragment();
    }

    public void setDestination(ExchangeNode destNode) {
        this.destNode = destNode;
        PlanFragment dest = getDestFragment();
        Preconditions.checkNotNull(dest);
        dest.addChild(this);
    }

    public void clearDestination() {
        this.destNode = null;
    }

    public DataPartition getDataPartition() {
        return dataPartition;
    }

    public void setOutputPartition(DataPartition outputPartition) {
        this.outputPartition = outputPartition;
    }

    public void clearOutputPartition() {
        this.outputPartition = DataPartition.UNPARTITIONED;
    }

    public PlanNode getPlanRoot() {
        return planRoot;
    }

    public void setPlanRoot(PlanNode root) {
        planRoot = root;
        setFragmentInPlanTree(planRoot);
    }

    public DataSink getSink() {
        return sink;
    }

    public void setSink(DataSink sink) {
        Preconditions.checkNotNull(sink);
        sink.setFragment(this);
        this.sink = sink;
    }

    public TDataSink sinkToThrift() {
        return sink != null ? sink.toThrift() : null;
    }

    public PlanFragmentId getFragmentId() {
        return fragmentId;
    }

    public boolean isTransferQueryStatisticsWithEveryBatch() {
        return transferQueryStatisticsWithEveryBatch;
    }

    public void collectBuildRuntimeFilters(PlanNode root) {
        if (root instanceof ExchangeNode) {
            return;
        }

        if (root instanceof RuntimeFilterBuildNode) {
            RuntimeFilterBuildNode rfBuildNode = (RuntimeFilterBuildNode) root;
            for (RuntimeFilterDescription description : rfBuildNode.getBuildRuntimeFilters()) {
                buildRuntimeFilters.put(description.getFilterId(), description);
            }
        }

        for (PlanNode node : root.getChildren()) {
            collectBuildRuntimeFilters(node);
        }
    }

    public void collectProbeRuntimeFilters(PlanNode root) {
        for (RuntimeFilterDescription description : root.getProbeRuntimeFilters()) {
            probeRuntimeFilters.put(description.getFilterId(), description);
        }

        if (root instanceof ExchangeNode) {
            return;
        }

        for (PlanNode node : root.getChildren()) {
            collectProbeRuntimeFilters(node);
        }
    }

    public void setRuntimeFilterMergeNodeAddresses(PlanNode root, TNetworkAddress host) {
        if (root instanceof ExchangeNode) {
            return;
        }

        if (root instanceof JoinNode) {
            JoinNode joinNode = (JoinNode) root;
            for (RuntimeFilterDescription description : joinNode.getBuildRuntimeFilters()) {
                description.addMergeNode(host);
            }
        }

        for (PlanNode node : root.getChildren()) {
            setRuntimeFilterMergeNodeAddresses(node, host);
        }
    }

    public Map<Integer, RuntimeFilterDescription> getBuildRuntimeFilters() {
        return buildRuntimeFilters;
    }

    public Map<Integer, RuntimeFilterDescription> getProbeRuntimeFilters() {
        return probeRuntimeFilters;
    }

    public List<Pair<Integer, ColumnDict>> getQueryGlobalDicts() {
        return this.queryGlobalDicts;
    }

    public Map<Integer, Expr> getQueryGlobalDictExprs() {
        return queryGlobalDictExprs;
    }

    public List<Pair<Integer, ColumnDict>> getLoadGlobalDicts() {
        return this.loadGlobalDicts;
    }

    public void setQueryGlobalDicts(List<Pair<Integer, ColumnDict>> dicts) {
        this.queryGlobalDicts = dicts;
    }

    // For plan fragment has join
    public void mergeQueryGlobalDicts(List<Pair<Integer, ColumnDict>> dicts) {
        if (this.queryGlobalDicts != dicts) {
            this.queryGlobalDicts = Stream.concat(this.queryGlobalDicts.stream(), dicts.stream()).distinct()
                    .collect(Collectors.toList());
        }
    }

    public void mergeQueryDictExprs(Map<Integer, Expr> queryGlobalDictExprs) {
        if (this.queryGlobalDictExprs != queryGlobalDictExprs) {
            Map<Integer, Expr> n = Maps.newHashMap(MapUtils.emptyIfNull(this.queryGlobalDictExprs));
            n.putAll(MapUtils.emptyIfNull(queryGlobalDictExprs));
            this.queryGlobalDictExprs = n;
        }
    }

    public void setQueryGlobalDictExprs(Map<Integer, Expr> queryGlobalDictExprs) {
        this.queryGlobalDictExprs = queryGlobalDictExprs;
    }

    public void setLoadGlobalDicts(
            List<Pair<Integer, ColumnDict>> loadGlobalDicts) {
        this.loadGlobalDicts = loadGlobalDicts;
    }

    public boolean hashLocalBucketShuffleRightOrFullJoin(PlanNode planRoot) {
        if (planRoot instanceof JoinNode) {
            JoinNode joinNode = (JoinNode) planRoot;
            if (joinNode.isLocalHashBucket() &&
                    (joinNode.getJoinOp().isFullOuterJoin() || joinNode.getJoinOp().isRightJoin())) {
                return true;
            }
        }
        for (PlanNode childNode : planRoot.getChildren()) {
            if (hashLocalBucketShuffleRightOrFullJoin(childNode)) {
                return true;
            }
        }
        return false;
    }

    public TCacheParam getCacheParam() {
        return cacheParam;
    }

    public void setCacheParam(TCacheParam cacheParam) {
        this.cacheParam = cacheParam;
    }

    public Map<PlanNodeId, ScanNode> collectScanNodes() {
        Map<PlanNodeId, ScanNode> scanNodes = Maps.newHashMap();
        Queue<PlanNode> queue = Lists.newLinkedList();
        queue.add(planRoot);
        while (!queue.isEmpty()) {
            PlanNode node = queue.poll();

            if (node instanceof ExchangeNode) {
                continue;
            }
            if (node instanceof ScanNode) {
                scanNodes.put(node.getId(), (ScanNode) node);
            }

            queue.addAll(node.getChildren());
        }

        return scanNodes;
    }

    public boolean isUnionFragment() {
        Deque<PlanNode> dq = new LinkedList<>();
        dq.offer(planRoot);

        while (!dq.isEmpty()) {
            PlanNode nd = dq.poll();

            if (nd instanceof UnionNode) {
                return true;
            }
            if (!(nd instanceof ExchangeNode)) {
                dq.addAll(nd.getChildren());
            }
        }
        return false;
    }

    public ArrayList<Expr> getOutputExprs() {
        return outputExprs;
    }

    public boolean isShortCircuit() {
        return isShortCircuit;
    }

    public void setShortCircuit(boolean shortCircuit) {
        isShortCircuit = shortCircuit;
    }

    /**
     * Returns the leftmost node of the fragment.
     */
    public PlanNode getLeftMostNode() {
        PlanNode node = planRoot;
        while (!node.getChildren().isEmpty() && !(node instanceof ExchangeNode)) {
            node = node.getChild(0);
        }
        return node;
    }

    public void reset() {
        // Do nothing.
    }

    public void disablePhysicalPropertyOptimize() {
        forEachNode(planRoot, PlanNode::disablePhysicalPropertyOptimize);
    }

    private void forEachNode(PlanNode root, Consumer<PlanNode> consumer) {
        consumer.accept(root);
        for (PlanNode child : root.getChildren()) {
            forEachNode(child, consumer);
        }
    }
}
