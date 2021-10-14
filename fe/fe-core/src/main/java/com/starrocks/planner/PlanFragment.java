// This file is made available under Elastic License 2.0.
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
import com.starrocks.analysis.Analyzer;
import com.starrocks.analysis.Expr;
import com.starrocks.common.Pair;
import com.starrocks.common.TreeNode;
import com.starrocks.common.UserException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.optimizer.statistics.ColumnDict;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.TGlobalDict;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TPartitionType;
import com.starrocks.thrift.TPlanFragment;
import com.starrocks.thrift.TResultSinkType;
import org.apache.commons.collections.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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
// Our new cost based query optimizer is more powerful and stable than old query optimizer,
// The old query optimizer related codes could be deleted safely.
// TODO: Remove old query optimizer related codes before 2021-09-30
public class PlanFragment extends TreeNode<PlanFragment> {
    private static final Logger LOG = LogManager.getLogger(PlanFragment.class);

    // id for this plan fragment
    private final PlanFragmentId fragmentId;

    // root of plan tree executed by this fragment
    private PlanNode planRoot;

    // exchange node to which this fragment sends its output
    private ExchangeNode destNode;

    // if null, outputs the entire row produced by planRoot
    private ArrayList<Expr> outputExprs;

    // created in finalize() or set in setSink()
    private DataSink sink;

    // specification of the partition of the input of this fragment;
    // an UNPARTITIONED fragment is executed on only a single node
    // TODO: improve this comment, "input" is a bit misleading
    private final DataPartition dataPartition;

    // specification of how the output of this fragment is partitioned (i.e., how
    // it's sent to its destination);
    // if the output is UNPARTITIONED, it is being broadcast
    private DataPartition outputPartition;

    // Whether query statistics is sent with every batch. In order to get the query
    // statistics correctly when query contains limit, it is necessary to send query 
    // statistics with every batch, or only in close.
    private boolean transferQueryStatisticsWithEveryBatch;

    // TODO: SubstitutionMap outputSmap;
    // substitution map to remap exprs onto the output of this fragment, to be applied
    // at destination fragment

    // specification of the number of parallel when fragment is executed
    // default value is 1
    private int parallelExecNum = 1;

    private final Map<Integer, RuntimeFilterDescription> buildRuntimeFilters = Maps.newTreeMap();
    private final Map<Integer, RuntimeFilterDescription> probeRuntimeFilters = Maps.newTreeMap();

    private List<Pair<Integer, ColumnDict>> globalDicts = Lists.newArrayList();

    /**
     * C'tor for fragment with specific partition; the output is by default broadcast.
     */
    public PlanFragment(PlanFragmentId id, PlanNode root, DataPartition partition) {
        this.fragmentId = id;
        this.planRoot = root;
        this.dataPartition = partition;
        this.outputPartition = DataPartition.UNPARTITIONED;
        this.transferQueryStatisticsWithEveryBatch = false;
        setParallelExecNumIfExists();
        setFragmentInPlanTree(planRoot);
    }

    /**
     * Assigns 'this' as fragment of all PlanNodes in the plan tree rooted at node.
     * Does not traverse the children of ExchangeNodes because those must belong to a
     * different fragment.
     */
    public void setFragmentInPlanTree(PlanNode node) {
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

    /**
     * Assign ParallelExecNum by PARALLEL_FRAGMENT_EXEC_INSTANCE_NUM in SessionVariable for synchronous request
     * Assign ParallelExecNum by default value for Asynchronous request
     */
    public void setParallelExecNumIfExists() {
        if (ConnectContext.get() != null) {
            parallelExecNum = ConnectContext.get().getSessionVariable().getParallelExecInstanceNum();
        }
    }

    /**
     * Parallel load fragment instance num in single host
     */
    public void setParallelExecNum(int parallelExecNum) {
        this.parallelExecNum = parallelExecNum;
    }

    public void setOutputExprs(List<Expr> outputExprs) {
        this.outputExprs = Expr.cloneList(outputExprs, null);
    }

    /**
     * Finalize plan tree and create stream sink, if needed.
     */
    public void finalize(Analyzer analyzer, boolean validateFileFormats)
            throws UserException {
        if (sink != null) {
            return;
        }
        if (destNode != null) {
            // we're streaming to an exchange node
            DataStreamSink streamSink = new DataStreamSink(destNode.getId());
            streamSink.setPartition(outputPartition);
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
            sink = new ResultSink(planRoot.getId(), TResultSinkType.MYSQL_PROTOCAL);
        }
    }

    public void finalizeForStatistic(boolean isStatistic) {
        if (sink != null) {
            return;
        }
        if (destNode != null) {
            // we're streaming to an exchange node
            DataStreamSink streamSink = new DataStreamSink(destNode.getId());
            streamSink.setPartition(outputPartition);
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
            if (isStatistic) {
                sink = new ResultSink(planRoot.getId(), TResultSinkType.STATISTIC);
            } else {
                sink = new ResultSink(planRoot.getId(), TResultSinkType.MYSQL_PROTOCAL);
            }
        }
    }

    /**
     * Return the number of nodes on which the plan fragment will execute.
     * invalid: -1
     */
    public int getNumNodes() {
        return dataPartition == DataPartition.UNPARTITIONED ? 1 : planRoot.getNumNodes();
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

        if (!globalDicts.isEmpty()) {
            List<TGlobalDict> dicts = Lists.newArrayList();
            for(Pair<Integer, ColumnDict> dictPair: globalDicts) {
                TGlobalDict globalDict = new TGlobalDict();
                globalDict.setColumnId(dictPair.first);
                List<String> strings = Lists.newArrayList();
                List<Integer> integers = Lists.newArrayList();
                for (Map.Entry<String, Integer> kv: dictPair.second.getDict().entrySet()) {
                    strings.add(kv.getKey());
                    integers.add(kv.getValue());
                }
                globalDict.setStrings(strings);
                globalDict.setIds(integers);
                dicts.add(globalDict);
            }
            result.setGlobal_dicts(dicts);
        }
        return result;
    }

    public String getExplainString(TExplainLevel explainLevel) {
        StringBuilder str = new StringBuilder();
        Preconditions.checkState(dataPartition != null);
        str.append(" OUTPUT EXPRS:");

        StringBuilder outputBuilder = new StringBuilder();
        List<String> vectorizedTrace = new ArrayList<>();
        if (CollectionUtils.isNotEmpty(outputExprs)) {
            outputBuilder.append(outputExprs.stream().map(Expr::toSql)
                    .collect(Collectors.joining(" | ")));

            outputExprs.forEach(v -> v.isVectorizedTrace(vectorizedTrace));
        }

        String outputString = outputBuilder.toString();
        for (String trace : vectorizedTrace) {
            outputString = outputString.replace(trace, "non-vectorized::" + trace);
        }

        str.append(outputString);
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

    public boolean isHashPartitioned() {
        return (dataPartition.getType() == TPartitionType.HASH_PARTITIONED);
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

    public DataPartition getDataPartition() {
        return dataPartition;
    }

    public DataPartition getOutputPartition() {
        return outputPartition;
    }

    public void setOutputPartition(DataPartition outputPartition) {
        this.outputPartition = outputPartition;
    }

    public PlanNode getPlanRoot() {
        return planRoot;
    }

    public void setPlanRoot(PlanNode root) {
        planRoot = root;
        setFragmentInPlanTree(planRoot);
    }

    /**
     * Adds a node as the new root to the plan tree. Connects the existing
     * root as the child of newRoot.
     */
    public void addPlanRoot(PlanNode newRoot) {
        Preconditions.checkState(newRoot.getChildren().size() == 1);
        newRoot.setChild(0, planRoot);
        planRoot = newRoot;
        planRoot.setFragment(this);
    }

    public DataSink getSink() {
        return sink;
    }

    public void setSink(DataSink sink) {
        Preconditions.checkNotNull(sink);
        sink.setFragment(this);
        this.sink = sink;
    }

    public PlanFragmentId getFragmentId() {
        return fragmentId;
    }

    public void setTransferQueryStatisticsWithEveryBatch(boolean value) {
        transferQueryStatisticsWithEveryBatch = value;
    }

    public boolean isTransferQueryStatisticsWithEveryBatch() {
        return transferQueryStatisticsWithEveryBatch;
    }

    public boolean isOutPutExprsVectorized() {
        if (outputExprs != null) {
            for (Expr expr : outputExprs) {
                if (!expr.isVectorized()) {
                    return false;
                }
            }
        }
        return true;
    }

    public void setOutPutExprsUseVectorized() {
        if (outputExprs != null) {
            for (Expr expr : outputExprs) {
                expr.setUseVectorized(true);
            }
        }
    }

    public void collectBuildRuntimeFilters(PlanNode root) {
        if (root instanceof ExchangeNode) {
            return;
        }

        if (root instanceof HashJoinNode) {
            HashJoinNode joinNode = (HashJoinNode) root;
            for (RuntimeFilterDescription description : joinNode.getBuildRuntimeFilters()) {
                buildRuntimeFilters.put(description.getFilterId(), description);
            }
        }

        for (PlanNode node : root.getChildren()) {
            collectBuildRuntimeFilters(node);
        }
    }

    public void collectProbeRuntimeFilters(PlanNode root) {
        if (root instanceof ExchangeNode) {
            return;
        }

        for (RuntimeFilterDescription description : root.getProbeRuntimeFilters()) {
            probeRuntimeFilters.put(description.getFilterId(), description);
        }

        for (PlanNode node : root.getChildren()) {
            collectProbeRuntimeFilters(node);
        }
    }

    public void setRuntimeFilterMergeNodeAddresses(PlanNode root, TNetworkAddress host) {
        if (root instanceof ExchangeNode) {
            return;
        }

        if (root instanceof HashJoinNode) {
            HashJoinNode joinNode = (HashJoinNode) root;
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

    public List<Pair<Integer, ColumnDict>> getGlobalDicts() {
        return globalDicts;
    }

    public void setGlobalDicts(List<Pair<Integer, ColumnDict>> dicts) {
        this.globalDicts = dicts;
    }
}
