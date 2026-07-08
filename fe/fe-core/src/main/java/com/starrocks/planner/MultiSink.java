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
import com.starrocks.thrift.TDataSink;
import com.starrocks.thrift.TDataSinkType;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.TMultiSink;
import com.starrocks.thrift.TMultiSinkBranch;
import com.starrocks.thrift.TOlapTableSink;

import java.util.List;

// Option X (CK-compatible logical-sink MV JOIN/agg): the sink of the single-root "collector" fragment. Holds N
// branches, each = a collector ExchangeNode id + the OlapTableSink that branch's stream is written to. The BE
// decomposes this into N independent pipelines: ExchangeSource(dest_node_id_i) -> OlapTableSink_i -> table_i.
// This keeps the ExecutionDAG single-rooted while writing N different tables from N different branch plans
// (unlike the broadcast MULTI_OLAP_TABLE_SINK path, which fans ONE chunk to per-sub transforms).
// See CH_REALTIME_MV_DESIGN.md section 12.
public class MultiSink extends DataSink {
    // per branch (child order): the collector ExchangeNode plan-node-id this branch's data arrives on,
    // and the OlapTableSink it is written to.
    private final List<Integer> destNodeIds = Lists.newArrayList();
    private final List<OlapTableSink> olapSinks = Lists.newArrayList();
    // Option X Fake-Root path: translate records (destNodeId, targetTableId) per branch; the branch
    // OlapTableSinks are built later by InsertPlanner post-processing (needs txn/session) via setOlapSinks.
    private final List<Long> targetTableIds = Lists.newArrayList();

    public void addBranch(int destExchangeNodeId, OlapTableSink olapSink) {
        destNodeIds.add(destExchangeNodeId);
        olapSinks.add(olapSink);
    }

    // Fake-Root translate: register a branch's collector-exchange id + its target table id (sink filled later).
    public void addBranchDest(int destExchangeNodeId, long targetTableId) {
        destNodeIds.add(destExchangeNodeId);
        targetTableIds.add(targetTableId);
    }

    public List<Long> getTargetTableIds() {
        return targetTableIds;
    }

    // InsertPlanner post-processing fills the per-branch OlapTableSinks (aligned with destNodeIds order).
    public void setOlapSinks(List<OlapTableSink> sinks) {
        olapSinks.clear();
        olapSinks.addAll(sinks);
    }

    public List<OlapTableSink> getOlapSinks() {
        return olapSinks;
    }

    public List<Integer> getDestNodeIds() {
        return destNodeIds;
    }

    @Override
    public String getExplainString(String prefix, TExplainLevel explainLevel) {
        StringBuilder sb = new StringBuilder();
        sb.append(prefix).append("MultiSink\n");
        for (int i = 0; i < olapSinks.size(); i++) {
            sb.append(prefix).append("  branch ").append(i).append(" <- exch node ")
                    .append(destNodeIds.get(i)).append("\n");
            sb.append(olapSinks.get(i).getExplainString(prefix + "  ", explainLevel));
        }
        return sb.toString();
    }

    @Override
    public String getVerboseExplain(String prefix) {
        StringBuilder sb = new StringBuilder();
        sb.append(prefix).append("MultiSink:\n");
        for (int i = 0; i < olapSinks.size(); i++) {
            sb.append(prefix).append("  branch ").append(i).append(" <- exch node ")
                    .append(destNodeIds.get(i)).append("\n");
            sb.append(olapSinks.get(i).getVerboseExplain(prefix + "  "));
        }
        return sb.toString();
    }

    @Override
    protected TDataSink toThrift() {
        TMultiSink multiSink = new TMultiSink();
        for (int i = 0; i < olapSinks.size(); i++) {
            TMultiSinkBranch branch = new TMultiSinkBranch();
            branch.setDest_node_id(destNodeIds.get(i));
            // A branch OlapTableSink is a plain target sink (no nested logical-sink fan-out), so toThrift()
            // returns an OLAP_TABLE_SINK carrying the TOlapTableSink directly.
            TOlapTableSink olap = olapSinks.get(i).toThrift().getOlap_table_sink();
            branch.setOlap_table_sink(olap);
            multiSink.addToBranches(branch);
        }
        TDataSink result = new TDataSink(TDataSinkType.MULTI_SINK);
        result.setMulti_sink(multiSink);
        return result;
    }

    @Override
    public PlanNodeId getExchNodeId() {
        // Terminal sink (writes to tables, not to a downstream exchange) — mirror OlapTableSink. Must NOT throw:
        // ProfilingExecPlan.buildFrom() calls getExchNodeId() on every fragment sink (null is treated as -1),
        // and throwing here aborts query-profile collection for the whole collector INSERT.
        return null;
    }

    @Override
    public DataPartition getOutputPartition() {
        return DataPartition.RANDOM;
    }
}
