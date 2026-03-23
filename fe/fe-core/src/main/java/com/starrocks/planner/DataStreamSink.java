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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/planner/DataStreamSink.java

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

import com.starrocks.planner.expression.ExprToThrift;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.ExprToSql;
import com.starrocks.thrift.TDataSink;
import com.starrocks.thrift.TDataSinkType;
import com.starrocks.thrift.TDataStreamSink;
import com.starrocks.thrift.TExplainLevel;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Data sink that forwards data to an exchange node.
 */
public class DataStreamSink extends DataSink {
    private final PlanNodeId exchNodeId;
    private int exchDop;

    private DataPartition outputPartition;

    private boolean isMerge;

    // Specify the columns which need to send, used on MultiCastSink
    private List<Integer> outputColumnIds;

    // Specify the limit on output columns, used on MultiCastSink
    private long limit;

    // Per-consumer conjuncts for MultiCast filter push-down
    private List<Expr> conjuncts;

    // Per-consumer runtime filters for MultiCast RF push-down
    private List<RuntimeFilterDescription> runtimeFilters;

    public DataStreamSink(PlanNodeId exchNodeId) {
        this.exchNodeId = exchNodeId;
        this.limit = -1;
    }

    @Override
    public PlanNodeId getExchNodeId() {
        return exchNodeId;
    }

    @Override
    public DataPartition getOutputPartition() {
        return outputPartition;
    }

    public void setExchDop(int exchDop) {
        this.exchDop = exchDop;
    }

    public void setPartition(DataPartition partition) {
        outputPartition = partition;
    }

    public void setMerge(boolean isMerge) {
        this.isMerge = isMerge;
    }

    public void setOutputColumnIds(List<Integer> outputColumnIds) {
        this.outputColumnIds = outputColumnIds;
    }

    public void setLimit(long limit) { this.limit = limit; }

    public void setConjuncts(List<Expr> conjuncts) {
        this.conjuncts = conjuncts;
    }

    public void setRuntimeFilters(List<RuntimeFilterDescription> runtimeFilters) {
        this.runtimeFilters = runtimeFilters;
    }

    public List<RuntimeFilterDescription> getRuntimeFilters() {
        return runtimeFilters;
    }

    @Override
    public String getExplainString(String prefix, TExplainLevel explainLevel) {
        StringBuilder strBuilder = new StringBuilder();
        strBuilder.append(prefix + "STREAM DATA SINK\n");
        strBuilder.append(prefix + "  EXCHANGE ID: " + exchNodeId + "\n");
        if (outputPartition != null) {
            strBuilder.append(prefix + "  " + outputPartition.getExplainString(explainLevel));
        }
        if (conjuncts != null && !conjuncts.isEmpty()) {
            strBuilder.append(prefix + "  CONJUNCTS: " +
                    formatExprs(conjuncts, explainLevel) + "\n");
        }
        if (runtimeFilters != null && !runtimeFilters.isEmpty()) {
            strBuilder.append(prefix + "  RUNTIME FILTERS: " +
                    runtimeFilters.stream().map(rf -> "RF" + rf.getFilterId())
                            .collect(Collectors.joining(", ")) + "\n");
        }
        return strBuilder.toString();
    }

    @Override
    public String getVerboseExplain(String prefix) {
        StringBuilder strBuilder = new StringBuilder();
        if (outputPartition != null) {
            strBuilder.append(prefix).append("OutPut Partition: ").
                    append(outputPartition.getExplainString(TExplainLevel.VERBOSE));
        }
        strBuilder.append(prefix).append("OutPut Exchange Id: ").append(exchNodeId).append("\n");
        if (conjuncts != null && !conjuncts.isEmpty()) {
            strBuilder.append(prefix).append("Conjuncts: ").
                    append(formatExprs(conjuncts, TExplainLevel.VERBOSE)).append("\n");
        }
        if (runtimeFilters != null && !runtimeFilters.isEmpty()) {
            strBuilder.append(prefix).append("RuntimeFilters: ").
                    append(runtimeFilters.stream().map(rf -> "RF" + rf.getFilterId())
                            .collect(Collectors.joining(", "))).append("\n");
        }
        return strBuilder.toString();
    }

    private static String formatExprs(List<? extends Expr> exprs, TExplainLevel level) {
        if (exprs == null) {
            return "";
        }
        StringBuilder output = new StringBuilder();
        for (int i = 0; i < exprs.size(); ++i) {
            if (i > 0) {
                output.append(", ");
            }
            if (level.equals(TExplainLevel.NORMAL)) {
                output.append(ExprToSql.toSql(exprs.get(i)));
            } else {
                output.append(ExprToSql.explain(exprs.get(i)));
            }
        }
        return output.toString();
    }

    @Override
    protected TDataSink toThrift() {
        TDataSink result = new TDataSink(TDataSinkType.DATA_STREAM_SINK);
        TDataStreamSink tStreamSink =
                new TDataStreamSink(exchNodeId.asInt(), outputPartition.toThrift());
        tStreamSink.setIs_merge(isMerge);
        tStreamSink.setDest_dop(exchDop);
        if (outputColumnIds != null && !outputColumnIds.isEmpty()) {
            tStreamSink.setOutput_columns(outputColumnIds);
        }
        if (limit != -1) {
            tStreamSink.setLimit(limit);
        }
        if (conjuncts != null && !conjuncts.isEmpty()) {
            tStreamSink.setConjuncts(ExprToThrift.treesToThrift(conjuncts));
        }
        if (runtimeFilters != null && !runtimeFilters.isEmpty()) {
            tStreamSink.setRuntime_filters(
                    runtimeFilters.stream().map(RuntimeFilterDescription::toThrift).collect(Collectors.toList()));
        }
        result.setStream_sink(tStreamSink);
        return result;
    }

    @Override
    public boolean canUsePipeLine() {
        return true;
    }

    @Override
    public boolean canUseRuntimeAdaptiveDop() {
        return true;
    }
}
