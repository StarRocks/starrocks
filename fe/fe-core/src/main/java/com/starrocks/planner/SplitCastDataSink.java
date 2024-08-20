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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.analysis.Expr;
import com.starrocks.thrift.TDataSink;
import com.starrocks.thrift.TDataSinkType;
import com.starrocks.thrift.TDataStreamSink;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.TExpr;
import com.starrocks.thrift.TPlanFragmentDestination;
import com.starrocks.thrift.TSplitDataStreamSink;

import java.util.List;
import java.util.stream.Collectors;

public class SplitCastDataSink extends DataSink {
    // SplitCastDataSink will have multiple dest fragment
    private final List<DataStreamSink> dataStreamSinks = Lists.newArrayList();
    // every DataStreamSink can have multiple destinations
    private final List<List<TPlanFragmentDestination>> destinations = Lists.newArrayList();
    // if splitExprs[i] is true for [0,i], then data will be sent to dest fragment i
    private final List<Expr> splitExprs = Lists.newArrayList();

    @Override
    public String getExplainString(String prefix, TExplainLevel explainLevel) {
        StringBuilder sb = new StringBuilder();

        sb.append(prefix).append("SplitCastDataSink\n");
        for (int i = 0; i < dataStreamSinks.size(); i++) {
            sb.append(dataStreamSinks.get(i).getExplainString(prefix, explainLevel));
            sb.append(prefix + splitExprs.get(i).explain() + "\n");
        }
        return sb.toString();
    }

    @Override
    public String getVerboseExplain(String prefix) {
        StringBuilder sb = new StringBuilder();

        sb.append(prefix).append("SplitCastDataSink:\n");
        for (int i = 0; i < dataStreamSinks.size(); i++) {
            sb.append(dataStreamSinks.get(i).getVerboseExplain(prefix));
            sb.append(prefix + "Split expr: " + splitExprs.get(i).explain() + "\n");
        }

        return sb.toString();
    }

    @Override
    protected TDataSink toThrift() {
        List<TDataStreamSink> streamSinkList = dataStreamSinks.stream().map(d -> d.toThrift().getStream_sink())
                .collect(Collectors.toList());
        List<TExpr> tSplitExprs = this.splitExprs.stream().map(Expr::treeToThrift).collect(Collectors.toList());
        TSplitDataStreamSink sink = new TSplitDataStreamSink();
        sink.setSinks(streamSinkList);
        sink.setDestinations(destinations);
        sink.setSplitExprs(tSplitExprs);

        TDataSink result = new TDataSink(TDataSinkType.SPLIT_DATA_STREAM_SINK);
        result.setSplit_stream_sink(sink);
        return result;
    }

    @Override
    public PlanNodeId getExchNodeId() {
        Preconditions.checkState(false);
        return null;
    }

    @Override
    public DataPartition getOutputPartition() {
        Preconditions.checkState(false);
        return null;
    }

    public List<DataStreamSink> getDataStreamSinks() {
        return dataStreamSinks;
    }

    public List<List<TPlanFragmentDestination>> getDestinations() {
        return destinations;
    }

    public List<Expr> getSplitExprs() {
        return splitExprs;
    }

    @Override
    public boolean canUseRuntimeAdaptiveDop() {
        return true;
    }
}
