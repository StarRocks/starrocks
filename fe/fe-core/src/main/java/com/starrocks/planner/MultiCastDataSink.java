// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.planner;

import com.clearspring.analytics.util.Lists;
import com.clearspring.analytics.util.Preconditions;
import com.starrocks.thrift.TDataSink;
import com.starrocks.thrift.TDataSinkType;
import com.starrocks.thrift.TDataStreamSink;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.TMultiCastDataStreamSink;
import com.starrocks.thrift.TPlanFragmentDestination;

import java.util.List;
import java.util.stream.Collectors;

public class MultiCastDataSink extends DataSink {
    private final List<DataStreamSink> dataStreamSinks = Lists.newArrayList();
    private final List<List<TPlanFragmentDestination>> destinations = Lists.newArrayList();

    @Override
    public String getExplainString(String prefix, TExplainLevel explainLevel) {
        StringBuilder sb = new StringBuilder();

        sb.append(prefix).append("MultiCastDataSinks\n");
        for (DataStreamSink dataStreamSink : dataStreamSinks) {
            sb.append(dataStreamSink.getExplainString(prefix, explainLevel));
        }

        return sb.toString();
    }

    @Override
    public String getVerboseExplain(String prefix) {
        StringBuilder sb = new StringBuilder();

        sb.append(prefix).append("MultiCastDataSinks:\n");
        for (DataStreamSink dataStreamSink : dataStreamSinks) {
            sb.append(dataStreamSink.getVerboseExplain(prefix));
        }

        return sb.toString();
    }

    @Override
    protected TDataSink toThrift() {
        List<TDataStreamSink> streamSinkList = dataStreamSinks.stream().map(d -> d.toThrift().getStream_sink())
                .collect(Collectors.toList());
        TMultiCastDataStreamSink sink = new TMultiCastDataStreamSink(streamSinkList, destinations);

        TDataSink result = new TDataSink(TDataSinkType.MULTI_CAST_DATA_STREAM_SINK);
        result.setMulti_cast_stream_sink(sink);
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

    @Override
    public boolean canUsePipeLine() {
        return true;
    }
}
