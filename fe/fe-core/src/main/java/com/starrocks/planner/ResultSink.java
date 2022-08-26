// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/planner/ResultSink.java

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
import com.starrocks.analysis.OutFileClause;
import com.starrocks.thrift.TDataSink;
import com.starrocks.thrift.TDataSinkType;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TResultFileSinkOptions;
import com.starrocks.thrift.TResultSink;
import com.starrocks.thrift.TResultSinkType;

/**
 * Result sink that forwards data to
 * 1. the FE data receiver, which result the final query result to user client. Or,
 * 2. files that save the result data
 */
public class ResultSink extends DataSink {
    private final PlanNodeId exchNodeId;
    private TResultSinkType sinkType;
    private String brokerName;
    private TResultFileSinkOptions fileSinkOptions;

    public ResultSink(PlanNodeId exchNodeId, TResultSinkType sinkType) {
        this.exchNodeId = exchNodeId;
        this.sinkType = sinkType;
    }

    @Override
    public String getExplainString(String prefix, TExplainLevel explainLevel) {
        StringBuilder strBuilder = new StringBuilder();
        strBuilder.append(prefix + "RESULT SINK\n");
        return strBuilder.toString();
    }

    @Override
    protected TDataSink toThrift() {
        TDataSink result = new TDataSink(TDataSinkType.RESULT_SINK);
        TResultSink tResultSink = new TResultSink();
        tResultSink.setType(sinkType);
        if (fileSinkOptions != null) {
            tResultSink.setFile_options(fileSinkOptions);
        }
        result.setResult_sink(tResultSink);
        return result;
    }

    @Override
    public PlanNodeId getExchNodeId() {
        return exchNodeId;
    }

    @Override
    public DataPartition getOutputPartition() {
        return null;
    }

    public boolean isOutputFileSink() {
        return sinkType == TResultSinkType.FILE;
    }

    public boolean needBroker() {
        return fileSinkOptions.isSetUse_broker() && fileSinkOptions.use_broker;
    }

    public String getBrokerName() {
        return brokerName;
    }

    public void setOutfileInfo(OutFileClause outFileClause) {
        sinkType = TResultSinkType.FILE;
        fileSinkOptions = outFileClause.toSinkOptions();
        brokerName = outFileClause.getBrokerDesc() == null ? null : outFileClause.getBrokerDesc().getName();
    }

    public void setBrokerAddr(String ip, int port) {
        Preconditions.checkNotNull(fileSinkOptions);
        fileSinkOptions.setBroker_addresses(Lists.newArrayList(new TNetworkAddress(ip, port)));
    }

    @Override
    public boolean canUsePipeLine() {
        return sinkType == TResultSinkType.MYSQL_PROTOCAL;
    }
}
