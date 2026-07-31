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

import com.starrocks.thrift.TDataSink;
import com.starrocks.thrift.TDataSinkType;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.TRemoteScanResultSink;
import com.starrocks.thrift.TStarRocksScanTransport;

public class RemoteScanResultSink extends DataSink {
    private final TStarRocksScanTransport transport;
    private final String scanToken;
    private final long expireMs;

    public RemoteScanResultSink(TStarRocksScanTransport transport, String scanToken, long expireMs) {
        this.transport = transport;
        this.scanToken = scanToken;
        this.expireMs = expireMs;
    }

    @Override
    public String getExplainString(String prefix, TExplainLevel explainLevel) {
        StringBuilder builder = new StringBuilder();
        builder.append(prefix).append("REMOTE SCAN RESULT SINK\n");
        builder.append(prefix).append("  TRANSPORT: ").append(transport).append("\n");
        return builder.toString();
    }

    @Override
    protected TDataSink toThrift() {
        TDataSink dataSink = new TDataSink(TDataSinkType.REMOTE_SCAN_RESULT_SINK);
        TRemoteScanResultSink remoteScanSink = new TRemoteScanResultSink();
        remoteScanSink.setTransport(transport);
        remoteScanSink.setScan_token(scanToken);
        remoteScanSink.setExpire_ms(expireMs);
        dataSink.setRemote_scan_result_sink(remoteScanSink);
        return dataSink;
    }

    @Override
    public PlanNodeId getExchNodeId() {
        return null;
    }

    @Override
    public DataPartition getOutputPartition() {
        return null;
    }
}
