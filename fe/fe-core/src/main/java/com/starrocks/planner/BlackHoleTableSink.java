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

public class BlackHoleTableSink extends DataSink {

    @Override
    public String getExplainString(String prefix, TExplainLevel explainLevel) {
        return prefix + "BLACKHOLE TABLE SINK\n";
    }

    @Override
    protected TDataSink toThrift() {
        return new TDataSink(TDataSinkType.BLACKHOLE_TABLE_SINK);
    }

    @Override
    public PlanNodeId getExchNodeId() {
        return null;
    }

    @Override
    public DataPartition getOutputPartition() {
        return DataPartition.RANDOM;
    }

    @Override
    public boolean canUseRuntimeAdaptiveDop() {
        return true;
    }
}
