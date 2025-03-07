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

package com.starrocks.connector;

import com.starrocks.thrift.TScanRangeLocations;

import java.util.List;

public abstract class ConnectorScanRangeSource {
    RemoteFilesSampleStrategy strategy = new RemoteFilesSampleStrategy();

    protected abstract List<TScanRangeLocations> getSourceOutputs(int maxSize);

    public List<TScanRangeLocations> getOutputs(int maxSize) {
        return strategy.sample(getSourceOutputs(maxSize));
    }

    public List<TScanRangeLocations> getAllOutputs() {
        return getOutputs(Integer.MAX_VALUE);
    }

    protected abstract boolean sourceHasMoreOutput();

    public boolean hasMoreOutput() {
        if (strategy.enough()) {
            return false;
        }
        return sourceHasMoreOutput();
    }

    public void setSampleStrategy(RemoteFilesSampleStrategy strategy) {
        this.strategy = strategy;
    }
}
