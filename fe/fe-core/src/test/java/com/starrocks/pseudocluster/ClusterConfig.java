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

package com.starrocks.pseudocluster;

import java.util.Random;

public class ClusterConfig {
    int tableSinkWriteLatencyMsMin = 0;
    int tableSinkWriteLatencyMsMax = 0;
    int publishTaskLatencyMsMin = 0;
    int publishTaskLatencyMsMax = 0;

    Random random = new Random();

    private void waitLatency(int latency) {
        if (latency > 0) {
            try {
                Thread.sleep(latency);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    void injectTableSinkWriteLatency() {
        waitLatency(getTableSinkWriteLatencyMs());
    }

    void injectPublishTaskLatency() {
        waitLatency(getPublishTaskLatencyMs());
    }

    int getTableSinkWriteLatencyMs() {
        int range = tableSinkWriteLatencyMsMax - tableSinkWriteLatencyMsMin;
        if (range > 0) {
            return tableSinkWriteLatencyMsMin + random.nextInt(range);
        } else {
            return tableSinkWriteLatencyMsMin;
        }
    }

    int getPublishTaskLatencyMs() {
        int range = publishTaskLatencyMsMax - publishTaskLatencyMsMin;
        if (range > 0) {
            return publishTaskLatencyMsMin + random.nextInt(range);
        } else {
            return publishTaskLatencyMsMin;
        }
    }

    /**
     * set tableSink write stage latency [min, max)
     */
    public void setTableSinkWriteLatencyMs(int min, int max) {
        tableSinkWriteLatencyMsMin = min;
        tableSinkWriteLatencyMsMax = max;
    }

    /**
     * set write txn publish task latency [min, max)
     */
    public void setPublishTaskLatencyMs(int min, int max) {
        publishTaskLatencyMsMin = min;
        publishTaskLatencyMsMax = max;
    }
}
