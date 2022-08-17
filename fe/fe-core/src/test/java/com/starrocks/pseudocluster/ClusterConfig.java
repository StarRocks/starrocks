// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
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
