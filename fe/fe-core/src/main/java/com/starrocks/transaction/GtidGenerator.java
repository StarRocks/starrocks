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

package com.starrocks.transaction;

import com.google.gson.annotations.SerializedName;

public class GtidGenerator {
    // |-- 1bit --|-- 42bit --|-- 8bit --|-- 13bit --|
    // |    0     | timestamp |  cluster |  sequence |
    public static final long EPOCH = 1577836800000L; // 2020-01-01 00:00:00 UTC
    public static final long CLUSTER_ID_BITS = 8L;
    public static final long SEQUENCE_BITS = 13L;

    public static final long MAX_CLUSTER_ID = -1L ^ (-1L << CLUSTER_ID_BITS);
    public static final long MAX_SEQUENCE = -1L ^ (-1L << SEQUENCE_BITS);

    public static final long CLUSTER_ID_SHIFT = SEQUENCE_BITS;
    public static final long TIMESTAMP_SHIFT = SEQUENCE_BITS + CLUSTER_ID_BITS;

    public static final long CLUSTER_ID = 0L;

    @SerializedName("lastTimestamp")
    private long lastTimestamp = -1L;
    @SerializedName("sequence")
    private long sequence = 0L;

    public GtidGenerator() {
        nextGtid();
    }

    public synchronized long nextGtid() {
        long timestamp = timeGen();

        if (timestamp < lastTimestamp) {
            timestamp = lastTimestamp;
        }

        if (lastTimestamp == timestamp) {
            sequence = (sequence + 1) & MAX_SEQUENCE;
            if (sequence == 0) {
                timestamp += 1;
            }
        } else {
            sequence = 0L;
        }

        if (timestamp - EPOCH >= (1L << 42)) {
            throw new IllegalStateException("Timestamp overflow");
        }

        lastTimestamp = timestamp;
        return ((timestamp - EPOCH) << TIMESTAMP_SHIFT) | (CLUSTER_ID << CLUSTER_ID_SHIFT) | sequence;
    }

    public synchronized long lastGtid() {
        return ((lastTimestamp - EPOCH) << TIMESTAMP_SHIFT) | (CLUSTER_ID << CLUSTER_ID_SHIFT) | sequence;
    }

    public synchronized void setLastGtid(long id) {
        this.lastTimestamp = (id >> TIMESTAMP_SHIFT) + EPOCH;
        this.sequence = id & MAX_SEQUENCE;
    }

    public static long getGtid(long timestamp) {
        return ((timestamp - EPOCH) << TIMESTAMP_SHIFT) | (CLUSTER_ID << CLUSTER_ID_SHIFT);
    }

    protected long timeGen() {
        return System.currentTimeMillis();
    }
}

