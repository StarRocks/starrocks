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

import com.google.common.base.Preconditions;
import com.google.gson.annotations.SerializedName;
import com.starrocks.common.Config;
import com.starrocks.persist.ImageWriter;
import com.starrocks.persist.metablock.SRMetaBlockEOFException;
import com.starrocks.persist.metablock.SRMetaBlockException;
import com.starrocks.persist.metablock.SRMetaBlockID;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.persist.metablock.SRMetaBlockWriter;
import com.starrocks.server.GlobalStateMgr;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;

public class GtidGenerator {
    private static final Logger LOG = LogManager.getLogger(GtidGenerator.class);

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
    // Inclusive last millisecond the 42-bit timestamp field can represent.
    public static final long MAX_TIMESTAMP = EPOCH + (1L << 42) - 1;

    // Inclusive last gtid of the persisted batch. Every issued gtid is <= some persisted value.
    @SerializedName("bi")
    private long batchEndGtid = 0L;
    private long lastTimestamp = -1L;
    private long lastSequence = 0L;
    private boolean firstGtidAfterInit = false;

    /**
     * Image load and journal replay. The whole reserved batch is treated as consumed.
     * Never moves the watermark backwards. Must not be used as the leader WAL apply path.
     */
    public synchronized void init(long incoming) {
        firstGtidAfterInit = true;
        if (incoming > this.batchEndGtid) {
            this.batchEndGtid = incoming;
            this.lastTimestamp = (incoming >> TIMESTAMP_SHIFT) + EPOCH;
            this.lastSequence = incoming & MAX_SEQUENCE;
        }
    }

    public synchronized long getBatchEndGtid() {
        return batchEndGtid;
    }

    public synchronized long getLastTimestamp() {
        return lastTimestamp;
    }

    public synchronized long getLastSequence() {
        return lastSequence;
    }

    /**
     * {@code max(0, lastTimestamp - now)}. Positive when the generator is ahead of the local wall
     * clock (unused batch after failover, or a fast previous leader). Zero before the first issue,
     * and when wall-clock time has caught up.
     */
    public synchronized long getTimestampAheadOfClockMs() {
        if (lastTimestamp < 0L) {
            return 0L;
        }
        return Math.max(0L, lastTimestamp - timeGen());
    }

    public synchronized long nextGtid() {
        // A gtid identifies one operation cluster-wide, and only the leader persists the operations that
        // carry one. A value handed out on any other node shares the format but nothing keeps it distinct
        // from what the leader hands out at the same moment.
        Preconditions.checkState(GlobalStateMgr.getCurrentState().isLeader(),
                "a gtid can only be generated on the leader");

        long now = timeGen();
        long timestamp = now;
        if (timestamp < lastTimestamp) {
            timestamp = lastTimestamp;
        }

        long nextSeq;
        if (lastTimestamp == timestamp) {
            nextSeq = lastSequence + 1;
            if (nextSeq > MAX_SEQUENCE) {
                timestamp += 1;
                nextSeq = 0L;
            }
        } else {
            nextSeq = 0L;
        }

        if (timestamp - EPOCH >= (1L << 42)) {
            throw new IllegalStateException("Timestamp overflow");
        }

        long candidate = encode(timestamp, nextSeq);
        if (candidate > batchEndGtid) {
            persistBatchEndGtid(nextBatchEndGtid(timestamp));
        }

        if (firstGtidAfterInit) {
            firstGtidAfterInit = false;
            if (timestamp > now) {
                logFirstGtidAfterInit(timestamp, now);
            }
        }

        lastTimestamp = timestamp;
        lastSequence = nextSeq;
        return candidate;
    }

    public synchronized long lastGtid() {
        return encode(lastTimestamp, lastSequence);
    }

    public synchronized void setLastGtid(long id) {
        this.lastTimestamp = (id >> TIMESTAMP_SHIFT) + EPOCH;
        this.lastSequence = id & MAX_SEQUENCE;
    }

    public static long getGtid(long timestamp) {
        return encode(timestamp, 0L);
    }

    public static long encode(long timestamp, long sequence) {
        return ((timestamp - EPOCH) << TIMESTAMP_SHIFT) | (CLUSTER_ID << CLUSTER_ID_SHIFT) | sequence;
    }

    public void save(ImageWriter imageWriter) throws IOException, SRMetaBlockException {
        SRMetaBlockWriter writer = imageWriter.getBlockWriter(SRMetaBlockID.GTID_GENERATOR, 1);
        writer.writeJson(this);
        writer.close();
    }

    public void load(SRMetaBlockReader reader) throws IOException, SRMetaBlockException, SRMetaBlockEOFException {
        GtidGenerator loaded = reader.readJson(GtidGenerator.class);
        init(loaded.getBatchEndGtid());
    }

    protected long timeGen() {
        return System.currentTimeMillis();
    }

    /**
     * Persist {@code newBatchEndGtid} then apply it on success. Leader apply only advances the
     * batch end; it must not call {@link #init} or the in-flight sequence would be wiped.
     */
    protected void persistBatchEndGtid(long newBatchEndGtid) {
        GlobalStateMgr.getCurrentState().getEditLog().logSaveGtid(newBatchEndGtid, wal -> {
            applyBatchEndGtid(newBatchEndGtid);
        });
    }

    protected void applyBatchEndGtid(long incoming) {
        if (incoming > batchEndGtid) {
            batchEndGtid = incoming;
        }
    }

    static long nextBatchEndGtid(long timestamp) {
        // Last gtid this generator can issue in [timestamp, timestamp + W).
        // Not getGtid(timestamp + W) - 1: subtracting 1 fills the cluster bits, which this
        // generator never sets.
        long window = batchWindowMs();
        if (timestamp < EPOCH || timestamp > MAX_TIMESTAMP) {
            throw new IllegalStateException("Timestamp overflow");
        }
        long remaining = MAX_TIMESTAMP - timestamp + 1L;
        if (window > remaining) {
            throw new IllegalStateException(
                    "gtid_batch_window_ms " + window + " exceeds remaining timestamp range " + remaining);
        }
        return encode(timestamp + window - 1, MAX_SEQUENCE);
    }

    static long batchWindowMs() {
        long window = Config.gtid_batch_window_ms;
        return window < 1L ? 1L : window;
    }

    /**
     * First {@link #nextGtid()} after {@link #init}: the issued timestamp was ahead of wall clock,
     * so the consumed batch actually affected this id. No-op when the first id uses {@code now}.
     */
    protected void logFirstGtidAfterInit(long timestamp, long now) {
        LOG.info("first gtid after init is {} ms ahead of now (timestamp={}, now={}, batchEndGtid={})",
                timestamp - now, timestamp, now, batchEndGtid);
    }
}
