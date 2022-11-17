// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.scheduler.mv;

import com.google.common.base.Preconditions;
import com.google.gson.annotations.SerializedName;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.gson.GsonUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The incremental maintenance of MV consists of epochs, whose lifetime is defined as:
 * 1. Triggered by transaction publish
 * 2. Acquire the last-committed binlog LSN and latest binlog LSN
 * 3. Start a transaction for incremental maintaining the MV
 * 4. Schedule task executor to consume binlog since last-committed, and apply these changes to MV
 * 5. Commit the transaction to make is visible to user
 * 6. Commit the binlog consumption LSN(be atomic with transaction commitment to make)
 */
public class MVEpoch implements Writable {
    @SerializedName("mvId")
    long mvId;
    @SerializedName("epochState")
    private final AtomicReference<EpochState> state;
    @SerializedName("binlogState")
    private BinlogConsumeStateVO binlogState;
    @SerializedName("startTimeMilli")
    public long startTimeMilli;
    @SerializedName("commitTimeMilli")
    public long commitTimeMilli;

    // Ephemeral states
    public transient long transactionId;

    public MVEpoch() {
        this.startTimeMilli = System.currentTimeMillis();
        this.state = new AtomicReference<>(EpochState.INIT);
        this.binlogState = new BinlogConsumeStateVO();
    }

    public void onReady() {
        Preconditions.checkState(state.get().equals(EpochState.INIT));
        this.state.set(EpochState.READY);
    }

    public boolean onSchedule() {
        if (state.compareAndSet(EpochState.READY, EpochState.RUNNING)) {
            this.startTimeMilli = System.currentTimeMillis();
            return true;
        }
        return false;
    }

    public void onCommitting() {
        Preconditions.checkState(state.get().equals(EpochState.RUNNING));
        this.state.set(EpochState.COMMITTING);
    }

    public void onCommitted(BinlogConsumeStateVO binlogState) {
        Preconditions.checkState(state.get().equals(EpochState.COMMITTING));
        this.state.set(EpochState.COMMITTED);
        this.binlogState = binlogState;
        this.commitTimeMilli = System.currentTimeMillis();
    }

    public void onFailed() {
        this.state.set(EpochState.FAILED);
    }

    public void reset() {
        Preconditions.checkState(state.get().equals(EpochState.COMMITTED));
        this.state.set(EpochState.INIT);
    }

    public static MVEpoch read(DataInput input) throws IOException {
        return GsonUtils.GSON.fromJson(Text.readString(input), MVEpoch.class);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    /**
     * ┌────────┐      ┌────────┐     ┌───────────┐           ┌────────┐
     * │  INIT  ├──────┤ READY  ├────►│ RUNNING   ├──────────►│ FAILED │
     * └────────┘      └────────┘     └─────┬─────┘           └────────┘
     * ▲                │                     ▲
     * │                │                     │
     * │                │                     │
     * │          ┌─────▼─────┐               │
     * │          │ COMMITTING├───────────────┤
     * │          └─────┬─────┘               │
     * │                │                     │
     * │                │                     │
     * │                │                     │
     * │          ┌─────▼─────┐               │
     * └──────────┤ COMMITTED ├───────────────┘
     * └───────────┘
     */
    public enum EpochState {
        // Wait for data
        INIT,
        // Ready for scheduling
        READY,
        // Scheduled and under execution
        RUNNING,
        // Execution finished and start committing
        COMMITTING,
        // Committed epoch
        COMMITTED,
        // Failed for any reason
        FAILED;

        public boolean isFailed() {
            return this.equals(FAILED);
        }

    }
}
