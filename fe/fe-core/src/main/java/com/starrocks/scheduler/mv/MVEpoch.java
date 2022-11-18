// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.scheduler.mv;

import com.google.common.base.Preconditions;
import com.google.gson.annotations.SerializedName;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.gson.GsonUtils;
import lombok.Data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * The incremental maintenance of MV consists of epochs, whose lifetime is defined as:
 * 1. Triggered by transaction publish
 * 2. Acquire the last-committed binlog LSN and latest binlog LSN
 * 3. Start a transaction for incremental maintaining the MV
 * 4. Schedule task executor to consume binlog since last-committed, and apply these changes to MV
 * 5. Commit the transaction to make is visible to user
 * 6. Commit the binlog consumption LSN(be atomic with transaction commitment to make)
 */
@Data
public class MVEpoch implements Writable {
    @SerializedName("mvId")
    private long mvId;
    @SerializedName("epochState")
    private EpochState state;
    @SerializedName("binlogState")
    private BinlogConsumeStateVO binlogState;
    @SerializedName("startTimeMilli")
    private long startTimeMilli;
    @SerializedName("commitTimeMilli")
    private long commitTimeMilli;

    // Ephemeral states
    private transient long txnId;

    public MVEpoch(long mvId) {
        this.mvId = mvId;
        this.startTimeMilli = System.currentTimeMillis();
        this.state = EpochState.INIT;
        this.binlogState = new BinlogConsumeStateVO();
    }

    public void onReady() {
        Preconditions.checkState(state.equals(EpochState.INIT));
        this.state = EpochState.READY;
    }

    public boolean onSchedule() {
        if (state.equals(EpochState.READY)) {
            this.state = EpochState.RUNNING;
            this.startTimeMilli = System.currentTimeMillis();
            return true;
        }
        return false;
    }

    public void onCommitting() {
        Preconditions.checkState(state.equals(EpochState.RUNNING));
        this.state = EpochState.COMMITTING;
    }

    public void onCommitted(BinlogConsumeStateVO binlogState) {
        Preconditions.checkState(state.equals(EpochState.COMMITTING));
        this.state = EpochState.COMMITTED;
        this.binlogState = binlogState;
        this.commitTimeMilli = System.currentTimeMillis();
    }

    public void onFailed() {
        this.state = EpochState.FAILED;
    }

    public void reset() {
        Preconditions.checkState(state.equals(EpochState.COMMITTED));
        this.state = EpochState.INIT;
    }

    public static MVEpoch read(DataInput input) throws IOException {
        return GsonUtils.GSON.fromJson(Text.readString(input), MVEpoch.class);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    /*
     * ┌────────┐      ┌────────┐     ┌───────────┐           ┌────────┐
     * │  INIT  ├──────┤ READY  ├────►│ RUNNING   ├──────────►│ FAILED │
     * └────────┘      └────────┘     └─────┬─────┘           └────────┘
     *                     ▲                │                     ▲
     *                     │                │                     │
     *                     │                │                     │
     *                     │          ┌─────▼─────┐               │
     *                     │          │ COMMITTING├───────────────┤
     *                     │          └─────┬─────┘               │
     *                     │                │                     │
     *                     │                │                     │
     *                     │                │                     │
     *                     │          ┌─────▼─────┐               │
     *                     └──────────┤ COMMITTED ├───────────────┘
     *                                └───────────┘
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
