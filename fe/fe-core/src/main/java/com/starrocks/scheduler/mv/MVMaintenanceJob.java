// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.scheduler.mv;

import com.google.common.base.Preconditions;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.common.io.Writable;
import com.starrocks.sql.common.UnsupportedException;
import com.starrocks.sql.plan.ExecPlan;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataOutput;
import java.io.IOException;

/**
 * Long-running job responsible for MV incremental maintenance
 */
public class MVMaintenanceJob implements Writable {
    private static final Logger LOG = LogManager.getLogger(MVMaintenanceJob.class);

    private final MaterializedView view;
    private ExecPlan plan;
    private final JobState state;

    /**
     * The incremental maintenance of MV consists of epochs, whose lifetime is defined as:
     * 1. Triggered by transaction publish
     * 2. Acquire the last-committed binlog LSN and latest binlog LSN
     * 3. Start a transaction for incremental maintaining the MV
     * 4. Schedule task executor to consume binlog since last-committed, and apply these changes to MV
     * 5. Commit the transaction to make is visible to user
     * 6. Commit the binlog consumption LSN(be atomic with transaction commitment to make)
     */
    public static class Epoch {
        public long transactionId;
        public long startTimeMilli;

        public Epoch() {

        }
    }

    public enum JobState {
        // Just initialized
        INIT,

        // Preparing for the job
        PREPARING,

        // Pause the job, waiting for the continue event
        PAUSED,

        // Wait for epoch start
        WAIT_EPOCH,

        // Running the epoch
        RUN_EPOCH,

        // Failed the whole job, needs reconstruction (unsupported environment change would cause job failure
        FAILED;

        public boolean isRunnable() {
            return this == INIT || this == PREPARING || this == WAIT_EPOCH;
        }
    }

    public enum EpochState {
        INIT,
        RUNNING,
        COMMITTING,
        COMMITTED,
        FINISHED,
        FAILED;

        public EpochState nextStateOnSuccess() {
            switch (this) {
                case INIT:
                    return RUNNING;
                case RUNNING:
                    return COMMITTING;
                case COMMITTING:
                    return COMMITTED;
                case COMMITTED:
                    return FINISHED;
                case FINISHED:
                    return FINISHED;
                case FAILED:
                    return FAILED;
                default:
                    Preconditions.checkState(false, "illegal state " + this);
            }
            return null;
        }

        public boolean isFinished() {
            return this.equals(FINISHED);
        }
    }


    public MVMaintenanceJob(MaterializedView view) {
        this.view = view;
        this.state = JobState.INIT;
    }

    /**
     * Main entrance of the job:
     * 0. Generate the physical job structure, including fragment distribution, parallelism
     * 1. Deliver tasks to executors on BE
     * 2. Trigger the epoch
     */
    public void start() {
    }

    public void stop() {
    }

    /**
     * Destroy the job and correspond state
     */
    public void destroy() {
        throw UnsupportedException.unsupportedException("TODO: implement destroy action");
    }

    public void pauseJob() {
        throw UnsupportedException.unsupportedException("TODO: implement pause action");
    }

    public void continueJob() {
        throw UnsupportedException.unsupportedException("TODO: implement continue action");
    }

    public void runDaemon() {
        throw UnsupportedException.unsupportedException("TODO: implement the daemon runner");
    }

    public void onSchedule() {
        if (state.equals(JobState.WAIT_EPOCH)) {
            runEpoch();
        } else if (state.equals(JobState.RUN_EPOCH)) {
            // TODO(murphy) make sure it would not lose any update
            throw UnsupportedException.unsupportedException("TODO: job is running, don't push me");
        } else {
            throw UnsupportedException.unsupportedException("TODO: implement ");
        }
    }

    /**
     * Trigger the incremental maintenance by transaction publish
     */
    public void triggerByTxn() {
        throw UnsupportedException.unsupportedException("TODO: implement ");
    }

    /**
     * TODO(murphy) abstract it to support other kinds of EpochExecutor
     */
    private static class TxnBasedEpochExecutor {
        private Epoch epoch;

        public TxnBasedEpochExecutor() {
        }

        public void run() {
            beginEpoch();
            commitEpoch();
        }

        private void beginEpoch() {
            throw UnsupportedException.unsupportedException("TODO: implement");
        }


        private void commitEpoch() {
            throw UnsupportedException.unsupportedException("TODO: implement");
        }
    }

    private void runEpoch() {
        TxnBasedEpochExecutor epochCoordinator = new TxnBasedEpochExecutor();
        try {
            epochCoordinator.run();
        } catch (Exception e) {
            LOG.warn("job {} run epoch failed: {}", this, e);
            throw e;
        }
    }

    public JobState getState() {
        return this.state;
    }

    public MaterializedView getView() {
        return view;
    }

    @Override
    public String toString() {
        return String.format("MVJob of %s/%s", view.getName(), view.getId());
    }

    @Override
    public void write(DataOutput out) throws IOException {
        throw UnsupportedException.unsupportedException("TODO");
    }
}
