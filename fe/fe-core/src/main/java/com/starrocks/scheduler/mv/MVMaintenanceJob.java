// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.scheduler.mv;

import com.google.common.base.Preconditions;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.MvId;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.DuplicatedRequestException;
import com.starrocks.common.LabelAlreadyUsedException;
import com.starrocks.common.UserException;
import com.starrocks.common.io.Writable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.common.UnsupportedException;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.thrift.TUniqueId;
import com.starrocks.transaction.BeginTransactionException;
import com.starrocks.transaction.TabletCommitInfo;
import com.starrocks.transaction.TransactionState;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Long-running job responsible for MV incremental maintenance
 */
public class MVMaintenanceJob implements Writable {
    private static final Logger LOG = LogManager.getLogger(MVMaintenanceJob.class);

    private final MaterializedView view;
    private ExecPlan plan;
    private final AtomicReference<JobState> state = new AtomicReference<>();


    public MVMaintenanceJob(MaterializedView view) {
        this.view = view;
        this.state.set(JobState.INIT);
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
        if (state.get().equals(JobState.WAIT_EPOCH)) {
            runEpoch();
        } else if (state.get().equals(JobState.RUN_EPOCH)) {
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
        this.state.set(JobState.RUN_EPOCH);
        throw UnsupportedException.unsupportedException("TODO: implement ");
    }

    private void runEpoch() {
        try {
            TxnBasedEpochExecutor epochCoordinator = new TxnBasedEpochExecutor(this);
            epochCoordinator.run();
            this.state.set(JobState.WAIT_EPOCH);
            LOG.debug("[MVJob] finish execution of job epoch: " + this);
        } catch (Exception e) {
            LOG.warn("job {} run epoch failed: {}", this, e);
            throw e;
        }
    }

    public JobState getState() {
        return this.state.get();
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

    /**
     * TODO(murphy) abstract it to support other kinds of EpochExecutor
     */
    private static class TxnBasedEpochExecutor {
        // TODO(murphy) make it configurable
        private static final long JOB_TIMEOUT = 120;

        private final MVMaintenanceJob job;
        private final Epoch epoch;

        public TxnBasedEpochExecutor(MVMaintenanceJob job) {
            this.job = job;
            this.epoch = new Epoch();
        }

        public void run() {
            beginEpoch();
            commitEpoch();
        }

        private void beginEpoch() {
            MvId mvId = job.getView().getMvId();
            long dbId = job.getView().getDbId();
            List<Long> tableIdList = new ArrayList<>(job.getView().getBaseTableIds());
            String label = "mv_refresh_" + mvId;
            TUniqueId requestId = new TUniqueId();
            TransactionState.TxnCoordinator txnCoordinator = TransactionState.TxnCoordinator.fromThisFE();
            TransactionState.LoadJobSourceType loadSource = TransactionState.LoadJobSourceType.BATCH_LOAD_JOB;

            try {
                long txnId = GlobalStateMgr.getCurrentGlobalTransactionMgr()
                        .beginTransaction(dbId, tableIdList, label, txnCoordinator, loadSource, JOB_TIMEOUT);
                this.epoch.transactionId = txnId;
            } catch (AnalysisException e) {
                throw new RuntimeException(e);
            } catch (LabelAlreadyUsedException e) {
                throw new RuntimeException(e);
            } catch (BeginTransactionException e) {
                throw new RuntimeException(e);
            } catch (DuplicatedRequestException e) {
                throw new RuntimeException(e);
            }
        }


        private void commitEpoch() {
            long dbId = job.getView().getDbId();
            // TODO(murphy) implement
            List<TabletCommitInfo> commitInfo = new ArrayList<>();

            try {
                GlobalStateMgr.getCurrentGlobalTransactionMgr().commitTransaction(dbId, this.epoch.transactionId, commitInfo);
            } catch (UserException e) {
                // TODO(murphy) handle error
            }

        }
    }

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

}
