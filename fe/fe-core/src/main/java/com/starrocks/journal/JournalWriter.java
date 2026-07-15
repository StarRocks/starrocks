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


package com.starrocks.journal;

import com.google.common.base.Preconditions;
import com.starrocks.common.Config;
import com.starrocks.common.util.Daemon;
import com.starrocks.common.util.Util;
import com.starrocks.metric.MetricRepo;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicReference;

/**
 * An independent thread to write journals by batch asynchronously.
 * Each thread that needs to write a log can put the log in a blocking queue, while JournalWriter constantly gets as
 * many logs as possible from the queue and write them all in one batch.
 * After committing, JournalWriter will notify the caller thread for consistency.
 */
public class JournalWriter {
    private enum WriterState {
        // Normal serving state. New append tasks are accepted and fatal journal failures
        // keep the historical process-exit behavior.
        RUNNING,
        // Demotion drain is in progress. Commit failures and interrupts are converted into graceful
        // task aborts (WRITER_ABORTED) instead of exiting the process, so in-flight writes unwind.
        SEALING,
        // The writer daemon has stopped; any straggler task taken from the queue is aborted.
        CLOSED
    }

    public static final Logger LOG = LogManager.getLogger(JournalWriter.class);
    // other threads can put log to this queue by calling Editlog.logEdit()
    private final BlockingQueue<JournalTask> journalQueue;
    private final Journal journal;

    // used for checking if edit log need to roll
    protected long rollJournalCounter = 0;
    // increment journal id
    // this is the persisted journal id
    protected long nextVisibleJournalId = -1;
    private volatile long lastCommittedJournalId = -1L;

    // belows are variables that will reset every batch
    // store journal tasks of this batch
    protected List<JournalTask> currentBatchTasks = new ArrayList<>();
    // current journal task
    private JournalTask currentJournal;
    // batch start time
    private long startTimeNano;
    // batch size in bytes
    private long uncommittedEstimatedBytes;

    /**
     * If this flag is set true, we will roll journal,
     * i.e. create a new database in BDB immediately after
     * current journal batch has been written.
     */
    private boolean forceRollJournal;

    /** Last timestamp in millisecond to log the commit triggered by delay. */
    private long lastLogTimeForDelayTriggeredCommit = -1;

    private long lastSlowEditLogTimeNs = -1L;
    private final AtomicReference<WriterState> writerState = new AtomicReference<>(WriterState.RUNNING);
    private volatile Daemon daemon;

    public JournalWriter(Journal journal, BlockingQueue<JournalTask> journalQueue) {
        this.journal = journal;
        this.journalQueue = journalQueue;
    }

    /**
     * reset journal id & roll journal as a start
     */
    public void init(long maxJournalId) throws JournalException {
        this.nextVisibleJournalId = maxJournalId + 1;
        this.lastCommittedJournalId = maxJournalId;
        this.writerState.set(WriterState.RUNNING);
        this.journal.rollJournal(this.nextVisibleJournalId);
    }

    public void startDaemon() {
        // ensure init() is called.
        assert (nextVisibleJournalId > 0);
        Daemon previous = daemon;
        if (previous != null && previous.isAlive()) {
            String msg = "previous JournalWriter daemon is still alive on restart, will exit now.";
            LOG.error(msg);
            Util.stdoutWithTime(msg);
            System.exit(-1);
        }
        Daemon d = new Daemon("JournalWriter", 0L) {
            @Override
            protected void runOneCycle() {
                try {
                    writeOneBatch();
                } catch (InterruptedException e) {
                    if (writerState.get() == WriterState.RUNNING) {
                        String msg = "journal writer interrupted while running, will exit now.";
                        LOG.error(msg, e);
                        Util.stdoutWithTime(msg);
                        System.exit(-1);
                    }
                    LOG.info("journal writer interrupted while in state {}, will stop", writerState.get());
                } catch (Throwable t) {
                    if (writerState.get() != WriterState.RUNNING) {
                        LOG.warn("journal writer hit error while in state {}, will stop without exit",
                                writerState.get(), t);
                        writerState.set(WriterState.CLOSED);
                    } else {
                        String msg = "got exception when trying to write one batch, will exit now.";
                        LOG.error(msg, t);
                        Util.stdoutWithTime(msg);
                        System.exit(-1);
                    }
                }
            }
        };
        daemon = d;
        d.start();
    }

    /**
     * Flip the writer out of RUNNING so a commit failure or interrupt during the demotion drain becomes a
     * graceful task abort instead of a process exit. Idempotent; a no-op once already sealing/closed.
     */
    public void beginSeal() {
        writerState.compareAndSet(WriterState.RUNNING, WriterState.SEALING);
    }

    /**
     * Stop the writer daemon and return the last committed journal id. Call only after the WAL admission
     * gate is closed and all in-flight leader writes have drained (EditLog.awaitWalDrained), which
     * guarantees the journal queue is empty here.
     */
    public long close(long timeoutMs) {
        // Flip out of RUNNING (if still running) so stopDaemon's interrupt is a graceful stop rather than the
        // RUNNING-state process exit in runOneCycle.
        writerState.compareAndSet(WriterState.RUNNING, WriterState.SEALING);
        stopDaemon(timeoutMs);
        writerState.set(WriterState.CLOSED);
        Preconditions.checkState(journalQueue.isEmpty(),
                "journal queue not empty after draining in-flight leader writes: remaining=%s", journalQueue.size());
        return lastCommittedJournalId;
    }

    private void stopDaemon(long timeoutMs) {
        Daemon d = daemon;
        if (d == null) {
            return;
        }
        d.setStop();
        d.interrupt();
        try {
            d.join(Math.max(1L, timeoutMs));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.warn("interrupted while waiting for journal writer daemon to stop", e);
        }
        if (d.isAlive()) {
            LOG.warn("journal writer daemon did not stop within {}ms; keeping reference so restart fails fast",
                    timeoutMs);
        } else {
            daemon = null;
        }
    }

    protected void writeOneBatch() throws InterruptedException {
        // waiting if necessary until an element becomes available
        currentJournal = journalQueue.take();

        if (writerState.get() == WriterState.CLOSED) {
            // Writer already stopped serving; abort the straggler so its waiter unwinds instead of hanging.
            abortJournalTask(currentJournal, "journal writer is closed");
            return;
        }

        long nextJournalId = nextVisibleJournalId;
        initBatch();

        try {
            this.journal.batchWriteBegin();

            while (true) {
                journal.batchWriteAppend(nextJournalId, currentJournal.getBuffer());
                currentBatchTasks.add(currentJournal);
                nextJournalId += 1;

                if (shouldCommitNow()) {
                    break;
                }

                currentJournal = journalQueue.take();
            }
        } catch (JournalException e) {
            // abort current task
            LOG.warn("failed to write batch, will abort current journal {} and commit", currentJournal, e);
            abortJournalTask(currentJournal, e.getMessage());
        } finally {
            try {
                // commit. The retry predicate only gates *retries*: the first commit attempt always runs, so
                // healthy batches still become durable while the writer is SEALING during a demotion drain.
                journal.batchWriteCommit(() -> writerState.get() == WriterState.RUNNING);
                LOG.debug("batch write commit success, from {} - {}", nextVisibleJournalId, nextJournalId);
                nextVisibleJournalId = nextJournalId;
                lastCommittedJournalId = nextJournalId - 1;
                markCurrentBatchSucceed();
            } catch (JournalException e) {
                // abort
                LOG.warn("failed to commit batch, will abort current {} journals.",
                        currentBatchTasks.size(), e);
                try {
                    journal.batchWriteAbort();
                } catch (JournalException e2) {
                    LOG.warn("failed to abort batch, will ignore and continue.", e);
                }
                if (writerState.get() == WriterState.RUNNING) {
                    // Commit failure while still serving is fatal (abortJournalTask exits the process).
                    abortCurrentBatch(e.getMessage());
                } else {
                    // Demotion drain (SEALING/CLOSED): unblock waiters with a terminal WRITER_ABORTED so they
                    // stop retrying and release the WAL fence, letting the drain converge instead of exiting.
                    abortCurrentBatch(e.getMessage(), new JournalWriteException(
                            JournalWriteException.Reason.WRITER_ABORTED, "journal commit failed while sealing", e));
                }
            }
        }

        if (writerState.get() != WriterState.RUNNING) {
            updateBatchMetrics();
            return;
        }

        rollJournalAfterBatch();

        updateBatchMetrics();
    }

    private void initBatch() {
        startTimeNano = System.nanoTime();
        uncommittedEstimatedBytes = 0;
        currentBatchTasks.clear();
    }

    private void markCurrentBatchSucceed() {
        for (JournalTask t : currentBatchTasks) {
            t.markSucceed();
        }
    }

    private void abortCurrentBatch(String errMsg) {
        for (JournalTask t : currentBatchTasks) {
            abortJournalTask(t, errMsg);
        }
    }

    private void abortCurrentBatch(String errMsg, Exception cause) {
        for (JournalTask t : currentBatchTasks) {
            abortJournalTask(t, errMsg, cause);
        }
    }

    /**
     * We should notify the caller to rollback or report error on abort, like this.
     * task.markAbort();
     * But now we have to exit for historical reason.
     * Note that if we exit here, the final clause(commit current batch) will not be executed.
     */
    protected void abortJournalTask(JournalTask task, String msg) {
        abortJournalTask(task, msg, null);
    }

    protected void abortJournalTask(JournalTask task, String msg, Exception cause) {
        if (writerState.get() != WriterState.RUNNING) {
            if (task != null) {
                task.markAbort(cause != null ? cause : new JournalWriteException(
                        JournalWriteException.Reason.WRITER_ABORTED, msg));
            }
            return;
        }
        LOG.error(msg);
        Util.stdoutWithTime(msg);
        System.exit(-1);
    }

    private boolean shouldCommitNow() {
        // 1. check if is an emergency journal
        if (currentJournal.getBetterCommitBeforeTimeInNano() > 0) {
            long delayNanos = System.nanoTime() - currentJournal.getBetterCommitBeforeTimeInNano();
            if (delayNanos >= 0) {
                long logTime = System.currentTimeMillis();
                // avoid logging too many messages if triggered frequently
                if (lastLogTimeForDelayTriggeredCommit + 500 < logTime) {
                    lastLogTimeForDelayTriggeredCommit = logTime;
                    LOG.warn("journal expect commit before {} is delayed {} nanos, will commit now",
                            currentJournal.getBetterCommitBeforeTimeInNano(), delayNanos);
                }
                return true;
            }
        }

        // 2. check uncommitted journal by count
        if (currentBatchTasks.size() >= Config.metadata_journal_max_batch_cnt) {
            LOG.warn("uncommitted journal {} >= {}, will commit now",
                    currentBatchTasks.size(), Config.metadata_journal_max_batch_cnt);
            return true;
        }

        // 3. check uncommitted journals by size
        uncommittedEstimatedBytes += currentJournal.estimatedSizeByte();
        if (uncommittedEstimatedBytes >= (long) Config.metadata_journal_max_batch_size_mb * 1024 * 1024) {
            LOG.warn("uncommitted estimated bytes {} >= {}MB, will commit now",
                    uncommittedEstimatedBytes, Config.metadata_journal_max_batch_size_mb);
            return true;
        }

        // 4. no more journal in queue
        return journalQueue.peek() == null;
    }

    /**
     * update all metrics after batch write
     */
    private void updateBatchMetrics() {
        // Log slow edit log write if needed.
        long currentTimeNs = System.nanoTime();
        long durationMs = (currentTimeNs - startTimeNano) / 1000000;
        final long DEFAULT_EDIT_LOG_SLOW_LOGGING_INTERVAL_NS = 2000000000L; // 2 seconds
        if (durationMs > Config.edit_log_write_slow_log_threshold_ms &&
                currentTimeNs - lastSlowEditLogTimeNs > DEFAULT_EDIT_LOG_SLOW_LOGGING_INTERVAL_NS) {
            LOG.warn("slow edit log write, batch size: {}, took: {}ms, current journal queue size: {}," +
                    " please check the IO pressure of FE LEADER node or the latency between LEADER and FOLLOWER nodes",
                    currentBatchTasks.size(), durationMs, journalQueue.size());
            lastSlowEditLogTimeNs = currentTimeNs;
        }
        if (MetricRepo.hasInit) {
            MetricRepo.COUNTER_EDIT_LOG_WRITE.increase((long) currentBatchTasks.size());
            MetricRepo.HISTO_JOURNAL_WRITE_LATENCY.update(durationMs);
            MetricRepo.HISTO_JOURNAL_WRITE_BATCH.update(currentBatchTasks.size());
            MetricRepo.HISTO_JOURNAL_WRITE_BYTES.update(uncommittedEstimatedBytes);
            MetricRepo.GAUGE_STACKED_JOURNAL_NUM.setValue((long) journalQueue.size());

            for (JournalTask e : currentBatchTasks) {
                MetricRepo.COUNTER_EDIT_LOG_SIZE_BYTES.increase(e.estimatedSizeByte());
            }
        }
        if (journalQueue.size() > Config.metadata_journal_max_batch_cnt) {
            LOG.warn("journal has piled up: {} in queue after consume", journalQueue.size());
        }
    }

    public void setForceRollJournal() {
        forceRollJournal = true;
    }

    public long getLastCommittedJournalId() {
        return lastCommittedJournalId;
    }

    boolean isDaemonAlive() {
        Daemon d = daemon;
        return d != null && d.isAlive();
    }

    private boolean needForceRollJournal() {
        if (forceRollJournal) {
            // Reset flag, alter system create image only trigger new image once
            forceRollJournal = false;
            return true;
        }

        return false;
    }

    private void rollJournalAfterBatch() {
        rollJournalCounter += currentBatchTasks.size();
        if (rollJournalCounter >= Config.edit_log_roll_num || needForceRollJournal()) {
            try {
                journal.rollJournal(nextVisibleJournalId);
            } catch (JournalException e) {
                String msg = String.format("failed to roll journal %d, will exit", nextVisibleJournalId);
                LOG.error(msg, e);
                Util.stdoutWithTime(msg);
                // TODO exit gracefully
                System.exit(-1);
            }
            String reason;
            if (rollJournalCounter >= Config.edit_log_roll_num) {
                reason = String.format("rollEditCounter %d >= edit_log_roll_num %d",
                        rollJournalCounter, Config.edit_log_roll_num);
            } else {
                reason = "triggering a new checkpoint manually";
            }
            LOG.info("edit log rolled because {}", reason);
            rollJournalCounter = 0;
        }
    }

}
