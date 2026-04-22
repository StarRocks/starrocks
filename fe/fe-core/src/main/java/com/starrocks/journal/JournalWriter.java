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

import com.starrocks.common.Config;
import com.starrocks.common.util.Daemon;
import com.starrocks.common.util.Util;
import com.starrocks.metric.MetricRepo;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
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
        // Demotion drain is in progress. New append tasks should be rejected/aborted and
        // commit failures are converted into drain results instead of exiting the process.
        SEALING,
        // The writer has finished sealing and will not process any more append tasks.
        CLOSED
    }

    public static class DrainResult {
        public enum Status {
            // The barrier was observed after all earlier committed appends became durable.
            BARRIER_REACHED,
            // The writer lost the ability to commit while sealing, so the returned watermark
            // is only the last successfully committed journal id.
            LEADER_LOST,
            // The caller stopped waiting before the barrier completed. The returned watermark
            // is the latest committed journal id visible at timeout.
            TIMEOUT
        }

        private final Status status;
        private final long lastCommittedJournalId;

        public DrainResult(Status status, long lastCommittedJournalId) {
            this.status = status;
            this.lastCommittedJournalId = lastCommittedJournalId;
        }

        public Status getStatus() {
            return status;
        }

        public long getLastCommittedJournalId() {
            return lastCommittedJournalId;
        }
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
    private volatile DrainResult latestDrainResult;
    private volatile JournalTask activeDrainBarrierTask;

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
        this.latestDrainResult = null;
        this.activeDrainBarrierTask = null;
        this.writerState.set(WriterState.RUNNING);
        this.journal.rollJournal(this.nextVisibleJournalId);
    }

    public void startDaemon() {
        // ensure init() is called.
        assert (nextVisibleJournalId > 0);
        Daemon d = new Daemon("JournalWriter", 0L) {
            @Override
            protected void runOneCycle() {
                try {
                    writeOneBatch();
                } catch (Throwable t) {
                    String msg = "got exception when trying to write one batch, will exit now.";
                    LOG.error(msg, t);
                    // TODO we should exit gracefully on InterruptedException
                    Util.stdoutWithTime(msg);
                    System.exit(-1);
                }
            }
        };
        d.start();
    }

    protected void writeOneBatch() throws InterruptedException {
        // waiting if necessary until an element becomes available
        currentJournal = journalQueue.take();

        if (writerState.get() == WriterState.CLOSED) {
            drainClosedTasks(currentJournal);
            return;
        }

        if (currentJournal.isBarrierTask()) {
            completeBarrier(currentJournal, DrainResult.Status.BARRIER_REACHED, lastCommittedJournalId, null);
            writerState.set(WriterState.CLOSED);
            drainClosedTasks(journalQueue.poll());
            return;
        }

        long nextJournalId = nextVisibleJournalId;
        JournalTask barrierTask = null;
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
                if (currentJournal.isBarrierTask()) {
                    barrierTask = currentJournal;
                    break;
                }
            }
        } catch (JournalException e) {
            // abort current task
            LOG.warn("failed to write batch, will abort current journal {} and commit", currentJournal, e);
            abortJournalTask(currentJournal, e.getMessage());
        } finally {
            try {
                // commit
                journal.batchWriteCommit();
                LOG.debug("batch write commit success, from {} - {}", nextVisibleJournalId, nextJournalId);
                nextVisibleJournalId = nextJournalId;
                lastCommittedJournalId = nextJournalId - 1;
                markCurrentBatchSucceed();
                if (barrierTask != null) {
                    completeBarrier(barrierTask, DrainResult.Status.BARRIER_REACHED, lastCommittedJournalId, null);
                    writerState.set(WriterState.CLOSED);
                }
            } catch (JournalException e) {
                // abort
                LOG.warn("failed to commit batch, will abort current {} journals.",
                        currentBatchTasks.size(), e);
                try {
                    journal.batchWriteAbort();
                } catch (JournalException e2) {
                    LOG.warn("failed to abort batch, will ignore and continue.", e);
                }
                if (writerState.get() == WriterState.SEALING) {
                    JournalWriteException abortException = new JournalWriteException(
                            JournalWriteException.Reason.WRITER_ABORTED,
                            "journal commit failed while sealing", e);
                    abortCurrentBatch(e.getMessage(), abortException);
                    latestDrainResult = new DrainResult(DrainResult.Status.LEADER_LOST, lastCommittedJournalId);
                    if (barrierTask != null) {
                        completeBarrier(barrierTask, DrainResult.Status.LEADER_LOST, lastCommittedJournalId,
                                abortException);
                    }
                    writerState.set(WriterState.CLOSED);
                } else {
                    abortCurrentBatch(e.getMessage());
                }
            }
        }

        if (writerState.get() == WriterState.CLOSED) {
            drainClosedTasks(journalQueue.poll());
            return;
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

    public synchronized DrainResult sealAndGetCommittedWatermark(long timeoutMs) throws InterruptedException {
        if (writerState.get() == WriterState.CLOSED) {
            return latestDrainResult != null
                    ? latestDrainResult
                    : new DrainResult(DrainResult.Status.BARRIER_REACHED, lastCommittedJournalId);
        }

        if (activeDrainBarrierTask == null) {
            writerState.compareAndSet(WriterState.RUNNING, WriterState.SEALING);
            activeDrainBarrierTask = JournalTask.createBarrierTask();
            journalQueue.put(activeDrainBarrierTask);
        }

        return waitForDrainResult(activeDrainBarrierTask, timeoutMs);
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

    private DrainResult waitForDrainResult(JournalTask barrierTask, long timeoutMs) throws InterruptedException {
        try {
            if (timeoutMs < 0) {
                barrierTask.get();
            } else {
                barrierTask.get(timeoutMs, TimeUnit.MILLISECONDS);
            }
            return buildDrainResult(barrierTask, DrainResult.Status.BARRIER_REACHED);
        } catch (ExecutionException e) {
            return buildDrainResult(barrierTask, DrainResult.Status.LEADER_LOST);
        } catch (TimeoutException e) {
            return new DrainResult(DrainResult.Status.TIMEOUT, lastCommittedJournalId);
        }
    }

    private DrainResult buildDrainResult(JournalTask barrierTask, DrainResult.Status fallbackStatus) {
        DrainResult.Status status = barrierTask.getDrainStatus() == null ? fallbackStatus : barrierTask.getDrainStatus();
        DrainResult result = new DrainResult(status,
                barrierTask.getCommittedJournalId() >= 0 ? barrierTask.getCommittedJournalId() : lastCommittedJournalId);
        if (status != DrainResult.Status.TIMEOUT) {
            latestDrainResult = result;
        }
        return result;
    }

    private void completeBarrier(JournalTask barrierTask, DrainResult.Status status,
                                 long committedJournalId, Exception cause) {
        if (barrierTask == null) {
            return;
        }
        if (status == DrainResult.Status.BARRIER_REACHED) {
            barrierTask.markBarrierReached(committedJournalId);
        } else {
            barrierTask.markBarrierFailed(status, committedJournalId, cause);
        }
        activeDrainBarrierTask = null;
        latestDrainResult = new DrainResult(status, committedJournalId);
    }

    private void drainClosedTasks(JournalTask firstTask) {
        JournalTask task = firstTask;
        while (task != null) {
            if (task.isBarrierTask()) {
                DrainResult result = latestDrainResult != null
                        ? latestDrainResult
                        : new DrainResult(DrainResult.Status.BARRIER_REACHED, lastCommittedJournalId);
                Exception cause = null;
                if (result.getStatus() == DrainResult.Status.LEADER_LOST) {
                    cause = new JournalWriteException(JournalWriteException.Reason.WRITER_ABORTED,
                            "journal writer already closed after sealing");
                }
                completeBarrier(task, result.getStatus(), result.getLastCommittedJournalId(), cause);
            } else {
                abortJournalTask(task, "journal writer is closed after sealing",
                        new JournalWriteException(JournalWriteException.Reason.WRITER_ABORTED,
                                "journal writer is closed after sealing"));
            }
            task = journalQueue.poll();
        }
    }
}
