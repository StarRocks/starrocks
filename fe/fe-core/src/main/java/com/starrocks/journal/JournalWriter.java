// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

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

/**
 * An independent thread to write journals by batch asynchronously.
 * Each thread that needs to write a log can put the log in a blocking queue, while JournalWriter constantly gets as
 * many logs as possible from the queue and write them all in one batch.
 * After committing, JournalWriter will notify the caller thread for consistency.
 */
public class JournalWriter {
    public static final Logger LOG = LogManager.getLogger(JournalWriter.class);
    // other threads can put log to this queue by calling Editlog.logEdit()
    private final BlockingQueue<JournalTask> journalQueue;
    private final Journal journal;

    // used for checking if edit log need to roll
    protected long rollJournalCounter = 0;
    // increment journal id
    // this is the persist journal id
    protected long nextVisibleJournalId = -1;

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

    public JournalWriter(Journal journal, BlockingQueue<JournalTask> journalQueue) {
        this.journal = journal;
        this.journalQueue = journalQueue;
    }

    /**
     * reset journal id & roll journal as a start
     */
    public void init(long maxJournalId) throws JournalException {
        this.nextVisibleJournalId = maxJournalId + 1;
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
                } catch (InterruptedException e) {
                    String msg = "got interrupted exception when trying to write one batch, will exit now.";
                    LOG.error(msg, e);
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
                // commit
                journal.batchWriteCommit();
                LOG.debug("batch write commit success, from {} - {}", nextVisibleJournalId, nextJournalId);
                nextVisibleJournalId = nextJournalId;
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
                abortCurrentBatch(e.getMessage());
            }
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

    /**
     * We should notify the caller to rollback or report error on abort, like this.
     * task.markAbort();
     * But now we have to exit for historical reason.
     * Note that if we exit here, the finally clause(commit current batch) will not be executed.
     */
    protected void abortJournalTask(JournalTask task, String msg) {
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
                reason = String.format("rollEditCounter {} >= edit_log_roll_num {}",
                        rollJournalCounter, Config.edit_log_roll_num);
            } else {
                reason = "triggering a new checkpoint manually";
            }
            LOG.info("edit log rolled because {}", reason);
            rollJournalCounter = 0;
        }
    }
}
