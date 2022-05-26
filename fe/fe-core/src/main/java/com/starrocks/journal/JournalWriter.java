// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.journal;


import com.starrocks.common.Config;
import com.starrocks.common.util.Daemon;
import com.starrocks.metric.MetricRepo;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.mortbay.log.Log;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;

public class JournalWriter extends Daemon {
    public static final Logger LOG = LogManager.getLogger(JournalWriter.class);
    // other threads can put log to this queue by calling Editlog.logEdit()
    private BlockingQueue<JournalQueueEntity> logQueue;
    private Journal journal;

    // store stagging log before commit
    protected List<JournalQueueEntity> staggingLogs = new ArrayList<>();
    // used for checking if edit log need to roll
    protected long rollEditCounter = 0;

    public JournalWriter(Journal journal, BlockingQueue<JournalQueueEntity> logQueue) {
        super("JournalWriter", 0L);
        this.journal = journal;
        this.logQueue = logQueue;
    }

    @Override
    protected void runOneCycle() {
        try {
            // 1. waiting if necessary until an element becomes available
            JournalQueueEntity entity = logQueue.take();

            long start = System.currentTimeMillis();
            long uncommitedEstimatedBytes = 0;
            staggingLogs.clear();

            // 2. batch write
            while (true) {
                if (!staggingLogs.isEmpty()) {
                    entity = logQueue.take();
                }
                // 2.1 write(no commit)
                journal.writeWithinTxn(entity.getOp(), entity.getBuffer());
                staggingLogs.add(entity);

                // 2.2 check if is an emergency log
                if (entity.getBetterCommitBeforeTime() > 0) {
                    long delayMillis = System.currentTimeMillis() - entity.getBetterCommitBeforeTime();
                    if (delayMillis >= 0) {
                        LOG.info("log expect commit before {} is delayed {} mills, will commit now",
                                entity.getBetterCommitBeforeTime(), delayMillis);
                        break;
                    }
                }

                // 2.3 check uncommitted logs by count
                if (staggingLogs.size() >= Config.batch_journal_cnt) {
                    LOG.info("staggging log {} >= {}, will commit now",
                            staggingLogs.size(), Config.batch_journal_cnt);
                    break;
                }

                // 2.4 check uncommitted logs by size
                uncommitedEstimatedBytes += entity.estimatedSizeByte();
                if (uncommitedEstimatedBytes >= Config.batch_journal_size_mb * 1024 * 1024) {
                    LOG.info("uncommited estimated bytes {} >= {}MB, will commit now",
                            uncommitedEstimatedBytes, Config.batch_journal_size_mb);
                    break;
                }

                // 2.5 no more log in queue
                if (logQueue.peek() == null) {
                    break;
                }
            }

            // 3. commit
            journal.commitTxn();


            // 4. countdown
            for (JournalQueueEntity e : staggingLogs) {
                e.countDown();
            }

            // 5. update metrics
            if (MetricRepo.isInit) {
                MetricRepo.COUNTER_EDIT_LOG_WRITE.increase((long) staggingLogs.size());
                MetricRepo.HISTO_JOURNAL_WRITE_BATCH.update(staggingLogs.size());
                MetricRepo.HISTO_JOURNAL_WRITE_LATENCY.update((System.currentTimeMillis() - start));
                MetricRepo.GAUGE_STACKED_EDIT_LOG_NUM.setValue((long) logQueue.size());
            }
            if (logQueue.size() > Config.batch_journal_cnt) {
                Log.warn("log has piled up: {} in queue after consume", logQueue.size());
            }

            // 6. roll edit
            rollEditCounter += staggingLogs.size();
            if (rollEditCounter >= Config.edit_log_roll_num) {
                LOG.info("rollEditiCounter {} is equal to or larger than edit_log_roll_num {}, will roll edit.",
                        rollEditCounter, Config.edit_log_roll_num);
                journal.rollJournal();
                rollEditCounter = 0;
            }
        } catch (InterruptedException e) {
            // TODO we should exit gracefully on InterruptedException
            LOG.warn("got interruption exception, will continue to next loop, ", e);
        }
    }
}
