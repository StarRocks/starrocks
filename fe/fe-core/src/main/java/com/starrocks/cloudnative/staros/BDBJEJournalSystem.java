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


package com.starrocks.cloudnative.staros;

import com.staros.exception.ExceptionCode;
import com.staros.exception.StarException;
import com.staros.journal.Journal;
import com.staros.journal.JournalSystem;
import com.starrocks.common.conf.Config;
import com.starrocks.common.util.Daemon;
import com.starrocks.common.util.Utils;
import com.starrocks.journal.JournalCursor;
import com.starrocks.journal.JournalEntity;
import com.starrocks.journal.JournalException;
import com.starrocks.journal.JournalInconsistentException;
import com.starrocks.journal.JournalTask;
import com.starrocks.journal.JournalWriter;
import com.starrocks.journal.bdbje.BDBEnvironment;
import com.starrocks.journal.bdbje.BDBJEJournal;
import com.starrocks.persist.EditLog;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

// wrapper for star manager to use bdbje
public class BDBJEJournalSystem implements JournalSystem {
    private static final String JOURNAL_PREFIX = "starmgr_"; // do not change this string!
    private static final int REPLAY_INTERVAL_MS = 1;
    private static final Logger LOG = LogManager.getLogger(BDBJEJournalSystem.class);

    private BDBJEJournal bdbjeJournal;
    private JournalWriter journalWriter;
    private EditLog editLog;
    private AtomicLong replayedJournalId;
    private Daemon replayer; // TODO: maybe it's better to move this to StarMgr

    public BDBJEJournalSystem(BDBEnvironment environment) {
        BlockingQueue<JournalTask> journalQueue = new ArrayBlockingQueue<JournalTask>(Config.metadata_journal_queue_size);
        bdbjeJournal = new BDBJEJournal(environment, JOURNAL_PREFIX);

        journalWriter = new JournalWriter(bdbjeJournal, journalQueue);

        editLog = new EditLog(journalQueue);

        replayedJournalId = new AtomicLong(0L);

        replayer = null;
    }

    // for checkpoint thread only
    public BDBJEJournalSystem(BDBJEJournal journal) {
        bdbjeJournal = journal;
        replayedJournalId = new AtomicLong(0L);
    }

    public long getReplayId() {
        return replayedJournalId.get();
    }

    public void setReplayId(long replayId) {
        replayedJournalId.set(replayId);
    }

    @java.lang.SuppressWarnings("squid:S2142")  // allow catch InterruptedException
    public void onBecomeLeader() {
        // stop replayer
        if (replayer != null) {
            replayer.setStop();
            try {
                replayer.join();
            } catch (InterruptedException e) {
                LOG.warn("got exception when stopping the star mgr replayer thread, {}.", e);
            }
            replayer = null;
        }

        // replay all journal and start journal writer
        try {
            bdbjeJournal.open();

            long replayStartTime = System.currentTimeMillis();
            replayTo(JournalCursor.CURSOR_END_KEY);
            long replayEndTime = System.currentTimeMillis();
            LOG.info("finish star manager replay in " + (replayEndTime - replayStartTime) + " msec.");

            journalWriter.init(bdbjeJournal.getMaxJournalId());

            journalWriter.startDaemon();
        } catch (Exception e) {
            LOG.warn("star mgr prepare journal failed before becoming leader, {}.", e);
            Utils.stdoutWithTime(e.getMessage());
            System.exit(-1);
        }
    }

    @java.lang.SuppressWarnings("squid:S2142")  // allow catch InterruptedException
    public void onBecomeFollower() {
        if (replayer == null) { // replayer might already started
            replayer = new Daemon("star mgr replayer", REPLAY_INTERVAL_MS) {
                private JournalCursor cursor = null;

                @Override
                protected void runOneCycle() {
                    try {
                        if (cursor == null) {
                            LOG.info("star mgr start to replay from {}", replayedJournalId.get() + 1);
                            cursor = bdbjeJournal.read(replayedJournalId.get() + 1, -1);
                        } else {
                            cursor.refresh();
                        }
                        replayJournal(cursor);
                    } catch (JournalInconsistentException | InterruptedException e) {
                        LOG.warn("got interrupt exception or inconsistent exception when replay star mgr journal, {}.", e);
                        Utils.stdoutWithTime(e.getMessage());
                        System.exit(-1);
                    } catch (Throwable e) {
                        LOG.error("star mgr replayer thread catch an exception when replay journal, {}.", e);
                        try {
                            Thread.sleep(5000);
                        } catch (InterruptedException e1) {
                            LOG.error("star mgr replayer sleep got exception, {}.", e1);
                        }
                    }
                }

                // close current db after replayer finished
                @Override
                public void run() {
                    super.run();
                    if (cursor != null) {
                        cursor.close();
                        LOG.info("star mgr quit replay at {}.", replayedJournalId.get());
                    }
                }
            };

            replayer.start();
        }
    }

    public void write(Journal journal) throws StarException {
        try {
            editLog.logStarMgrOperation(new StarMgrJournal(journal));
        } catch (Exception e) {
            throw new StarException(ExceptionCode.JOURNAL, e.getMessage());
        }
    }

    @java.lang.SuppressWarnings("squid:S2142")  // allow catch InterruptedException
    public void replayTo(long journalId) throws StarException {
        JournalCursor cursor = null;
        try {
            cursor = bdbjeJournal.read(replayedJournalId.get() + 1, journalId);
            replayJournal(cursor);
        } catch (InterruptedException | JournalInconsistentException e) {
            LOG.warn("got interrupt exception or inconsistent exception when replay star mgr journal, {}.", e);
            Utils.stdoutWithTime(e.getMessage());
            System.exit(-1);
        } catch (Exception e) {
            throw new StarException(ExceptionCode.JOURNAL, e.getMessage());
        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }
    }

    private boolean replayJournal(JournalCursor cursor)
            throws JournalException, InterruptedException, JournalInconsistentException {
        long startReplayId = replayedJournalId.get();
        long startTime = System.currentTimeMillis();
        while (true) {
            JournalEntity entity = cursor.next();

            // EOF or aggressive retry
            if (entity == null) {
                break;
            }

            EditLog.loadJournal(null /* GlobalStateMgr */, entity);
            replayedJournalId.incrementAndGet();

            LOG.debug("star mgr journal {} replayed.", replayedJournalId);
        }
        long cost = System.currentTimeMillis() - startTime;
        if (cost >= 1000) {
            LOG.warn("star mgr replay journal cost too much time: {} replayedJournalId: {}.", cost, replayedJournalId);
        }

        if (replayedJournalId.get() - startReplayId > 0) {
            LOG.info("star mgr replayed journal from {} to {}.", startReplayId, replayedJournalId);
            return true;
        }
        return false;
    }

    public BDBJEJournal getJournal() {
        return bdbjeJournal;
    }
}

