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
package com.starrocks.meta;

import com.starrocks.common.io.Writable;
import com.starrocks.journal.Journal;
import com.starrocks.journal.JournalCursor;
import com.starrocks.journal.JournalEntity;
import com.starrocks.journal.JournalException;
import com.starrocks.journal.JournalInconsistentException;
import com.starrocks.meta.store.bdb.BDBMetaStore;
import com.starrocks.persist.CreateDbInfo;
import com.starrocks.persist.CreateTableInfo;
import com.starrocks.persist.DropDbInfo;
import com.starrocks.persist.OperationType;
import com.starrocks.server.GlobalStateMgr;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class EditLogReplayer {
    public static final Logger LOG = LogManager.getLogger(EditLogReplayer.class);

    public void loadJournal(JournalEntity journalEntity, long replayedJournalId)
            throws JournalInconsistentException {
        short opCode = journalEntity.getOpCode();
        Writable writable = journalEntity.getData();

        if (!(writable instanceof TxnMeta)) {
            LOG.info("ignore replay edit log " + opCode);
            return;
        }

        TxnMeta txnMeta = (TxnMeta) journalEntity.getData();
        txnMeta.setReplay(true);
        txnMeta.setReplayedJournalId(replayedJournalId);
        BDBMetaStore bdbMetaStore = new BDBMetaStore();

        try {
            switch (opCode) {
                case OperationType.OP_CREATE_DB_V2: {
                    CreateDbInfo createDbInfo = (CreateDbInfo) journalEntity.getData();
                    bdbMetaStore.createDb(createDbInfo);
                    break;
                }

                case OperationType.OP_DROP_DB: {
                    DropDbInfo dropDbInfo = (DropDbInfo) journalEntity.getData();
                    bdbMetaStore.dropDb(dropDbInfo);
                    break;
                }

                case OperationType.OP_CREATE_TABLE_V2: {
                    CreateTableInfo createTableInfo = (CreateTableInfo) journalEntity.getData();
                    bdbMetaStore.createTable(createTableInfo);
                    break;
                }

                default:
                    LOG.info("ignore replay edit log " + opCode);
            }
        } catch (Exception e) {
            JournalInconsistentException exception =
                    new JournalInconsistentException(opCode, "failed to load journal type " + opCode);
            exception.initCause(e);
            throw exception;
        }
    }

    public boolean load(long maxJournalId) {
        BDBMetaStore bdbMetaStore = new BDBMetaStore();
        //因为cursor左闭右闭区间，记录的id是已成功的id，所以这里需要+1
        long replayedJournalId = bdbMetaStore.getEditLogReplayId() + 1;
        if (replayedJournalId >= maxJournalId) {
            return true;
        }

        JournalCursor cursor = null;
        try {
            LOG.info("Begin reload EditLog from " + replayedJournalId + " to " + maxJournalId);
            Journal journal = GlobalStateMgr.getCurrentState().getJournal();
            cursor = journal.read(replayedJournalId, maxJournalId);

            for (long rid = replayedJournalId; rid <= maxJournalId; ++rid) {
                JournalEntity journalEntity = cursor.next();
                loadJournal(journalEntity, rid);
            }
        } catch (JournalException | JournalInconsistentException | InterruptedException e) {
            LOG.warn("Fail to reload edit log", e);
            return false;
        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }

        LOG.info("Success reload edit log from " + replayedJournalId + " to " + maxJournalId);
        return true;
    }
}
