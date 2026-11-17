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

package com.starrocks.persist.image;

import com.starrocks.backup.AbstractJob;
import com.starrocks.backup.BackupHandler;
import com.starrocks.catalog.CatalogRecycleBin;
import com.starrocks.catalog.ColocateTableIndex;
import com.starrocks.catalog.Database;
import com.starrocks.common.Pair;
import com.starrocks.connector.elasticsearch.EsRepository;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LocalMetastore;
import com.starrocks.transaction.GlobalTransactionMgr;
import com.starrocks.transaction.TransactionState;
import com.starrocks.transaction.TransactionStatus;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.starrocks.common.jmockit.Deencapsulation.getField;
import static com.starrocks.common.jmockit.Deencapsulation.invoke;
import static com.starrocks.persist.image.GlobalTransactionMgrImageV3Test.finalIds;
import static com.starrocks.persist.image.TransactionStateProtoTest.transaction;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Verifies the dependency on DB managers registered by the metastore and recycle-bin sections. */
public class BackupTransactionImageV3Test {
    @TempDir
    Path tempDir;

    @Test
    public void testLoadSectionsForLiveAndRecycledDatabases() throws Exception {
        GlobalStateMgr global = Mockito.mock(GlobalStateMgr.class);
        Mockito.when(global.getEsRepository()).thenReturn(Mockito.mock(EsRepository.class));
        new MockUp<GlobalStateMgr>() {
            @Mock
            public GlobalStateMgr getCurrentState() {
                return global;
            }

            @Mock
            public boolean isCheckpointThread() {
                return true;
            }
        };
        GlobalTransactionMgr liveTxns = new GlobalTransactionMgr(global);
        Mockito.when(global.getGlobalTransactionMgr()).thenReturn(liveTxns);
        CatalogRecycleBin liveRecycle = new CatalogRecycleBin();
        LocalMetastore liveCatalog = new LocalMetastore(global, liveRecycle, new ColocateTableIndex());
        invoke(liveCatalog, "registerDatabase", new Database(100001, "active_database"));
        Database recycledDb = new Database(100002, "recycled_database");
        liveRecycle.recycleDatabase(recycledDb, Set.of(), true);
        liveTxns.addDatabaseTransactionMgr(100002L);
        long now = System.currentTimeMillis();
        List<TransactionState> transactions = List.of(
                transaction(100001, 1, TransactionStatus.PREPARED, -1, -1),
                transaction(100002, 2, TransactionStatus.ABORTED, -1, now - 10),
                transaction(100002, 3, TransactionStatus.VISIBLE, now - 100, now - 30));
        for (TransactionState state : transactions) {
            invoke(liveTxns.getDatabaseTransactionMgr(state.getDbId()), "unprotectUpsertTransactionState", state);
        }
        BackupHandler liveBackup = new BackupHandler();
        Map<Long, AbstractJob> jobs = getField(liveBackup, "dbIdToBackupOrRestoreJob");
        BackupJobProtoTest.jobs(now).forEach(job -> jobs.put(job.getDbId(), job));
        Path file = tempDir.resolve("image");
        try (OutputStream output = Files.newOutputStream(file);
                ImageWriterV3 writer = new ImageWriterV3(output, 99)) {
            liveCatalog.save(writer);
            liveRecycle.save(writer);
            liveBackup.save(writer);
            liveTxns.save(writer);
            writer.finish();
        }
        GlobalTransactionMgr loadedTxns = new GlobalTransactionMgr(global);
        Mockito.when(global.getGlobalTransactionMgr()).thenReturn(loadedTxns);
        CatalogRecycleBin loadedRecycle = new CatalogRecycleBin();
        LocalMetastore loadedCatalog = new LocalMetastore(global, loadedRecycle, new ColocateTableIndex());
        BackupHandler loadedBackup = new BackupHandler();
        List<Pair<SectionId, SectionLoader>> loaders = new ArrayList<>(loadedCatalog.getImageV3Loaders());
        loaders.addAll(loadedRecycle.getImageV3Loaders());
        loaders.addAll(loadedBackup.getImageV3Loaders());
        loaders.addAll(loadedTxns.getImageV3Loaders());
        try (ImageReaderV3 reader = new ImageReaderV3(file)) {
            assertEquals(9, reader.sections().size());
            reader.load(loaders);
        }
        assertNotNull(loadedCatalog.getDb(100001));
        assertTrue(loadedRecycle.getAllDbIds().contains(100002L));
        assertNotNull(loadedTxns.getDatabaseTransactionMgr(100002));
        assertEquals(List.of(2L, 3L), finalIds(loadedTxns.getDatabaseTransactionMgr(100002)));
        for (TransactionState state : transactions) {
            GsonEquivalence.assertEquivalent(state,
                    loadedTxns.getTransactionState(state.getDbId(), state.getTransactionId()));
        }
        for (AbstractJob job : jobs.values()) {
            BackupJobProtoTest.assertJobEquivalent(job, loadedBackup.getJob(job.getDbId()));
        }

    }
}
