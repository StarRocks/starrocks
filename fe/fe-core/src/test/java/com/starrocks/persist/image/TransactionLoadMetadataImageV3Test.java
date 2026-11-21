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

import com.starrocks.catalog.BaseTableInfo;
import com.starrocks.catalog.ColumnId;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.common.Pair;
import com.starrocks.common.tvr.TvrTableDelta;
import com.starrocks.common.tvr.TvrTableSnapshot;
import com.starrocks.common.tvr.TvrVersion;
import com.starrocks.common.tvr.TvrVersionRange;
import com.starrocks.load.loadv2.IVMRefreshCommitInfo;
import com.starrocks.load.loadv2.InsertLoadJob;
import com.starrocks.load.loadv2.JobState;
import com.starrocks.load.loadv2.LoadJob;
import com.starrocks.load.loadv2.LoadJobScheduler;
import com.starrocks.load.loadv2.LoadMgr;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.persist.gson.SubtypeNotFoundException;
import com.starrocks.persist.proto.IVMRefreshCommitInfoPB;
import com.starrocks.persist.proto.PartitionCommitInfoPB;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LocalMetastore;
import com.starrocks.transaction.GlobalTransactionMgr;
import com.starrocks.transaction.InsertTxnCommitAttachment;
import com.starrocks.transaction.PartitionCommitInfo;
import com.starrocks.transaction.TableCommitInfo;
import com.starrocks.transaction.TransactionState;
import com.starrocks.transaction.TransactionStatus;
import com.starrocks.utframe.UtFrameUtils;
import com.starrocks.warehouse.cngroup.WarehouseComputeResource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.starrocks.common.jmockit.Deencapsulation.getField;
import static com.starrocks.common.jmockit.Deencapsulation.invoke;
import static com.starrocks.common.jmockit.Deencapsulation.setField;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TransactionLoadMetadataImageV3Test {
    @TempDir
    Path tempDir;
    private GlobalStateMgr global;
    private MaterializedView.MvRefreshScheme refreshScheme;

    @BeforeEach
    public void setUp() {
        global = LoadImageTestSupport.global();
        LocalMetastore metastore = Mockito.mock(LocalMetastore.class);
        Mockito.when(global.getLocalMetastore()).thenReturn(metastore);
        Database db = new Database(7, "ivm_db");
        Mockito.when(metastore.getDb(7L)).thenReturn(db);
        MaterializedView mv = Mockito.mock(MaterializedView.class);
        refreshScheme = new MaterializedView.MvRefreshScheme();
        Mockito.when(mv.getRefreshScheme()).thenReturn(refreshScheme);
        Mockito.when(metastore.getTable(7L, 501L)).thenReturn(mv);
    }

    private static Map<BaseTableInfo, TvrVersionRange> committedVersions() {
        Map<BaseTableInfo, TvrVersionRange> map = new LinkedHashMap<>();
        map.put(new BaseTableInfo(7, "ivm_db", "internal_base", 401), TvrTableSnapshot.of(123L));
        map.put(new BaseTableInfo("iceberg_catalog", "external_db", "external_base", "table-uuid"),
                TvrTableSnapshot.of(456L));
        return map;
    }

    private GlobalTransactionMgr transactions() {
        GlobalTransactionMgr mgr = new GlobalTransactionMgr(global);
        mgr.addDatabaseTransactionMgr(7L);
        Mockito.when(global.getGlobalTransactionMgr()).thenReturn(mgr);
        return mgr;
    }

    @Test
    public void testImageMetadataAndIvmJournalContinuation() throws Exception {
        Map<BaseTableInfo, TvrVersionRange> expectedVersions = committedVersions();
        TransactionState ivm = TransactionStateProtoTest.transaction(7, 601, TransactionStatus.PREPARED, -1, -1);
        setField(ivm, "tableIdList", List.of(501L));
        setField(ivm, "sourceType", TransactionState.LoadJobSourceType.INSERT_STREAMING);
        setField(ivm, "callbackId", 91L);
        setField(ivm, "callbackIdList", List.of(91L));
        ivm.setComputeResource(WarehouseComputeResource.of(17));
        InsertTxnCommitAttachment attachment = new InsertTxnCommitAttachment(700);
        attachment.setIvmRefreshCommitInfo(new IVMRefreshCommitInfo(expectedVersions));
        setField(ivm, "txnCommitAttachment", attachment);
        ProtoInstanceAssertions.assertInstances(ivm.toProto());

        TransactionState dictionary = TransactionStateProtoTest.transaction(7, 602, TransactionStatus.PREPARED, -1, -1);
        dictionary.setComputeResource(WarehouseComputeResource.of(18));
        PartitionCommitInfo partition = new PartitionCommitInfo(301, 400, 500,
                List.of(ColumnId.create("invalid")), List.of(ColumnId.create("first"), ColumnId.create("second")),
                List.of(31L, 32L));
        List<Long> identities = List.of(Long.MIN_VALUE + 1, 0x1234_5678_9abc_def0L);
        partition.setDictContentIdentities(identities);
        partition.setIsDoubleWrite(true);
        TableCommitInfo table = new TableCommitInfo(101);
        Map<Long, PartitionCommitInfo> partitions = getField(table, "idToPartitionCommitInfo");
        partitions.put(301L, partition);
        dictionary.putIdToTableCommitInfo(101, table);

        GlobalTransactionMgr sourceTransactions = transactions();
        for (TransactionState txn : List.of(ivm, dictionary)) {
            invoke(sourceTransactions.getDatabaseTransactionMgr(7), "unprotectUpsertTransactionState", txn);
        }
        InsertLoadJob insert = LoadJobProtoTest.insert(91, JobState.LOADING);
        InsertLoadJob statistics = LoadJobProtoTest.insert(92, JobState.LOADING);
        for (InsertLoadJob job : List.of(insert, statistics)) {
            setField(job, "dbId", 7L);
            setField(job, "tableId", 501L);
            setField(job, "warehouseId", 17L);
            setField(job, "computeResource", WarehouseComputeResource.of(17));
        }
        LoadMgr sourceLoads = LoadMgrImageV3Test.manager(List.of(insert, statistics));
        assertInstanceOf(LoadMgr.class, sourceLoads);

        UtFrameUtils.PseudoImage txnImage = new UtFrameUtils.PseudoImage();
        sourceTransactions.saveTransactionStateV2(txnImage.getImageWriter());
        UtFrameUtils.PseudoImage loadImage = new UtFrameUtils.PseudoImage();
        sourceLoads.saveLoadJobsV2JsonFormat(loadImage.getImageWriter());
        Path path = tempDir.resolve("transaction-load.image");
        try (ImageWriterV3 writer = new ImageWriterV3(Files.newOutputStream(path), 1)) {
            sourceTransactions.save(writer);
            sourceLoads.save(writer);
            writer.finish();
        }
        List<TransactionState> loadedIvm = new ArrayList<>();
        long committedTime = System.currentTimeMillis();
        for (boolean v3 : List.of(false, true)) {
            GlobalTransactionMgr restoredTransactions = transactions();
            LoadMgr restoredLoads = new LoadMgr(new LoadJobScheduler());
            if (v3) {
                List<Pair<SectionId, SectionLoader>> loaders = new ArrayList<>(restoredTransactions.getImageV3Loaders());
                loaders.addAll(restoredLoads.getImageV3Loaders());
                LoadImageTestSupport.load(path, loaders);
            } else {
                restoredTransactions.loadTransactionStateV2(txnImage.getMetaBlockReader());
                restoredLoads.loadLoadJobsV2JsonFormat(loadImage.getMetaBlockReader());
            }
            TransactionState restoredIvm = restoredTransactions.getTransactionState(7, 601);
            loadedIvm.add(restoredIvm);
            assertNotNull(restoredIvm);
            assertInstanceOf(WarehouseComputeResource.class, restoredIvm.getComputeResource());
            assertEquals(17, restoredIvm.getComputeResource().getWarehouseId());
            assertEquals(0, restoredIvm.getComputeResource().getWorkerGroupId());
            InsertTxnCommitAttachment restoredAttachment =
                    assertInstanceOf(InsertTxnCommitAttachment.class, restoredIvm.getTxnCommitAttachment());
            assertEquals(expectedVersions, restoredAttachment.getIvmRefreshCommitInfo().getCommittedTvrMap());
            GsonEquivalence.assertEquivalent(ivm, restoredIvm, Set.of(),
                    Set.of("^\\$\\.ta\\.ivmRefreshCommitInfo\\.committedTvrMap$"));
            TransactionState restoredDictionary = restoredTransactions.getTransactionState(7, 602);
            GsonEquivalence.assertEquivalent(dictionary, restoredDictionary);
            assertTrue(restoredDictionary.getTableCommitInfo(101).getIdToPartitionCommitInfo().get(301L).isDoubleWrite());
            assertEquals(identities, restoredDictionary.getTableCommitInfo(101).getIdToPartitionCommitInfo()
                    .get(301L).getDictContentIdentities());
            for (InsertLoadJob original : List.of(insert, statistics)) {
                LoadJob restored = restoredLoads.getLoadJob(original.getId());
                assertInstanceOf(InsertLoadJob.class, restored);
                GsonEquivalence.assertEquivalent(original, restored);
                assertEquals(original.isInternalJob(), restored.isInternalJob());
                assertInstanceOf(WarehouseComputeResource.class, getField(restored, "computeResource"));
                assertSame(restored, restoredTransactions.getCallbackFactory().getCallback(original.getId()));
            }

            // A later COMMITTED journal record must reach the restored load job and promote the recorded IVM bookmark.
            refreshScheme.getAsyncRefreshContext().getBaseTableInfoTvrVersionRangeMap().clear();
            TransactionState journal = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(restoredIvm), TransactionState.class);
            setField(journal, "transactionStatus", TransactionStatus.COMMITTED);
            journal.setCommitTime(committedTime);
            restoredTransactions.replayUpsertTransactionState(journal);
            assertEquals(expectedVersions, refreshScheme.getAsyncRefreshContext().getBaseTableInfoTvrVersionRangeMap());
            assertEquals(journal.getCommitTime(), (long) getField(restoredLoads.getLoadJob(91), "loadCommittedTimestamp"));
        }
        GsonEquivalence.assertEquivalent(loadedIvm.get(0), loadedIvm.get(1), Set.of(),
                Set.of("^\\$\\.ta\\.ivmRefreshCommitInfo\\.committedTvrMap$"));
        Mockito.verifyNoInteractions(global.getEditLog());
    }

    @Test
    public void testIvmMapPresenceAndAllRangeTypesMatchV2() throws Exception {
        Map<BaseTableInfo, TvrVersionRange> ranges = new LinkedHashMap<>(committedVersions());
        ranges.put(new BaseTableInfo(8, "other_db", "delta", 402), TvrTableDelta.of(TvrVersion.MIN, TvrVersion.MAX));
        ranges.put(new BaseTableInfo(8, "other_db", "empty_snapshot", 403), TvrTableSnapshot.empty());
        for (Map<BaseTableInfo, TvrVersionRange> map : Arrays.asList(null, Map.<BaseTableInfo, TvrVersionRange>of(), ranges)) {
            IVMRefreshCommitInfo source = new IVMRefreshCommitInfo(map);
            IVMRefreshCommitInfoPB pb = source.toProto();
            assertEquals(map != null, pb.hasCommittedTvrMap());
            IVMRefreshCommitInfo v3 = IVMRefreshCommitInfo.fromProto(IVMRefreshCommitInfoPB.parseFrom(pb.toByteArray()));
            IVMRefreshCommitInfo v2 = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(source), IVMRefreshCommitInfo.class);
            assertEquals(map, v2.getCommittedTvrMap());
            assertEquals(map, v3.getCommittedTvrMap());
            GsonEquivalence.assertEquivalent(v2, v3, Set.of(), Set.of("^\\$\\.committedTvrMap$"));
        }
    }

    @Test
    public void testAbsentDictionaryListsKeepV2Defaults() {
        PartitionCommitInfo v2 = GsonUtils.GSON.fromJson("{}", PartitionCommitInfo.class);
        PartitionCommitInfo v3 = PartitionCommitInfo.fromProto(PartitionCommitInfoPB.getDefaultInstance());
        assertFalse(v2.isDoubleWrite());
        assertFalse(v3.isDoubleWrite());
        GsonEquivalence.assertEquivalent(v2, v3);
        assertTrue(v3.getInvalidDictCacheColumns().isEmpty());
        assertTrue(v3.getValidDictCacheColumns().isEmpty());
        assertTrue(v3.getDictCollectedVersions().isEmpty());
        assertTrue(v3.getDictContentIdentities().isEmpty());
    }

    @Test
    public void testUnknownIvmRangePropagatesToTransactionBoundary() {
        IVMRefreshCommitInfoPB.Builder pb = new IVMRefreshCommitInfo(committedVersions()).toProto().toBuilder();
        pb.getCommittedTvrMapBuilder().getValuesBuilder(0).getRangeBuilder().setKindValue(99999);
        assertThrows(SubtypeNotFoundException.class, () -> IVMRefreshCommitInfo.fromProto(pb.build()));
    }
}
