// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.lake;

import com.google.common.collect.Lists;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Tablet;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.DuplicatedRequestException;
import com.starrocks.common.LabelAlreadyUsedException;
import com.starrocks.common.UserException;
import com.starrocks.common.util.MasterDaemon;
import com.starrocks.lake.proto.CompactRequest;
import com.starrocks.lake.proto.CompactResponse;
import com.starrocks.rpc.LakeServiceClient;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.service.FrontendOptions;
import com.starrocks.system.Backend;
import com.starrocks.system.SystemInfoService;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.transaction.BeginTransactionException;
import com.starrocks.transaction.TabletCommitInfo;
import com.starrocks.transaction.TransactionState;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class CompactionDispatchDaemon extends MasterDaemon {
    private static final long TXN_TIMEOUT_SECOND = 1800;
    private static final Logger LOG = LogManager.getLogger(CompactionDispatchDaemon.class);

    private static final String HOST_NAME = FrontendOptions.getLocalHostAddress();

    public CompactionDispatchDaemon() {
        super("COMPACTION_DISPATCH", 10/*TODO: configurable*/);
    }

    @Override
    protected void runAfterCatalogReady() {
        CompactionManager compactionManager = GlobalStateMgr.getCurrentState().getCompactionManager();
        while (true) {
            CompactionContext compactionContext = compactionManager.getCompactionContext();
            if (compactionContext == null) {
                break;
            }

            PartitionIdentifier partitionIdentifier = compactionContext.getPartition();
            Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(partitionIdentifier.getDbId());
            if (db == null) {
                compactionManager.removeCompactionContext(compactionContext);
                continue;
            }

            if (!db.tryReadLock(50, TimeUnit.MILLISECONDS)) {
                continue;
            }

            LakeTable table;
            Partition partition;
            try {
                table = (LakeTable) db.getTable(partitionIdentifier.getTableId());
                if (table == null) {
                    compactionManager.removeCompactionContext(compactionContext);
                    continue;
                }

                partition = table.getPartition(partitionIdentifier.getPartitionId());
                if (partition == null) {
                    compactionManager.removeCompactionContext(compactionContext);
                    continue;
                }
            } finally {
                db.readUnlock();
            }

            long updateCount = compactionContext.getUpdateCount();
            try {
                boolean success = compactPartition(db, table, partition);
                if (success) {
                    long newValue = compactionContext.subUpdateCount(updateCount);
                }
            } finally {
                compactionManager.returnCompactionContext(compactionContext);
            }
        }
    }

    private boolean compactPartition(Database db, LakeTable table, Partition partition) {
        long currentTs = System.currentTimeMillis();
        String label = String.format("COMPACTION_%d-%d-%d-%d", db.getId(), table.getId(), partition.getId(), currentTs);
        // I don't want to add a new LoadJobSourceType for the time being.
        TransactionState.LoadJobSourceType loadJobSourceType = TransactionState.LoadJobSourceType.BATCH_LOAD_JOB;
        TransactionState.TxnSourceType txnSourceType = TransactionState.TxnSourceType.FE;
        TransactionState.TxnCoordinator coordinator = new TransactionState.TxnCoordinator(txnSourceType, HOST_NAME);

        long txnId = 0;
        try {
            txnId = GlobalStateMgr.getCurrentGlobalTransactionMgr().beginTransaction(db.getId(),
                    Lists.newArrayList(table.getId()), label, coordinator,
                    loadJobSourceType, TXN_TIMEOUT_SECOND);
        } catch (AnalysisException | LabelAlreadyUsedException | BeginTransactionException | DuplicatedRequestException e) {
            LOG.error(e);
            return false;
        }

        List<MaterializedIndex> visibleIndexes = partition.getMaterializedIndices(MaterializedIndex.IndexExtState.VISIBLE);

        Map<Long, List<Long>> beToTablets = new HashMap<>();
        for (MaterializedIndex index : visibleIndexes) {
            for (Tablet tablet : index.getTablets()) {
                Long beId = chooseCompactionBackend((LakeTablet) tablet);
                if (beId == null) {
                    LOG.info("No available backend can execute publish version task");
                    return false;
                }
                beToTablets.computeIfAbsent(beId, k -> Lists.newArrayList()).add(tablet.getId());
            }
        }

        List<Future<CompactResponse>> responseList = Lists.newArrayListWithCapacity(beToTablets.size());
        SystemInfoService systemInfoService = GlobalStateMgr.getCurrentSystemInfo();
        for (Map.Entry<Long, List<Long>> entry : beToTablets.entrySet()) {
            Backend backend = systemInfoService.getBackend(entry.getKey());
            if (backend == null) {
                LOG.info("Backend {} has been dropped", entry.getKey());
                return false;
            }
            TNetworkAddress address = new TNetworkAddress();
            address.setHostname(backend.getHost());
            address.setPort(backend.getBrpcPort());

            LakeServiceClient client = new LakeServiceClient(address);
            CompactRequest request = new CompactRequest();
            request.tabletIds = entry.getValue();
            request.txnId = txnId;
            request.version = partition.getVisibleVersion();

            try {
                Future<CompactResponse> responseFuture = client.compact(request);
                responseList.add(responseFuture);
            } catch (Exception e) {
                LOG.warn(e);
                return false;
            }
        }

        for (Future<CompactResponse> responseFuture : responseList) {
            try {
                CompactResponse response = responseFuture.get();
                if (response != null && response.failedTablets != null && !response.failedTablets.isEmpty()) {
                    LOG.warn("Fail to compact tablet {}", response.failedTablets);
                    return false;
                }
            } catch (Exception e) {
                LOG.warn(e);
                return false;
            }
        }

        List<TabletCommitInfo> commitInfoList = Lists.newArrayList();
        for (Map.Entry<Long, List<Long>> entry : beToTablets.entrySet()) {
            for (Long tabletId : entry.getValue()) {
                commitInfoList.add(new TabletCommitInfo(tabletId, entry.getKey()));
            }
        }

        try {
            GlobalStateMgr.getCurrentGlobalTransactionMgr().commitTransaction(db.getId(), txnId, commitInfoList);
        } catch (UserException e) {
            LOG.error(e);
            return false;
        }

        return true;
    }

    // Returns null if no backend available.
    private Long chooseCompactionBackend(LakeTablet tablet) {
        try {
            return tablet.getPrimaryBackendId();
        } catch (UserException e) {
            LOG.info("Fail to get primary backend for tablet {}, choose a random alive backend", tablet.getId());
        }
        List<Long> backendIds = GlobalStateMgr.getCurrentSystemInfo().seqChooseBackendIds(1, true, false);
        if (backendIds.isEmpty()) {
            return null;
        }
        return backendIds.get(0);
    }
}
