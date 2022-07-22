// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.lake.compaction;

import com.google.common.collect.Lists;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Tablet;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.DuplicatedRequestException;
import com.starrocks.common.LabelAlreadyUsedException;
import com.starrocks.common.UserException;
import com.starrocks.common.util.LeaderDaemon;
import com.starrocks.lake.LakeTable;
import com.starrocks.lake.LakeTablet;
import com.starrocks.lake.proto.CompactRequest;
import com.starrocks.lake.proto.CompactResponse;
import com.starrocks.rpc.LakeServiceClient;
import com.starrocks.rpc.RpcException;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.service.FrontendOptions;
import com.starrocks.system.Backend;
import com.starrocks.system.SystemInfoService;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.transaction.BeginTransactionException;
import com.starrocks.transaction.GlobalTransactionMgr;
import com.starrocks.transaction.TabletCommitInfo;
import com.starrocks.transaction.TransactionState;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class CompactionDispatchDaemon extends LeaderDaemon {
    private static final Logger LOG = LogManager.getLogger(CompactionDispatchDaemon.class);
    private static final String HOST_NAME = FrontendOptions.getLocalHostAddress();
    private static final long TXN_TIMEOUT_SECOND = 1800L;
    private static final long BUSY_LOOP_INTERVAL_MS = 10L;
    private static final long IDLE_LOOP_INTERVAL_MS = 500L;
    private static final long MIN_COMPACTION_INTERVAL_MS_ON_SUCCESS = 1000L;
    private static final long MIN_COMPACTION_INTERVAL_MS_ON_FAILURE = 5000L;

    public CompactionDispatchDaemon() {
        super("COMPACTION_DISPATCH", BUSY_LOOP_INTERVAL_MS);
    }

    @Override
    protected void runAfterCatalogReady() {
        CompactionManager compactionManager = GlobalStateMgr.getCurrentState().getCompactionManager();
        PartitionIdentifier partitionIdentifier = compactionManager.choosePartitionToCompact();
        if (partitionIdentifier == null) {
            try {
                Thread.sleep(IDLE_LOOP_INTERVAL_MS - BUSY_LOOP_INTERVAL_MS);
            } catch (InterruptedException ignored) {
            }
            return;
        }
        Database db = GlobalStateMgr.getCurrentState().getDb(partitionIdentifier.getDbId());
        if (db == null) {
            compactionManager.removePartition(partitionIdentifier);
            return;
        }

        if (!db.tryReadLock(50, TimeUnit.MILLISECONDS)) {
            LOG.info("Skipped partition compaction due to get database lock timeout");
            compactionManager.enableCompactionAfter(partitionIdentifier, MIN_COMPACTION_INTERVAL_MS_ON_FAILURE);
            return;
        }

        long currentVersion;
        LakeTable table;
        Partition partition;
        Map<Long, List<Long>> beToTablets;

        try {
            table = (LakeTable) db.getTable(partitionIdentifier.getTableId());
            partition = (table != null) ? table.getPartition(partitionIdentifier.getPartitionId()) : null;
            if (partition == null) {
                compactionManager.removePartition(partitionIdentifier);
                return;
            }
            currentVersion = partition.getVisibleVersion();
            beToTablets = collectPartitionTablets(partition);
            if (beToTablets.isEmpty()) {
                compactionManager.enableCompactionAfter(partitionIdentifier, 10L * 1000);
                return;
            }
        } finally {
            db.readUnlock();
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Compacting partition {}.{}.{}", db.getFullName(), table.getName(), partition.getName());
        }

        long currentTs = System.currentTimeMillis();
        String label = String.format("COMPACTION_%d-%d-%d-%d", db.getId(), table.getId(), partition.getId(), currentTs);
        TransactionState.LoadJobSourceType loadJobSourceType = TransactionState.LoadJobSourceType.LAKE_COMPACTION;
        TransactionState.TxnSourceType txnSourceType = TransactionState.TxnSourceType.FE;
        TransactionState.TxnCoordinator coordinator = new TransactionState.TxnCoordinator(txnSourceType, HOST_NAME);

        long txnId;
        try {
            txnId = GlobalStateMgr.getCurrentGlobalTransactionMgr().beginTransaction(db.getId(),
                    Lists.newArrayList(table.getId()), label, coordinator,
                    loadJobSourceType, TXN_TIMEOUT_SECOND);
        } catch (BeginTransactionException | AnalysisException | LabelAlreadyUsedException | DuplicatedRequestException e) {
            LOG.error(e);
            return;
        }

        long nextCompactionInterval = MIN_COMPACTION_INTERVAL_MS_ON_SUCCESS;
        try {
            compactTablets(db, currentVersion, beToTablets, txnId);
        } catch (UserException | RpcException | ExecutionException | InterruptedException e) {
            nextCompactionInterval = MIN_COMPACTION_INTERVAL_MS_ON_FAILURE;
            LOG.error(e);
            try {
                GlobalStateMgr.getCurrentGlobalTransactionMgr().abortTransaction(db.getId(), txnId, e.getMessage());
            } catch (UserException ex) {
                LOG.error(ex);
            }
        } finally {
            compactionManager.enableCompactionAfter(partitionIdentifier, nextCompactionInterval);
        }
    }

    private void compactTablets(Database db, long currentVersion, Map<Long, List<Long>> beToTablets, long txnId)
            throws UserException, RpcException, ExecutionException, InterruptedException {
        List<Future<CompactResponse>> responseList = Lists.newArrayListWithCapacity(beToTablets.size());
        SystemInfoService systemInfoService = GlobalStateMgr.getCurrentSystemInfo();
        for (Map.Entry<Long, List<Long>> entry : beToTablets.entrySet()) {
            Backend backend = systemInfoService.getBackend(entry.getKey());
            if (backend == null) {
                throw new UserException("Backend " + entry.getKey() + " has been dropped");
            }
            TNetworkAddress address = new TNetworkAddress();
            address.setHostname(backend.getHost());
            address.setPort(backend.getBrpcPort());

            LakeServiceClient client = new LakeServiceClient(address);
            CompactRequest request = new CompactRequest();
            request.tabletIds = entry.getValue();
            request.txnId = txnId;
            request.version = currentVersion;

            Future<CompactResponse> responseFuture = client.compact(request);
            responseList.add(responseFuture);
        }

        for (Future<CompactResponse> responseFuture : responseList) {
            CompactResponse response = responseFuture.get();
            if (response != null && response.failedTablets != null && !response.failedTablets.isEmpty()) {
                throw new UserException("Fail to compact tablet " + response.failedTablets);
            }
        }

        List<TabletCommitInfo> commitInfoList = Lists.newArrayList();
        for (Map.Entry<Long, List<Long>> entry : beToTablets.entrySet()) {
            for (Long tabletId : entry.getValue()) {
                commitInfoList.add(new TabletCommitInfo(tabletId, entry.getKey()));
            }
        }

        GlobalTransactionMgr txnManager = GlobalStateMgr.getCurrentGlobalTransactionMgr();
        while (true) {
            long timeoutMs = 3L * 1000;
            boolean visible = txnManager.commitAndPublishTransaction(db, txnId, commitInfoList, timeoutMs);
            if (visible) {
                break;
            }
            LOG.info("Publish version timed out, will retry");
        }
    }

    private Map<Long, List<Long>> collectPartitionTablets(Partition partition) {
        List<MaterializedIndex> visibleIndexes = partition.getMaterializedIndices(MaterializedIndex.IndexExtState.VISIBLE);
        Map<Long, List<Long>> beToTablets = new HashMap<>();
        for (MaterializedIndex index : visibleIndexes) {
            for (Tablet tablet : index.getTablets()) {
                Long beId = chooseBackendForDoingCompaction((LakeTablet) tablet);
                if (beId == null) {
                    beToTablets.clear();
                    return beToTablets;
                }
                beToTablets.computeIfAbsent(beId, k -> Lists.newArrayList()).add(tablet.getId());
            }
        }
        return beToTablets;
    }

    // Returns null if no backend available.
    private Long chooseBackendForDoingCompaction(LakeTablet tablet) {
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
