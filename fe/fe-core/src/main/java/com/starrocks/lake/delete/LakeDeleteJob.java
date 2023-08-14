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


package com.starrocks.lake.delete;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.analysis.BinaryPredicate;
import com.starrocks.analysis.InPredicate;
import com.starrocks.analysis.IsNullPredicate;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.analysis.Predicate;
import com.starrocks.analysis.SlotRef;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.UserException;
import com.starrocks.lake.Utils;
import com.starrocks.load.DeleteJob;
import com.starrocks.load.DeleteMgr;
import com.starrocks.load.MultiDeleteInfo;
import com.starrocks.proto.BinaryPredicatePB;
import com.starrocks.proto.DeleteDataRequest;
import com.starrocks.proto.DeleteDataResponse;
import com.starrocks.proto.DeletePredicatePB;
import com.starrocks.proto.InPredicatePB;
import com.starrocks.proto.IsNullPredicatePB;
import com.starrocks.qe.QueryStateException;
import com.starrocks.rpc.BrpcProxy;
import com.starrocks.rpc.LakeService;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.DeleteStmt;
import com.starrocks.system.ComputeNode;
import com.starrocks.system.SystemInfoService;
import com.starrocks.transaction.TabletCommitInfo;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

/**
 * LakeDeleteJob is used to delete data for lake table.
 * 1. Creates DeletePredicatePB which is used in BE tablet metadata directly.
 * 2. No longer use the old push task api.
 */
public class LakeDeleteJob extends DeleteJob {
    private static final Logger LOG = LogManager.getLogger(LakeDeleteJob.class);

    private Map<Long, List<Long>> beToTablets;

    public LakeDeleteJob(long id, long transactionId, String label, MultiDeleteInfo deleteInfo) {
        super(id, transactionId, label, deleteInfo);
        beToTablets = Maps.newHashMap();
    }

    @Override
    @java.lang.SuppressWarnings("squid:S2142")  // allow catch InterruptedException
    public void run(DeleteStmt stmt, Database db, Table table, List<Partition> partitions)
            throws DdlException, QueryStateException {
        Preconditions.checkState(table.isCloudNativeTable());

        db.readLock();
        try {
            beToTablets = Utils.groupTabletID(partitions, MaterializedIndex.IndexExtState.VISIBLE);
        } catch (Throwable t) {
            LOG.warn("error occurred during delete process", t);
            // if transaction has been begun, need to abort it
            if (GlobalStateMgr.getCurrentGlobalTransactionMgr()
                    .getTransactionState(db.getId(), getTransactionId()) != null) {
                cancel(DeleteMgr.CancelType.UNKNOWN, t.getMessage());
            }
            throw new DdlException(t.getMessage(), t);
        } finally {
            db.readUnlock();
        }

        // create delete predicate
        List<Predicate> conditions = stmt.getDeleteConditions();
        DeletePredicatePB deletePredicate = createDeletePredicate(conditions);

        // send delete data request to BE
        try {
            List<Future<DeleteDataResponse>> responseList = Lists.newArrayListWithCapacity(
                    beToTablets.size());
            SystemInfoService systemInfoService = GlobalStateMgr.getCurrentSystemInfo();
            for (Map.Entry<Long, List<Long>> entry : beToTablets.entrySet()) {
                // TODO: need to refactor after be split into cn + dn
                ComputeNode backend = systemInfoService.getBackendOrComputeNode(entry.getKey());
                if (backend == null) {
                    throw new DdlException("Backend or computeNode" + entry.getKey() + " has been dropped");
                }
                DeleteDataRequest request = new DeleteDataRequest();
                request.tabletIds = entry.getValue();
                request.txnId = getTransactionId();
                request.deletePredicate = deletePredicate;

                LakeService lakeService = BrpcProxy.getLakeService(backend.getHost(), backend.getBrpcPort());
                Future<DeleteDataResponse> responseFuture = lakeService.deleteData(request);
                responseList.add(responseFuture);
            }

            for (Future<DeleteDataResponse> responseFuture : responseList) {
                DeleteDataResponse response = responseFuture.get();
                if (response != null && response.failedTablets != null && !response.failedTablets.isEmpty()) {
                    LOG.warn("Failed to execute delete. failed tablet: {}", response.failedTablets);
                    throw new DdlException("Failed to execute delete. failed tablet num: "
                            + response.failedTablets.size());
                }
            }
        } catch (Throwable e) {
            cancel(DeleteMgr.CancelType.UNKNOWN, e.getMessage());
            throw new DdlException(e.getMessage());
        }

        commit(db, getTimeoutMs());
    }

    private DeletePredicatePB createDeletePredicate(List<Predicate> conditions) {
        DeletePredicatePB deletePredicate = new DeletePredicatePB();
        deletePredicate.version = -1; // Required but unused
        deletePredicate.binaryPredicates = Lists.newArrayList();
        deletePredicate.isNullPredicates = Lists.newArrayList();
        deletePredicate.inPredicates = Lists.newArrayList();
        for (Predicate condition : conditions) {
            if (condition instanceof BinaryPredicate) {
                BinaryPredicate binaryPredicate = (BinaryPredicate) condition;
                BinaryPredicatePB binaryPredicatePB = new BinaryPredicatePB();
                binaryPredicatePB.columnName = ((SlotRef) binaryPredicate.getChild(0)).getColumnName();
                binaryPredicatePB.op = binaryPredicate.getOp().toString();
                binaryPredicatePB.value = ((LiteralExpr) binaryPredicate.getChild(1)).getStringValue();
                deletePredicate.binaryPredicates.add(binaryPredicatePB);
            } else if (condition instanceof IsNullPredicate) {
                IsNullPredicate isNullPredicate = (IsNullPredicate) condition;
                IsNullPredicatePB isNullPredicatePB = new IsNullPredicatePB();
                isNullPredicatePB.columnName = ((SlotRef) isNullPredicate.getChild(0)).getColumnName();
                isNullPredicatePB.isNotNull = isNullPredicate.isNotNull();
                deletePredicate.isNullPredicates.add(isNullPredicatePB);
            } else if (condition instanceof InPredicate) {
                InPredicate inPredicate = (InPredicate) condition;
                InPredicatePB inPredicatePB = new InPredicatePB();
                inPredicatePB.columnName = ((SlotRef) inPredicate.getChild(0)).getColumnName();
                inPredicatePB.isNotIn = inPredicate.isNotIn();
                inPredicatePB.values = Lists.newArrayList();
                for (int i = 1; i <= inPredicate.getInElementNum(); i++) {
                    inPredicatePB.values.add(((LiteralExpr) inPredicate.getChild(i)).getStringValue());
                }
                deletePredicate.inPredicates.add(inPredicatePB);
            }
        }
        return deletePredicate;
    }

    @Override
    public long getTimeoutMs() {
        long totalTablets = beToTablets.values().stream().flatMap(List::stream).count();
        // timeout is between 30 seconds to 5 min
        long timeout = Math.max(totalTablets * Config.tablet_delete_timeout_second * 1000L, 30000L);
        return Math.min(timeout, Config.load_straggler_wait_second * 1000L);
    }

    @Override
    public void clear() {
        GlobalStateMgr.getCurrentState().getDeleteMgr().removeKillJob(getId());
    }

    @Override
    public boolean commitImpl(Database db, long timeoutMs) throws UserException {
        List<TabletCommitInfo> tabletCommitInfos = Lists.newArrayList();
        for (Map.Entry<Long, List<Long>> entry : beToTablets.entrySet()) {
            long backendId = entry.getKey();
            for (long tabletId : entry.getValue()) {
                tabletCommitInfos.add(new TabletCommitInfo(tabletId, backendId));
            }
        }

        return GlobalStateMgr.getCurrentGlobalTransactionMgr()
                .commitAndPublishTransaction(db, getTransactionId(), tabletCommitInfos, Collections.emptyList(),
                        timeoutMs);
    }
}