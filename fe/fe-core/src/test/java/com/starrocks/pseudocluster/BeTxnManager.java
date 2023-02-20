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

package com.starrocks.pseudocluster;

import com.starrocks.common.UserException;
import com.starrocks.thrift.TFinishTaskRequest;
import com.starrocks.thrift.TPartitionVersionInfo;
import com.starrocks.thrift.TTabletVersionPair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BeTxnManager {
    private static final Logger LOG = LogManager.getLogger(BeTxnManager.class);

    static class TxnTabletInfo {
        long tabletId;
        Rowset rowset;

        public TxnTabletInfo(long tabletId) {
            this.tabletId = tabletId;
        }
    }

    static class TxnInfo {
        long txnId;
        Map<Long, Map<Long, TxnTabletInfo>> partitions;

        TxnInfo(long txnId) {
            this.txnId = txnId;
            partitions = new HashMap<>();
        }
    }

    PseudoBackend backend;
    Map<Long, TxnInfo> txns = new HashMap<>();

    BeTxnManager(PseudoBackend backend) {
        this.backend = backend;
    }

    public synchronized void commit(long txnId, long partitionId, Tablet tablet, Rowset rowset) throws UserException {
        TxnInfo tinfo = txns.computeIfAbsent(txnId, k -> new TxnInfo(k));
        Map<Long, TxnTabletInfo> tablets = tinfo.partitions.computeIfAbsent(partitionId, k -> new HashMap<>());
        TxnTabletInfo tabletInfo = tablets.get(tablet.id);
        if (tabletInfo != null) {
            LOG.info("txn:" + txnId + " tablet:" + tablet.id + " already exists, multi node sink");
            return;
        }
        tabletInfo = tablets.computeIfAbsent(tablet.id, id -> new TxnTabletInfo(id));
        tabletInfo.rowset = rowset;
    }

    public synchronized void publish(long txnId, List<TPartitionVersionInfo> partitions, TFinishTaskRequest finish) {
        TxnInfo tinfo = txns.get(txnId);
        Exception e = null;
        int totalTablets = 0;
        List<Long> errorTabletIds = new ArrayList<>();
        List<TTabletVersionPair> tabletVersions = new ArrayList<>();
        for (TPartitionVersionInfo pInfo : partitions) {
            if (tinfo == null) {
                List<Tablet> tabletsInPartition = backend.getTabletManager().getTablets(pInfo.partition_id);
                totalTablets += tabletsInPartition.size();
                for (Tablet tablet : tabletsInPartition) {
                    TTabletVersionPair p = new TTabletVersionPair();
                    p.tablet_id = tablet.id;
                    p.version = tablet.maxContinuousVersion();
                    tabletVersions.add(p);
                }
                continue;
            }
            Map<Long, TxnTabletInfo> tablets = tinfo.partitions.get(pInfo.partition_id);
            if (tablets == null) {
                LOG.warn("publish version txn:" + txnId + " partition:" + pInfo.partition_id + " not found");
                continue;
            }
            for (TxnTabletInfo tabletInfo : tablets.values()) {
                totalTablets++;
                Tablet tablet = backend.getTabletManager().getTablet(tabletInfo.tabletId);
                if (tablet == null) {
                    errorTabletIds.add(tabletInfo.tabletId);
                    if (e == null) {
                        e = new UserException(
                                "publish version failed txn:" + txnId + " partition:" + pInfo.partition_id + " tablet:" +
                                        tabletInfo.tabletId + " not found");
                    }
                } else {
                    try {
                        tablet.commitRowset(tabletInfo.rowset, pInfo.version);
                    } catch (Exception ex) {
                        errorTabletIds.add(tablet.id);
                        e = ex;
                    }
                    TTabletVersionPair p = new TTabletVersionPair();
                    p.tablet_id = tablet.id;
                    p.version = tablet.maxContinuousVersion();
                    tabletVersions.add(p);
                }
            }
        }
        LOG.info("backend: {} txn: {} publish version error:{} / total:{} {}", backend.be.getId(), txnId, errorTabletIds.size(),
                totalTablets,
                e == null ? "" : e.getMessage());
        finish.setError_tablet_ids(errorTabletIds);
        finish.setTablet_versions(tabletVersions);
        if (e != null) {
            finish.setTask_status(PseudoBackend.toStatus(e));
        }
    }

}
