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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/common/proc/TabletsProcDir.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.common.proc;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.LocalTablet;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Replica;
import com.starrocks.catalog.Tablet;
import com.starrocks.catalog.TabletInvertedIndex;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.FeConstants;
import com.starrocks.common.util.ListComparator;
import com.starrocks.common.util.NetUtils;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.monitor.unit.ByteSizeValue;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.Backend;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/*
 * SHOW PROC /dbs/dbId/tableId/partitions/partitionId/indexId
 * show tablets' detail info within an index
 * for LocalTablet
 */
public class LocalTabletsProcDir implements ProcDirInterface {
    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("TabletId").add("ReplicaId").add("BackendId").add("SchemaHash").add("Version")
            .add("VersionHash").add("LstSuccessVersion").add("LstSuccessVersionHash")
            .add("LstFailedVersion").add("LstFailedVersionHash").add("LstFailedTime")
            .add("DataSize").add("RowCount").add("State")
            .add("LstConsistencyCheckTime").add("CheckVersion").add("CheckVersionHash")
            .add("VersionCount").add("PathHash").add("MetaUrl").add("CompactionStatus")
            .add("DiskRootPath").add("IsConsistent").add("Checksum")
            .build();

    private final Database db;
    private final OlapTable table;
    private final MaterializedIndex index;

    public LocalTabletsProcDir(Database db, OlapTable table, MaterializedIndex index) {
        this.db = db;
        this.table = table;
        this.index = index;
    }

    public static int analyzeColumn(String columnName) throws AnalysisException {
        for (String title : TITLE_NAMES) {
            if (title.equalsIgnoreCase(columnName)) {
                return TITLE_NAMES.indexOf(title);
            }
        }

        throw new AnalysisException("Title name[" + columnName + "] does not exist");
    }

    public List<List<Comparable>> fetchComparableResult(long version, long backendId, Replica.ReplicaState state,
                                                        Boolean isConsistent, Boolean hideIpPort) {
        Preconditions.checkNotNull(db);
        Preconditions.checkNotNull(index);
        Preconditions.checkState(table.isOlapTableOrMaterializedView());
        ImmutableMap<Long, Backend> backendMap = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getIdToBackend();

        List<List<Comparable>> tabletInfos = new ArrayList<List<Comparable>>();
        Locker locker = new Locker();
        locker.lockDatabase(db.getId(), LockType.READ);
        try {
            // get infos
            for (Tablet tablet : index.getTablets()) {
                LocalTablet localTablet = (LocalTablet) tablet;
                long tabletId = tablet.getId();
                if (localTablet.getImmutableReplicas().isEmpty()) {
                    List<Comparable> tabletInfo = createTabletInfo(tabletId);
                    tabletInfos.add(tabletInfo);
                } else {
                    for (Replica replica : localTablet.getImmutableReplicas()) {
                        if ((version > -1 && replica.getVersion() != version)
                                || (backendId > -1 && replica.getBackendId() != backendId)
                                || (state != null && replica.getState() != state)
                                || (isConsistent != null && localTablet.isConsistent() != isConsistent)) {
                            continue;
                        }
                        List<Comparable> tabletInfo = new ArrayList<Comparable>();
                        // tabletId -- replicaId -- backendId -- version -- versionHash -- dataSize -- rowCount -- state
                        tabletInfo.add(tabletId);
                        tabletInfo.add(replica.getId());
                        tabletInfo.add(replica.getBackendId());
                        tabletInfo.add(Replica.DEPRECATED_PROP_SCHEMA_HASH);
                        tabletInfo.add(replica.getVersion());
                        tabletInfo.add(0);
                        tabletInfo.add(replica.getLastSuccessVersion());
                        tabletInfo.add(0);
                        tabletInfo.add(replica.getLastFailedVersion());
                        tabletInfo.add(0);
                        tabletInfo.add(TimeUtils.longToTimeString(replica.getLastFailedTimestamp()));
                        tabletInfo.add(new ByteSizeValue(replica.getDataSize()));
                        tabletInfo.add(replica.getRowCount());
                        tabletInfo.add(replica.getState());

                        tabletInfo.add(TimeUtils.longToTimeString(tablet.getLastCheckTime()));
                        tabletInfo.add(localTablet.getCheckedVersion());
                        tabletInfo.add(0);
                        tabletInfo.add(replica.getVersionCount());
                        tabletInfo.add(replica.getPathHash());
                        Backend backend = backendMap.get(replica.getBackendId());
                        String metaUrl;
                        String compactionUrl;
                        String diskRootPath;
                        if (backend != null) {
                            String hostPort = hideIpPort ? "*:0" :
                                    NetUtils.getHostPortInAccessibleFormat(backend.getHost(), backend.getHttpPort());
                            metaUrl = String.format("http://" + hostPort + "/api/meta/header/%d", tabletId);
                            compactionUrl = String.format(
                                    "http://" + hostPort + "/api/compaction/show?tablet_id=%d", tabletId);
                            diskRootPath = backend.getDiskRootPath(replica.getPathHash());
                        } else {
                            metaUrl = "N/A";
                            compactionUrl = "N/A";
                            diskRootPath = "N/A";
                        }
                        tabletInfo.add(metaUrl);
                        tabletInfo.add(compactionUrl);
                        tabletInfo.add(diskRootPath);
                        tabletInfo.add(localTablet.isConsistent());
                        tabletInfo.add(replica.getChecksum());

                        tabletInfos.add(tabletInfo);
                    }
                }
            }
        } finally {
            locker.unLockDatabase(db.getId(), LockType.READ);
        }
        return tabletInfos;
    }

    private static List<Comparable> createTabletInfo(long tabletId) {
        List<Comparable> tabletInfo = new ArrayList<Comparable>();
        tabletInfo.add(tabletId);
        tabletInfo.add(-1); // replica id
        tabletInfo.add(-1); // backend id
        tabletInfo.add(-1); // schema hash
        tabletInfo.add(-1); // version
        tabletInfo.add(0); // version hash
        tabletInfo.add(-1); // lst success version
        tabletInfo.add(0); // lst success version hash
        tabletInfo.add(-1); // lst failed version
        tabletInfo.add(0); // lst failed version hash
        tabletInfo.add(-1); // lst failed time
        tabletInfo.add(-1); // data size
        tabletInfo.add(-1); // row count
        tabletInfo.add(FeConstants.NULL_STRING); // state
        tabletInfo.add(-1); // lst consistency check time
        tabletInfo.add(-1); // check version
        tabletInfo.add(0); // check version hash
        tabletInfo.add(-1); // version count
        tabletInfo.add(-1); // path hash
        tabletInfo.add(FeConstants.NULL_STRING); // meta url
        tabletInfo.add(FeConstants.NULL_STRING); // compaction status
        tabletInfo.add(FeConstants.NULL_STRING); // DiskRootPath
        tabletInfo.add(true); // is consistent
        tabletInfo.add(-1); // checksum
        return tabletInfo;
    }

    private List<List<Comparable>> fetchComparableResult() {
        return fetchComparableResult(-1, -1, null, null, false);
    }

    @Override
    public ProcResult fetchResult() {
        List<List<Comparable>> tabletInfos = fetchComparableResult();
        // sort by tabletId, replicaId
        ListComparator<List<Comparable>> comparator = new ListComparator<List<Comparable>>(0, 1);
        Collections.sort(tabletInfos, comparator);

        // set result
        BaseProcResult result = new BaseProcResult();
        result.setNames(TITLE_NAMES);

        for (int i = 0; i < tabletInfos.size(); i++) {
            List<Comparable> info = tabletInfos.get(i);
            List<String> row = new ArrayList<String>(info.size());
            for (int j = 0; j < info.size(); j++) {
                row.add(info.get(j).toString());
            }
            result.addRow(row);
        }
        return result;
    }

    @Override
    public boolean register(String name, ProcNodeInterface node) {
        return false;
    }

    @Override
    public ProcNodeInterface lookup(String tabletIdStr) throws AnalysisException {
        Preconditions.checkNotNull(db);
        Preconditions.checkNotNull(index);

        long tabletId;
        try {
            tabletId = Long.valueOf(tabletIdStr);
        } catch (NumberFormatException e) {
            throw new AnalysisException("Invalid tablet id format: " + tabletIdStr);
        }

        TabletInvertedIndex invertedIndex = GlobalStateMgr.getCurrentState().getTabletInvertedIndex();
        List<Replica> replicas = invertedIndex.getReplicasByTabletId(tabletId);
        return new ReplicasProcNode(db, table, tabletId, replicas);
    }
}

