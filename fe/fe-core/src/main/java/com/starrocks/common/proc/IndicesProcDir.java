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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/common/proc/IndicesProcDir.java

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
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedIndex.IndexExtState;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.util.ListComparator;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/*
 * SHOW PROC /dbs/dbId/tableId/partitions/partitionId
 * show index's detail info within a partition
 */
public class IndicesProcDir implements ProcDirInterface {
    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("IndexId").add("IndexName").add("State").add("LastConsistencyCheckTime")
            .add("VirtualBuckets").add("Tablets")
            .build();

    private Database db;
    private OlapTable olapTable;
    private PhysicalPartition partition;

    public IndicesProcDir(Database db, OlapTable olapTable, PhysicalPartition partition) {
        this.db = db;
        this.olapTable = olapTable;
        this.partition = partition;
    }

    @Override
    public ProcResult fetchResult() throws AnalysisException {
        Preconditions.checkNotNull(db);
        Preconditions.checkNotNull(partition);

        BaseProcResult result = new BaseProcResult();
        // get info
        List<List<Comparable>> indexInfos = new ArrayList<List<Comparable>>();
        Locker locker = new Locker();
        locker.lockDatabase(db.getId(), LockType.READ);
        try {
            result.setNames(TITLE_NAMES);
            for (MaterializedIndex materializedIndex : partition.getMaterializedIndices(IndexExtState.ALL)) {
                List<Comparable> indexInfo = new ArrayList<Comparable>();
                indexInfo.add(materializedIndex.getId());
                indexInfo.add(olapTable.getIndexNameById(materializedIndex.getId()));
                indexInfo.add(materializedIndex.getState());
                indexInfo.add(TimeUtils.longToTimeString(materializedIndex.getLastCheckTime()));
                indexInfo.add(materializedIndex.getVirtualBuckets().size());
                indexInfo.add(materializedIndex.getTablets().size());

                indexInfos.add(indexInfo);
            }

        } finally {
            locker.unLockDatabase(db.getId(), LockType.READ);
        }

        // sort by index id
        ListComparator<List<Comparable>> comparator = new ListComparator<List<Comparable>>(0);
        Collections.sort(indexInfos, comparator);

        // set result
        for (List<Comparable> info : indexInfos) {
            List<String> row = new ArrayList<String>(info.size());
            for (Comparable comparable : info) {
                row.add(comparable.toString());
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
    public ProcNodeInterface lookup(String indexIdStr) throws AnalysisException {
        Preconditions.checkNotNull(db);
        Preconditions.checkNotNull(partition);
        if (Strings.isNullOrEmpty(indexIdStr)) {
            throw new AnalysisException("Index id is null");
        }

        long indexId;
        try {
            indexId = Long.valueOf(indexIdStr);
        } catch (NumberFormatException e) {
            throw new AnalysisException("Invalid index id format: " + indexIdStr);
        }

        Locker locker = new Locker();
        locker.lockDatabase(db.getId(), LockType.READ);
        try {
            MaterializedIndex materializedIndex = partition.getIndex(indexId);
            if (materializedIndex == null) {
                throw new AnalysisException("Index[" + indexId + "] does not exist.");
            }
            if (olapTable.isCloudNativeTableOrMaterializedView()) {
                return new LakeTabletsProcDir(db, olapTable, materializedIndex);
            } else {
                return new LocalTabletsProcDir(db, olapTable, materializedIndex);
            }
        } finally {
            locker.unLockDatabase(db.getId(), LockType.READ);
        }
    }

}
