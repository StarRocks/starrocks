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


package com.starrocks.common.proc;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Tablet;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.util.ListComparator;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.lake.LakeTablet;
import com.starrocks.monitor.unit.ByteSizeValue;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/*
 * SHOW PROC /dbs/dbId/tableId/partitions/partitionId/indexId
 * show tablets' detail info within an index
 * for LakeTablet
 */
public class LakeTabletsProcDir implements ProcDirInterface {
    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("TabletId").add("BackendId").add("DataSize").add("RowCount")
            .build();

    private final Database db;
    // lake table or lake materialized view
    private final OlapTable table;
    private final MaterializedIndex index;

    public LakeTabletsProcDir(Database db, OlapTable table, MaterializedIndex index) {
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

    public List<List<Comparable>> fetchComparableResult() {
        Preconditions.checkNotNull(db);
        Preconditions.checkNotNull(index);
        Preconditions.checkState(table.isCloudNativeTableOrMaterializedView());

        List<List<Comparable>> tabletInfos = Lists.newArrayList();
        Locker locker = new Locker();
        locker.lockDatabase(db, LockType.READ);
        try {
            for (Tablet tablet : index.getTablets()) {
                List<Comparable> tabletInfo = Lists.newArrayList();
                LakeTablet lakeTablet = (LakeTablet) tablet;
                tabletInfo.add(lakeTablet.getId());
                tabletInfo.add(new Gson().toJson(lakeTablet.getBackendIds()));
                tabletInfo.add(new ByteSizeValue(lakeTablet.getDataSize(true)));
                tabletInfo.add(lakeTablet.getRowCount(0L));
                tabletInfos.add(tabletInfo);
            }
        } finally {
            locker.unLockDatabase(db, LockType.READ);
        }
        return tabletInfos;
    }

    @Override
    public ProcResult fetchResult() {
        List<List<Comparable>> tabletInfos = fetchComparableResult();

        // sort by tabletId
        ListComparator<List<Comparable>> comparator = new ListComparator<List<Comparable>>(0);
        Collections.sort(tabletInfos, comparator);

        // set result
        BaseProcResult result = new BaseProcResult();
        result.setNames(TITLE_NAMES);

        for (int i = 0; i < tabletInfos.size(); i++) {
            List<Comparable> info = tabletInfos.get(i);
            List<String> row = Lists.newArrayListWithCapacity(info.size());
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
        Preconditions.checkState(table.isCloudNativeTableOrMaterializedView());

        long tabletId = -1L;
        try {
            tabletId = Long.parseLong(tabletIdStr);
        } catch (NumberFormatException e) {
            throw new AnalysisException("Invalid tablet id format: " + tabletIdStr);
        }

        Locker locker = new Locker();
        locker.lockDatabase(db, LockType.READ);
        try {
            Tablet tablet = index.getTablet(tabletId);
            if (tablet == null) {
                throw new AnalysisException("Can't find tablet id: " + tabletIdStr);
            }
            Preconditions.checkState(tablet instanceof LakeTablet);
            return new LakeTabletProcNode((LakeTablet) tablet);
        } finally {
            locker.unLockDatabase(db, LockType.READ);
        }
    }

    // Handle showing single tablet info
    public static class LakeTabletProcNode implements ProcNodeInterface {
        private final LakeTablet tablet;

        public LakeTabletProcNode(LakeTablet tablet) {
            this.tablet = tablet;
        }

        @Override
        public ProcResult fetchResult() throws AnalysisException {
            BaseProcResult result = new BaseProcResult();
            result.setNames(TITLE_NAMES);
            List<String> row = Arrays.asList(
                    String.valueOf(tablet.getId()),
                    new Gson().toJson(tablet.getBackendIds()),
                    new ByteSizeValue(tablet.getDataSize(true)).toString(),
                    String.valueOf(tablet.getRowCount(0L))
            );
            result.addRow(row);
            return result;
        }
    }
}
