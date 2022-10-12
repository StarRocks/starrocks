// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.common.proc;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.Tablet;
import com.starrocks.common.util.ListComparator;
import com.starrocks.lake.LakeTable;
import com.starrocks.lake.LakeTablet;
import com.starrocks.monitor.unit.ByteSizeValue;

import java.util.Collections;
import java.util.List;

/*
 * SHOW PROC /dbs/dbId/tableId/partitions/partitionId/indexId
 * show tablets' detail info within an index
 * for LakeTablet
 */
public class LakeTabletsProcNode implements ProcNodeInterface {
    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("TabletId").add("BackendIds").add("DataSize").add("RowCount")
            .build();

    private final Database db;
    private final LakeTable table;
    private final MaterializedIndex index;

    public LakeTabletsProcNode(Database db, LakeTable table, MaterializedIndex index) {
        this.db = db;
        this.table = table;
        this.index = index;
    }

    public List<List<Comparable>> fetchComparableResult() {
        Preconditions.checkNotNull(db);
        Preconditions.checkNotNull(index);
        Preconditions.checkState(table.isLakeTable());

        List<List<Comparable>> tabletInfos = Lists.newArrayList();
        db.readLock();
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
            db.readUnlock();
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
}
