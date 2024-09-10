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
package com.starrocks.scheduler.mv;

import com.google.common.collect.Lists;
import com.starrocks.catalog.BaseTableInfo;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.Table;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.connector.HivePartitionDataInfo;
import com.starrocks.connector.TableUpdateArbitrator;
import com.starrocks.mv.MVMetaVersionRepairer;
import com.starrocks.sql.common.DmlException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * When some tables(eg: hive) have been dropped and recreated, the table identifier may change, we can repair it by updating the
 * table identifier in the materialized view's base table info.
 * NOTE: This is a simple policy for now, we can make it more robust and flexible for more table kinds in the future.
 */
public class MVPCTMetaRepairer {
    private static final Logger LOG = LogManager.getLogger(MVPCTMetaRepairer.class);

    // mv's database
    private final Database db;
    // mv to repair
    private final MaterializedView mv;

    public MVPCTMetaRepairer(Database db, MaterializedView mv) {
        this.db = db;
        this.mv = mv;
    }

    /**
     * Repair the table's meta-info in the materialized view's base table info if possible.
     * @param table the base table of materialized view to repair
     * @param baseTableInfo the base table info of the materialized view
     */
    public void repairTableIfNeeded(Table table,
                                    BaseTableInfo baseTableInfo) throws DmlException {
        if (baseTableInfo.getTableIdentifier().equals(table.getTableIdentifier())) {
            LOG.info("base table:{} identifier has not changed, no need to repair",
                    baseTableInfo.getTableInfoStr());
            return;
        }

        // table identifier changed, original table may be dropped and recreated
        // consider auto refresh partition limit
        // format: l_shipdate=1998-01-02
        // consider __HIVE_DEFAULT_PARTITION__
        LOG.info("base table:{} identifier has changed from:{} to:{}",
                baseTableInfo.getTableName(), baseTableInfo.getTableIdentifier(), table.getTableIdentifier());
        Map<String, MaterializedView.BasePartitionInfo> partitionInfoMap =
                mv.getBaseTableRefreshInfo(baseTableInfo);
        if (partitionInfoMap == null || partitionInfoMap.isEmpty()) {
            return;
        }
        boolean isAutoRefresh = mv.getRefreshScheme().isAsync();
        int autoRefreshPartitionsLimit = -1;
        if (isAutoRefresh) {
            // only work for auto refresh
            // for manual refresh, we respect the partition range specified by user
            autoRefreshPartitionsLimit = mv.getTableProperty().getAutoRefreshPartitionsLimit();
        }
        List<String> partitionNames = Lists.newArrayList(partitionInfoMap.keySet());
        TableUpdateArbitrator.UpdateContext updateContext = new TableUpdateArbitrator.UpdateContext(
                table,
                autoRefreshPartitionsLimit,
                partitionNames);
        TableUpdateArbitrator arbitrator = TableUpdateArbitrator.create(updateContext);
        if (arbitrator == null) {
            return;
        }
        Map<String, Optional<HivePartitionDataInfo>> partitionDataInfos = arbitrator.getPartitionDataInfos();
        List<String> updatedPartitionNames =
                getUpdatedPartitionNames(partitionNames, partitionInfoMap, partitionDataInfos);
        LOG.info("try to get updated partitions names based on data.partitionNames:{}, isAutoRefresh:{}," +
                        " autoRefreshPartitionsLimit:{}, updatedPartitionNames:{}",
                partitionNames, isAutoRefresh, autoRefreshPartitionsLimit, updatedPartitionNames);
        // if partition is not modified, change the last refresh time to update
        repairMvBaseTableMeta(mv, baseTableInfo, table, updatedPartitionNames);
    }

    private List<String> getUpdatedPartitionNames(
            List<String> partitionNames,
            Map<String, MaterializedView.BasePartitionInfo> tablePartitionInfoMap,
            Map<String, Optional<HivePartitionDataInfo>> partitionDataInfos) {
        List<String> updatedPartitionNames = Lists.newArrayList();
        for (int i = 0; i < partitionNames.size(); i++) {
            String partitionName = partitionNames.get(i);
            if (!partitionDataInfos.containsKey(partitionName)) {
                continue;
            }
            MaterializedView.BasePartitionInfo basePartitionInfo = tablePartitionInfoMap.get(partitionName);
            Optional<HivePartitionDataInfo> partitionDataInfoOptional = partitionDataInfos.get(partitionName);
            if (partitionDataInfoOptional.isEmpty()) {
                updatedPartitionNames.add(partitionNames.get(i));
            } else {
                HivePartitionDataInfo hivePartitionDataInfo = partitionDataInfoOptional.get();
                // if file last modified time changed or file number under partition change,
                // the partition is treated as changed
                if (basePartitionInfo.getExtLastFileModifiedTime() != hivePartitionDataInfo.getLastFileModifiedTime()
                        || basePartitionInfo.getFileNumber() != hivePartitionDataInfo.getFileNumber()) {
                    updatedPartitionNames.add(partitionNames.get(i));
                }
            }
        }
        return updatedPartitionNames;
    }

    private void repairMvBaseTableMeta(MaterializedView mv, BaseTableInfo oldBaseTableInfo,
                                       Table newTable, List<String> updatedPartitionNames) {
        if (oldBaseTableInfo.isInternalCatalog()) {
            return;
        }
        // acquire db write lock to modify meta of mv
        Locker locker = new Locker();
        if (!locker.lockDatabaseAndCheckExist(db, mv, LockType.WRITE)) {
            throw new DmlException("repair mv meta failed. database:" + db.getFullName() + " not exist");
        }
        try {
            MVMetaVersionRepairer.repairExternalBaseTableInfo(mv, oldBaseTableInfo, newTable, updatedPartitionNames);
        } finally {
            locker.unLockTableWithIntensiveDbLock(db.getId(), mv.getId(), LockType.WRITE);
        }
    }
}
