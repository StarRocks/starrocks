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

package com.starrocks.alter.dynamictablet;

import com.starrocks.catalog.Database;
import com.starrocks.catalog.DistributionInfo.DistributionInfoType;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.util.FrontendDaemon;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.SplitTabletClause;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class DynamicTabletChecker extends FrontendDaemon {
    private static final Logger LOG = LogManager.getLogger(DynamicTabletChecker.class);

    public DynamicTabletChecker() {
        super("DynamicTabletChecker", Config.dynamic_tablet_checker_interval_ms);
    }

    @Override
    protected void runAfterCatalogReady() {
        if (!Config.enable_dynamic_tablet) {
            return;
        }

        for (Database db : GlobalStateMgr.getCurrentState().getLocalMetastore().getAllDbs()) {
            for (Table table : GlobalStateMgr.getCurrentState().getLocalMetastore().getTables(db.getId())) {
                if (!table.isCloudNativeTableOrMaterializedView()) {
                    continue;
                }

                OlapTable olapTable = (OlapTable) table;
                if (!olapTable.isEnableDynamicTablet()) {
                    continue;
                }

                if (olapTable.getDefaultDistributionInfo().getType() != DistributionInfoType.HASH) {
                    continue;
                }

                SplitTabletClause splitTabletClause = null;

                Locker locker = new Locker();
                locker.lockTableWithIntensiveDbLock(db.getId(), table.getId(), LockType.READ);
                try {
                    if (olapTable.getState() != OlapTable.OlapTableState.NORMAL) {
                        continue;
                    }

                    if (olapTable.getPhysicalPartitions().stream()
                            .anyMatch(p -> p.getTabletMaxDataSize() >= Config.dynamic_tablet_split_size)) {
                        splitTabletClause = new SplitTabletClause();
                    }
                } finally {
                    locker.unLockTableWithIntensiveDbLock(db.getId(), table.getId(), LockType.READ);
                }

                if (splitTabletClause == null) {
                    continue;
                }

                LOG.info("Creating split tablet job for table {}.{}", db.getFullName(), table.getName());

                try {
                    GlobalStateMgr.getCurrentState().getDynamicTabletJobMgr().createDynamicTabletJob(db, olapTable,
                            splitTabletClause);
                } catch (StarRocksException e) {
                    LOG.info("Failed to create split tablet job for table {}.{}. ", db.getFullName(), table.getName(),
                            e);
                    break;
                } catch (Exception e) {
                    LOG.warn("Failed to create split tablet job for table {}.{}. ", db.getFullName(), table.getName(),
                            e);
                    break;
                }
            }
        }
    }
}
