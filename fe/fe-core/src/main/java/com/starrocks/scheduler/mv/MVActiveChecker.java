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

import com.starrocks.catalog.Database;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.MvId;
import com.starrocks.catalog.Table;
import com.starrocks.common.FeConstants;
import com.starrocks.common.util.LeaderDaemon;
import com.starrocks.server.GlobalStateMgr;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

public class MVActiveChecker extends LeaderDaemon {
    private static final long EXECUTOR_INTERVAL_MILLIS = 10000;
    private static final Logger LOG = LogManager.getLogger(MVActiveChecker.class);

    public MVActiveChecker() {
        super("MV Active Checker", EXECUTOR_INTERVAL_MILLIS);
    }

    @Override
    protected void runAfterCatalogReady() {
        try {
            runImpl();
        } catch (Throwable e) {
            LOG.error("Failed to run the MVActiveChecker ", e);
        }
    }

    private void runImpl() {
        if (FeConstants.runningUnitTest) {
            setStop();
        }

        List<String> dbNames = GlobalStateMgr.getCurrentState().getMetadataMgr().
                listDbNames(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME);

        for (String dbName : dbNames) {
            Database db = GlobalStateMgr.getCurrentState().getMetadataMgr().getDb(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                    dbName);
            for (MaterializedView mv : db.getMaterializedViews()) {
                for (MaterializedView.BaseTableInfo baseTableInfo : mv.getBaseTableInfos()) {
                    Table table = baseTableInfo.getTable();
                    if (table == null && mv.isActive()) {
                        LOG.warn("tableName :{} do not exist. set materialized view:{} to invalid",
                                baseTableInfo.getTableName(), mv.getId());
                        mv.setActive(false);
                        continue;
                    }
                    if (mv.isActive() && table instanceof MaterializedView && !((MaterializedView) table).isActive()) {
                        LOG.warn("tableName :{} is invalid. set materialized view:{} to invalid",
                                baseTableInfo.getTableName(), mv.getId());
                        mv.setActive(false);
                        continue;
                    }
                    MvId mvId = new MvId(db.getId(), mv.getId());
                    table.addRelatedMaterializedView(mvId);
                }
            }
        }
    }
}
