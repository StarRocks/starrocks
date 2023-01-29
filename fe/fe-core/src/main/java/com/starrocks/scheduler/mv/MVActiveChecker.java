// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

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

        long startMillis = System.currentTimeMillis();
        for (String dbName : dbNames) {
            Database db = GlobalStateMgr.getCurrentState().getMetadataMgr().getDb(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                    dbName);
            for (MaterializedView mv : db.getMaterializedViews()) {
                for (MaterializedView.BaseTableInfo baseTableInfo : mv.getBaseTableInfos()) {
                    Table table = baseTableInfo.getTable();
                    if (table == null) {
                        LOG.warn("tableName :{} do not exist. set materialized view:{} to invalid",
                                baseTableInfo.getTableName(), mv.getId());
                        mv.setActive(false);
                        continue;
                    }
                    if (table instanceof MaterializedView && !((MaterializedView) table).isActive()) {
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

        long duration = System.currentTimeMillis() - startMillis;
        LOG.info("[MVActiveChecker] finish check all materialized view in {}ms", duration);
    }
}
