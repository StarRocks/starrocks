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


package com.starrocks.external.starrocks;

import com.google.common.collect.Maps;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.ExternalOlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Table.TableType;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.common.util.LeaderDaemon;
import com.starrocks.meta.MetaContext;
import com.starrocks.server.GlobalStateMgr;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;

/**
 * It is responsible for loading all StarRocks OLAP external table's meta-data periodically
 */
public class StarRocksRepository extends LeaderDaemon {
    private static final Logger LOG = LogManager.getLogger(StarRocksRepository.class);

    private Map<Long, ExternalOlapTable> srTables;

    private TableMetaSyncer metaSyncer;

    private boolean inited;

    public StarRocksRepository() {
        super("star rocks repository", Config.es_state_sync_interval_second * 1000);
        srTables = Maps.newConcurrentMap();
        metaSyncer = new TableMetaSyncer();
        inited = false;
    }

    public void init() {
        MetaContext metaContext = new MetaContext();
        metaContext.setMetaVersion(FeConstants.META_VERSION);
        metaContext.setStarRocksMetaVersion(FeConstants.STARROCKS_META_VERSION);
        metaContext.setThreadLocalInfo();
        inited = true;
    }

    @Override
    protected final void runOneCycle() {
        if (!inited) {
            init();
        }
        super.runOneCycle();
    }

    public void registerTable(ExternalOlapTable srTable) {
        if (GlobalStateMgr.isCheckpointThread()) {
            return;
        }
        srTables.put(srTable.getId(), srTable);
        LOG.info("register a new olap table [{}] to sync list", srTable);
    }

    public void deRegisterTable(ExternalOlapTable srTable) {
        srTables.remove(srTable.getId());
        LOG.info("deregister table [{}] from sync list", srTable.getId());
    }

    @Override
    protected void runAfterCatalogReady() {
        if (!GlobalStateMgr.getCurrentState().isLeader()) {
            return;
        }
        for (ExternalOlapTable table : srTables.values()) {
            metaSyncer.syncTable(table);
        }
    }

    // should call this method to init the state store after loading image
    // the rest of tables will be added or removed by replaying edit log
    // when fe is start to load image, should call this method to init the state store
    public void loadTableFromCatalog() {
        if (GlobalStateMgr.isCheckpointThread()) {
            return;
        }

        List<Long> dbIds = GlobalStateMgr.getCurrentState().getDbIds();
        for (Long dbId : dbIds) {
            Database database = GlobalStateMgr.getCurrentState().getDb(dbId);

            List<Table> tables = database.getTables();
            for (Table table : tables) {
                if (table.getType() == TableType.OLAP_EXTERNAL) {
                    ExternalOlapTable olapTable = (ExternalOlapTable) table;
                    LOG.info("load external olap table {} from globalStateMgr", table.getName());
                    registerTable(olapTable);
                }
            }
        }
    }
}