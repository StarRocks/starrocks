// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.external.starrocks;

import com.google.common.collect.Maps;
import com.starrocks.catalog.Catalog;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.ExternalOlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Table.TableType;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.common.util.MasterDaemon;
import com.starrocks.meta.MetaContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;


/**
 * It is responsible for loading all StarRocks OLAP external table's meta-data periodically
 */
public class StarRocksRepository extends MasterDaemon {
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
        metaContext.setMetaVersion(FeConstants.meta_version);
        metaContext.setStarRocksMetaVersion(FeConstants.starrocks_meta_version);
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
        if (Catalog.isCheckpointThread()) {
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
        if (!Catalog.getCurrentCatalog().isMaster()) {
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
        if (Catalog.isCheckpointThread()) {
            return;
        }

        List<Long> dbIds = Catalog.getCurrentCatalog().getDbIds();
        for (Long dbId : dbIds) {
            Database database = Catalog.getCurrentCatalog().getDb(dbId);

            List<Table> tables = database.getTables();
            for (Table table : tables) {
                if (table.getType() == TableType.OLAP_EXTERNAL) {
                    ExternalOlapTable olapTable = (ExternalOlapTable) table;
                    LOG.info("load external olap table {} from catalog", table.getName());
                    registerTable(olapTable);
                }
            }
        }
    }
}