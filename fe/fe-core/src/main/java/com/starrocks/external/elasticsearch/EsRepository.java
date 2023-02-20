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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/external/elasticsearch/EsRepository.java

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

package com.starrocks.external.elasticsearch;

import com.google.common.collect.Maps;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.EsTable;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Table.TableType;
import com.starrocks.common.Config;
import com.starrocks.common.util.LeaderDaemon;
import com.starrocks.server.GlobalStateMgr;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;

/**
 * It is responsible for loading all ES external table's meta-data such as `fields`, `partitions` periodically,
 * playing the `repo` role at StarRocks On ES
 */
public class EsRepository extends LeaderDaemon {

    private static final Logger LOG = LogManager.getLogger(EsRepository.class);

    private Map<Long, EsTable> esTables;

    private Map<Long, EsRestClient> esClients;

    public EsRepository() {
        super("es repository", Config.es_state_sync_interval_second * 1000);
        esTables = Maps.newConcurrentMap();
        esClients = Maps.newConcurrentMap();
    }

    public void registerTable(EsTable esTable) {
        if (GlobalStateMgr.isCheckpointThread()) {
            return;
        }
        // We should edit esClients first, otherwise will occur thread safety issues.
        // In runAfterCatalogReady(), we read esTables first,  If the client in esClients has
        // not been added at this time, it will result NullPointerException.
        esClients.put(esTable.getId(),
                new EsRestClient(esTable.getSeeds(), esTable.getUserName(), esTable.getPasswd(), esTable.sslEnabled()));
        esTables.put(esTable.getId(), esTable);
        LOG.info(String.format("Thread %s: register a new table [%s] to sync list",
                Thread.currentThread().getName(), esTable));
    }

    public void deRegisterTable(long tableId) {
        // When do doRegister, remove esTables first.
        esTables.remove(tableId);
        esClients.remove(tableId);
        LOG.info(String.format("Thread %s: deregister table [%s] from sync list",
                Thread.currentThread().getName(), tableId));
    }

    @Override
    protected void runAfterCatalogReady() {
        for (EsTable esTable : esTables.values()) {
            EsRestClient esClient = esClients.get(esTable.getId());
            if (esClient == null) {
                LOG.warn(String.format("EsTable[%s] existed, but EsClient not existed now, need retry.", esTable));
                continue;
            }
            try {
                esTable.syncTableMetaData(esClient);
                // After synchronize success, we should set LastMetaDataSyncException to null.
                esTable.setLastMetaDataSyncException(null);
            } catch (Exception e) {
                LOG.warn(String.format("Thread %s: Exception happens when fetch index [%s] meta " +
                                "data from remote es cluster. Table info: [%s]",
                        Thread.currentThread().getName(), esTable.getName(), esTable), e);
                esTable.setEsTablePartitions(null);
                esTable.setLastMetaDataSyncException(e);
            }
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
                if (table.getType() == TableType.ELASTICSEARCH) {
                    registerTable((EsTable) table);
                }
            }
        }
    }
}
