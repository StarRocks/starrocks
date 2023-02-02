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

package com.starrocks.connector.hive;

import com.starrocks.catalog.Database;
import com.starrocks.catalog.HiveTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.common.util.LeaderDaemon;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.MetadataMgr;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.spark_project.guava.collect.Lists;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class BackgroundHiveMetadataProcessor extends LeaderDaemon {
    private static final Logger LOG = LogManager.getLogger(BackgroundHiveMetadataProcessor.class);

    public BackgroundHiveMetadataProcessor() {
        super(BackgroundHiveMetadataProcessor.class.getName(), Config.background_refresh_hive_metadata_interval_sec);
    }

    @Override
    protected void runAfterCatalogReady() {
        GlobalStateMgr gsm = GlobalStateMgr.getCurrentState();
        MetadataMgr metadataMgr = gsm.getMetadataMgr();
        List<Database> databases = gsm.getDbIds().stream()
                .map(gsm::getDb)
                .filter(Objects::nonNull)
                .filter(db -> !db.isInfoSchemaDb())
                .collect(Collectors.toList());

        for (Database db : databases) {
            List<HiveTable> tables = db.getTables().stream()
                    .filter(tbl -> tbl.getType() == Table.TableType.HIVE)
                    .map(tbl -> (HiveTable) tbl)
                    .collect(Collectors.toList());
            for (HiveTable table : tables) {
                try {
                    if (db.getTable(table.getId()) != null) {
                        metadataMgr.refreshTable(table.getCatalogName(), db.getFullName(),
                                table, Lists.newArrayList(), false);
                    }
                } catch (Exception e) {
                    LOG.error("background refresh hive metadata failed on {}.{}", db.getFullName(), table.getName(), e);
                }
            }
        }
    }
}
