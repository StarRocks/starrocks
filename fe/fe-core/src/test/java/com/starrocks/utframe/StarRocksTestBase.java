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

package com.starrocks.utframe;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.server.GlobalStateMgr;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections4.ListUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.util.List;
import java.util.Set;

/**
 * Basic test class for StarRocks.
 */
public abstract class StarRocksTestBase {
    protected static final Logger LOG = LogManager.getLogger(StarRocksTestBase.class);

<<<<<<< HEAD
=======
    // Whether print log to system out
    protected static boolean isOutputSystemOut = false;
    // Whether output trace log by default
    protected static boolean isOutputTraceLog = false;

>>>>>>> 0a08eaa5c4 ([UT] Output trace logs when fe ut fails (#64408))
    // StarRocksAssert is a class that provides methods to interact with StarRocks.
    public static StarRocksAssert starRocksAssert;
    // existedTables is a set that contains all tables that have been created.
    protected static Set<Table> existedTables = Sets.newHashSet();

    @BeforeEach
    public void before() {
        if (starRocksAssert != null) {
            collectTables(starRocksAssert, existedTables);
        }
    }

    @AfterEach
    public void after() throws Exception {
        if (starRocksAssert != null) {
            try {
                cleanup(starRocksAssert, existedTables);
            } catch (Exception e) {
                // ignore exception
            }
        }
    }

    public static void collectTables(StarRocksAssert starRocksAssert, Set<Table> tables) {
        Preconditions.checkArgument(starRocksAssert != null, "StarRocksAssert is null");
        String currentDb = starRocksAssert.getCtx().getDatabase();
        if (StringUtils.isNotEmpty(currentDb)) {
            Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(currentDb);
            if (testDb != null) {
                tables.addAll(ListUtils.emptyIfNull(testDb.getTables()));
            }
        }
    }

    public static void cleanup(StarRocksAssert starRocksAssert, Set<Table> existedTables) throws Exception {
        Preconditions.checkArgument(starRocksAssert != null, "StarRocksAssert is null");
        String currentDb = starRocksAssert.getCtx().getDatabase();
        if (StringUtils.isNotEmpty(currentDb)) {
            Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(currentDb);
            List<Table> tables = ListUtils.emptyIfNull(testDb.getTables());
            for (Table table : tables) {
                if (!table.isNativeTableOrMaterializedView()) {
                    continue;
                }
                if (!existedTables.contains(table)) {
                    if (table.isNativeTable()) {
                        starRocksAssert.dropTable(table.getName());
                    } else {
                        starRocksAssert.dropMaterializedView(table.getName());
                    }
                    LOG.warn("cleanup table after test case: {}", table.getName());
                }
            }
            if (CollectionUtils.isNotEmpty(testDb.getMaterializedViews())) {
                LOG.warn("database [{}] still contains {} materialized views",
                        testDb.getFullName(), testDb.getMaterializedViews().size());
            }
        }
    }
}
