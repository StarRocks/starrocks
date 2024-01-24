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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/analysis/AccessTestUtil.java

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

package com.starrocks.analysis;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedIndex.IndexState;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.RandomDistributionInfo;
import com.starrocks.catalog.SinglePartitionInfo;
import com.starrocks.catalog.Type;
import com.starrocks.common.DdlException;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.journal.JournalTask;
import com.starrocks.mysql.privilege.Auth;
import com.starrocks.mysql.privilege.PrivPredicate;
import com.starrocks.persist.EditLog;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LocalMetastore;
import com.starrocks.sql.ast.SetPassVar;
import com.starrocks.system.SystemInfoService;
import com.starrocks.thrift.TStorageType;
import mockit.Expectations;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class AccessTestUtil {

    public static SystemInfoService fetchSystemInfoService() {
        return new SystemInfoService();
    }

    public static Auth fetchAdminAccess() {
        Auth auth = new Auth();
        try {
            new Expectations(auth) {
                {
                    auth.checkGlobalPriv((ConnectContext) any, (PrivPredicate) any);
                    minTimes = 0;
                    result = true;

                    auth.checkDbPriv((ConnectContext) any, anyString, (PrivPredicate) any);
                    minTimes = 0;
                    result = true;

                    auth.checkTblPriv((ConnectContext) any, anyString, anyString, (PrivPredicate) any);
                    minTimes = 0;
                    result = true;

                    auth.setPassword((SetPassVar) any);
                    minTimes = 0;
                }
            };
        } catch (DdlException e) {
            e.printStackTrace();
        }
        return auth;
    }

    public static GlobalStateMgr fetchAdminCatalog() {
        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        BlockingQueue<JournalTask> journalQueue = new ArrayBlockingQueue<JournalTask>(100);
        EditLog editLog = new EditLog(journalQueue);
        globalStateMgr.setEditLog(editLog);

        Database db = new Database(50000L, "testCluster:testDb");
        MaterializedIndex baseIndex = new MaterializedIndex(30001, IndexState.NORMAL);
        RandomDistributionInfo distributionInfo = new RandomDistributionInfo(10);
        Partition partition = new Partition(20000L, "testTbl", baseIndex, distributionInfo);
        List<Column> baseSchema = new LinkedList<Column>();
        Column column = new Column();
        baseSchema.add(column);
        OlapTable table = new OlapTable(30000, "testTbl", baseSchema,
                KeysType.AGG_KEYS, new SinglePartitionInfo(), distributionInfo, globalStateMgr.getNodeMgr().getClusterId(),
                null);
        table.setIndexMeta(baseIndex.getId(), "testTbl", baseSchema, 0, 1, (short) 1,
                TStorageType.COLUMN, KeysType.AGG_KEYS);
        table.addPartition(partition);
        table.setBaseIndexId(baseIndex.getId());
        db.registerTableUnlocked(table);
        return globalStateMgr;
    }

    public static Auth fetchBlockAccess() {
        Auth auth = new Auth();
        new Expectations(auth) {
            {
                auth.checkGlobalPriv((ConnectContext) any, (PrivPredicate) any);
                minTimes = 0;
                result = false;

                auth.checkDbPriv((ConnectContext) any, anyString, (PrivPredicate) any);
                minTimes = 0;
                result = false;

                auth.checkTblPriv((ConnectContext) any, anyString, anyString, (PrivPredicate) any);
                minTimes = 0;
                result = false;
            }
        };
        return auth;
    }

    public static OlapTable mockTable(String name) {
        Column column1 = new Column("col1", Type.BIGINT);
        Column column2 = new Column("col2", Type.DOUBLE);

        MaterializedIndex index = new MaterializedIndex();
        new Expectations(index) {
            {
                index.getId();
                minTimes = 0;
                result = 30000L;
            }
        };

        Partition partition = Deencapsulation.newInstance(Partition.class);
        new Expectations(partition) {
            {
                partition.getBaseIndex();
                minTimes = 0;
                result = index;

                partition.getIndex(30000L);
                minTimes = 0;
                result = index;
            }
        };

        OlapTable table = new OlapTable();
        new Expectations(table) {
            {
                table.getBaseSchema();
                minTimes = 0;
                result = Lists.newArrayList(column1, column2);

                table.getPartition(40000L);
                minTimes = 0;
                result = partition;
            }
        };
        return table;
    }

    public static Database mockDb(String name) {
        Database db = new Database();
        OlapTable olapTable = mockTable("testTable");

        new Expectations(db) {
            {
                db.getTable("testTable");
                minTimes = 0;
                result = olapTable;

                db.getTable("emptyTable");
                minTimes = 0;
                result = null;

                db.getTableNamesViewWithLock();
                minTimes = 0;
                result = Sets.newHashSet("testTable");

                db.getTables();
                minTimes = 0;
                result = Lists.newArrayList(olapTable);

                db.readLock();
                minTimes = 0;

                db.readUnlock();
                minTimes = 0;

                db.getFullName();
                minTimes = 0;
                result = name;
            }
        };
        return db;
    }

    public static GlobalStateMgr fetchBlockCatalog() {
        try {
            GlobalStateMgr globalStateMgr = Deencapsulation.newInstance(GlobalStateMgr.class);

            Auth auth = fetchBlockAccess();
            Database db = mockDb("testDb");

            new Expectations(globalStateMgr) {
                {
                    globalStateMgr.getAuth();
                    minTimes = 0;
                    result = auth;

                    globalStateMgr.changeCatalogDb((ConnectContext) any, anyString);
                    minTimes = 0;
                    result = new DdlException("failed");

                    globalStateMgr.getDb("testDb");
                    minTimes = 0;
                    result = db;

                    globalStateMgr.getDb("emptyDb");
                    minTimes = 0;
                    result = null;

                    globalStateMgr.getDb(anyString);
                    minTimes = 0;
                    result = new Database();

                    globalStateMgr.getDb("emptyCluster");
                    minTimes = 0;
                    result = null;
                }
            };
            return globalStateMgr;
        } catch (DdlException e) {
            return null;
        }
    }

    public static Analyzer fetchAdminAnalyzer() {
        Analyzer analyzer = new Analyzer(fetchAdminCatalog(), new ConnectContext(null));
        new Expectations(analyzer) {
            {
                analyzer.getDefaultCatalog();
                minTimes = 0;
                result = InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME;

                analyzer.getDefaultDb();
                minTimes = 0;
                result = "testDb";

                analyzer.incrementCallDepth();
                minTimes = 0;
                result = 1;

                analyzer.decrementCallDepth();
                minTimes = 0;
                result = 0;

                analyzer.getCallDepth();
                minTimes = 0;
                result = 1;
            }
        };
        return analyzer;
    }

    public static Analyzer fetchTableAnalyzer() {
        Column column1 = new Column("k1", Type.VARCHAR);
        Column column2 = new Column("k2", Type.VARCHAR);
        Column column3 = new Column("k3", Type.VARCHAR);
        Column column4 = new Column("k4", Type.BIGINT);

        MaterializedIndex index = new MaterializedIndex();
        new Expectations(index) {
            {
                index.getId();
                minTimes = 0;
                result = 30000L;
            }
        };

        Partition partition = Deencapsulation.newInstance(Partition.class);
        new Expectations(partition) {
            {
                partition.getBaseIndex();
                minTimes = 0;
                result = index;

                partition.getIndex(30000L);
                minTimes = 0;
                result = index;
            }
        };

        OlapTable table = new OlapTable();
        new Expectations(table) {
            {
                table.getBaseSchema();
                minTimes = 0;
                result = Lists.newArrayList(column1, column2, column3, column4);

                table.getPartition(40000L);
                minTimes = 0;
                result = partition;

                table.getColumn("k1");
                minTimes = 0;
                result = column1;

                table.getColumn("k2");
                minTimes = 0;
                result = column2;

                table.getColumn("k3");
                minTimes = 0;
                result = column3;

                table.getColumn("k4");
                minTimes = 0;
                result = column4;
            }
        };

        Database db = new Database();

        new Expectations(db) {
            {
                db.getTable("t");
                minTimes = 0;
                result = table;

                db.getTable("emptyTable");
                minTimes = 0;
                result = null;

                db.getTableNamesViewWithLock();
                minTimes = 0;
                result = Sets.newHashSet("t");

                db.getTables();
                minTimes = 0;
                result = Lists.newArrayList(table);

                db.readLock();
                minTimes = 0;

                db.readUnlock();
                minTimes = 0;

                db.getFullName();
                minTimes = 0;
                result = "testDb";
            }
        };
        GlobalStateMgr globalStateMgr = fetchBlockCatalog();
        Analyzer analyzer = new Analyzer(globalStateMgr, new ConnectContext(null));
        new Expectations(analyzer) {
            {
                analyzer.getDefaultDb();
                minTimes = 0;
                result = "testDb";

                analyzer.getTable((TableName) any);
                minTimes = 0;
                result = table;

                analyzer.getCatalog();
                minTimes = 0;
                result = globalStateMgr;

                analyzer.incrementCallDepth();
                minTimes = 0;
                result = 1;

                analyzer.decrementCallDepth();
                minTimes = 0;
                result = 0;

                analyzer.getCallDepth();
                minTimes = 0;
                result = 1;

                analyzer.getContext();
                minTimes = 0;
                result = new ConnectContext(null);

            }
        };
        return analyzer;
    }
}

