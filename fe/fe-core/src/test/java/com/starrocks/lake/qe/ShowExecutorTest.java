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

package com.starrocks.lake.qe;

import com.google.common.collect.Lists;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.common.DdlException;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.lake.LakeTable;
import com.starrocks.lake.TabletRepairHelper;
import com.starrocks.mysql.MysqlCommand;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ShowExecutor;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.sql.ast.AdminShowTabletStatusStmt;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.sql.ast.LakeTabletStatus;
import com.starrocks.sql.ast.QualifiedName;
import com.starrocks.sql.ast.ShowCreateDbStmt;
import com.starrocks.sql.ast.TableRef;
import com.starrocks.sql.ast.expression.BinaryPredicate;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.sql.ast.expression.StringLiteral;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.utframe.UtFrameUtils;
import com.starrocks.warehouse.cngroup.ComputeResource;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

public class ShowExecutorTest {
    private static ConnectContext ctx;

    private GlobalStateMgr globalStateMgr;

    @BeforeAll
    public static void beforeClass() throws Exception {
        ctx = UtFrameUtils.createDefaultCtx();
    }

    @BeforeEach
    public void setUp() throws Exception {
        ctx = new ConnectContext(null);
        ctx.setCommand(MysqlCommand.COM_SLEEP);

        new MockUp<RunMode>() {
            @Mock
            public RunMode getCurrentRunMode() {
                return RunMode.SHARED_DATA;
            }
        };

        // mock internal database.
        Database db = new Database();
        new Expectations(db) {
            {
                db.getFullName();
                minTimes = 0;
                result = "testDb";
            }};

        // mock external database.
        Database db1 = new Database();
        new Expectations(db1) {
            {
                db1.getFullName();
                minTimes = 0;
                result = "testDb1";

                db1.getCatalogName();
                minTimes = 0;
                result = "catalog";
            }};

        // mock globalStateMgr.
        globalStateMgr = Deencapsulation.newInstance(GlobalStateMgr.class);
        new Expectations(globalStateMgr) {
            {
                globalStateMgr.getLocalMetastore().getDb("testDb");
                minTimes = 0;
                result = db;

                globalStateMgr.getLocalMetastore().getDb("testDb1");
                minTimes = 0;
                result = db1;
            }
        };
    }

    @Test
    public void testShowCreateDb() throws DdlException {
        ctx.setGlobalStateMgr(globalStateMgr);
        ctx.setQualifiedUser("testUser");

        ShowCreateDbStmt stmt = new ShowCreateDbStmt("testDb");
        ShowResultSet resultSet = ShowExecutor.execute(stmt, ctx);
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals("testDb", resultSet.getString(0));
        Assertions.assertEquals(resultSet.getString(1), "CREATE DATABASE `testDb`\n" +
                        "PROPERTIES (\"storage_volume\" = \"builtin_storage_volume\")");
        Assertions.assertFalse(resultSet.next());

        stmt = new ShowCreateDbStmt("testDb1");
        resultSet = ShowExecutor.execute(stmt, ctx);
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals("testDb1", resultSet.getString(0));
        Assertions.assertEquals("CREATE DATABASE `testDb1`", resultSet.getString(1));
        Assertions.assertFalse(resultSet.next());
    }

    @Test
    public void testAdminShowTabletStatusStmt() throws Exception {
        ctx.setGlobalStateMgr(globalStateMgr);
        ctx.setQualifiedUser("testUser");

        Database db = globalStateMgr.getLocalMetastore().getDb("testDb");
        new Expectations(db) {
            {
                db.getTable("testTbl");
                minTimes = 0;
                result = new LakeTable(1L, "testTbl", Lists.newArrayList(), KeysType.DUP_KEYS, null, null);
            }
        };

        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                result = globalStateMgr;
            }
        };

        TableRef tableRef = new TableRef(QualifiedName.of(Lists.newArrayList("testDb", "testTbl")), null, NodePosition.ZERO);
        BinaryPredicate where = new BinaryPredicate(BinaryType.EQ, new SlotRef(null, "status"), new StringLiteral("NORMAL"));
        AdminShowTabletStatusStmt stmt =
                new AdminShowTabletStatusStmt(tableRef, where, Collections.emptyMap(), NodePosition.ZERO);

        new MockUp<TabletRepairHelper>() {
            @Mock
            public List<List<String>> getTabletStatus(Database db, OlapTable table, List<String> partitionNames,
                                                      LakeTabletStatus statusFilter, BinaryType op, int maxMissingDataFilesToShow,
                                                      ComputeResource computeResource) {
                List<List<String>> results = Lists.newArrayList();
                results.add(Lists.newArrayList("1", "2", "10", "NORMAL", "0", "[]"));
                return results;
            }
        };

        ShowResultSet resultSet = ShowExecutor.execute(stmt, ctx);
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals("1", resultSet.getString(0));
        Assertions.assertEquals("2", resultSet.getString(1));
        Assertions.assertEquals("10", resultSet.getString(2));
        Assertions.assertEquals("NORMAL", resultSet.getString(3));
        Assertions.assertEquals("0", resultSet.getString(4));
        Assertions.assertEquals("[]", resultSet.getString(5));
        Assertions.assertFalse(resultSet.next());
    }
}
