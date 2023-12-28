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
package com.starrocks.qe;

import com.google.common.collect.Sets;
import com.starrocks.privilege.PrivilegeBuiltinConstants;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.ShowTableStmt;
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ShowTablesTest {
    private ConnectContext ctx;
    @Before
    public void setUp() throws Exception {
        ctx = UtFrameUtils.initCtxForNewPrivilege(UserIdentity.ROOT);
        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        ShowTableMockMeta metadataMgr =
                new ShowTableMockMeta(globalStateMgr.getLocalMetastore(), globalStateMgr.getConnectorMgr());
        metadataMgr.init();
        globalStateMgr.setMetadataMgr(metadataMgr);

        ctx.setCurrentUserIdentity(UserIdentity.ROOT);
        ctx.setCurrentRoleIds(Sets.newHashSet(PrivilegeBuiltinConstants.ROOT_ROLE_ID));
    }

    @Test
    public void testShowTable() throws Exception {
        ctx = UtFrameUtils.initCtxForNewPrivilege(UserIdentity.ROOT);
        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        ShowTableMockMeta metadataMgr =
                new ShowTableMockMeta(globalStateMgr.getLocalMetastore(), globalStateMgr.getConnectorMgr());
        metadataMgr.init();
        globalStateMgr.setMetadataMgr(metadataMgr);

        ctx.setCurrentUserIdentity(UserIdentity.ROOT);
        ctx.setCurrentRoleIds(Sets.newHashSet(PrivilegeBuiltinConstants.ROOT_ROLE_ID));

        ShowTableStmt stmt = new ShowTableStmt("testDb", false, null);
        ShowExecutor executor = new ShowExecutor(ctx, stmt);
        ShowResultSet resultSet = executor.execute();

        Assert.assertTrue(resultSet.next());
        Assert.assertEquals("hive_test", resultSet.getString(0));
        Assert.assertTrue(resultSet.next());
        Assert.assertEquals("testMv", resultSet.getString(0));
        Assert.assertTrue(resultSet.next());
        Assert.assertEquals("testTbl", resultSet.getString(0));
        Assert.assertFalse(resultSet.next());
    }

    @Test
    public void testShowTableVerbose() throws Exception {
        ShowTableStmt stmt = new ShowTableStmt("testDb", true, null);
        ShowExecutor executor = new ShowExecutor(ctx, stmt);
        ShowResultSet resultSet = executor.execute();

        Assert.assertTrue(resultSet.next());
        Assert.assertEquals("hive_test", resultSet.getString(0));
        Assert.assertEquals("BASE TABLE", resultSet.getString(1));
        Assert.assertTrue(resultSet.next());
        Assert.assertEquals("testMv", resultSet.getString(0));
        Assert.assertEquals("VIEW", resultSet.getString(1));
        Assert.assertTrue(resultSet.next());
        Assert.assertEquals("testTbl", resultSet.getString(0));
        Assert.assertEquals("BASE TABLE", resultSet.getString(1));
        Assert.assertFalse(resultSet.next());
    }
}
