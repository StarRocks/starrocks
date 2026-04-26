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

package com.starrocks.warehouse;

import com.starrocks.authentication.AuthenticationMgr;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.DDLStmtExecutor;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.ast.CreateUserStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Optional;

public class UtilsTest {
    private static StarRocksAssert starRocksAssert;
    private static ConnectContext ctx;

    @BeforeAll
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        ctx = UtFrameUtils.initCtxForNewPrivilege(UserIdentity.ROOT);
        starRocksAssert = new StarRocksAssert(ctx);
        starRocksAssert.getCtx().setRemoteIP("localhost");
        starRocksAssert.getCtx().getGlobalStateMgr().getAuthorizationMgr().initBuiltinRolesAndUsers();
    }

    private static void createUser(String createUserSql) throws Exception {
        CreateUserStmt createUserStmt =
                (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(createUserSql, starRocksAssert.getCtx());
        AuthenticationMgr authenticationManager =
                starRocksAssert.getCtx().getGlobalStateMgr().getAuthenticationMgr();
        authenticationManager.createUser(createUserStmt);
    }

    @Test
    public void testGetUserDefaultWarehouse() throws Exception {
        Assertions.assertEquals(Optional.empty(), Utils.getUserDefaultWarehouse(null));
        Assertions.assertEquals(Optional.empty(), Utils.getUserDefaultWarehouse(new UserIdentity("", "%")));
        Assertions.assertEquals(Optional.empty(), Utils.getUserDefaultWarehouse(new UserIdentity("non_exist_user", "%")));
        Assertions.assertEquals(Optional.empty(), Utils.getUserDefaultWarehouse(new UserIdentity(true, "ephemeral_user", "%")));

        // Test user without warehouse
        String userName = "test_user_no_wh";
        createUser("CREATE USER '" + userName + "' IDENTIFIED BY ''");
        Assertions.assertEquals(Optional.empty(), Utils.getUserDefaultWarehouse(new UserIdentity(userName, "%")));

        // Test user with warehouse
        String userWithWh = "test_user_with_wh";
        createUser("CREATE USER '" + userWithWh + "' IDENTIFIED BY ''");
        String setPropSql = "ALTER USER '" + userWithWh + "' SET PROPERTIES ('session." + SessionVariable.WAREHOUSE_NAME +
                "' = 'default_warehouse')";
        StatementBase stmt = UtFrameUtils.parseStmtWithNewParser(setPropSql, ctx);
        DDLStmtExecutor.execute(stmt, ctx);

        Optional<String> wh = Utils.getUserDefaultWarehouse(new UserIdentity(userWithWh, "%"));
        Assertions.assertTrue(wh.isPresent());
        Assertions.assertEquals("default_warehouse", wh.get());
    }
}
