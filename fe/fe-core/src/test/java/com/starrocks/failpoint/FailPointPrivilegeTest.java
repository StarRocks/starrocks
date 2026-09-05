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

package com.starrocks.failpoint;

import com.starrocks.authentication.AuthenticationMgr;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.common.ErrorReportException;
import com.starrocks.qe.DDLStmtExecutor;
import com.starrocks.sql.analyzer.Authorizer;
import com.starrocks.sql.ast.CreateUserStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.UserRef;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;

/**
 * Arming a failpoint injects faults into every targeted node, and WITH PAUSE can park node threads
 * outright, so both failpoint statements require the OPERATE system privilege. Before this was added
 * the ADMIN keyword was syntax only and any authenticated user could arm any failpoint.
 */
public class FailPointPrivilegeTest {
    private static StarRocksAssert starRocksAssert;
    private static UserIdentity userWithOperate;
    private static UserIdentity userWithoutOperate;

    private static final List<String> FAILPOINT_SQLS = List.of(
            "ADMIN ENABLE FAILPOINT 'fp' WITH PAUSE",
            "ADMIN DISABLE FAILPOINT 'fp'",
            "SHOW FAILPOINTS");

    @BeforeAll
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        starRocksAssert = new StarRocksAssert(UtFrameUtils.initCtxForNewPrivilege(UserIdentity.ROOT));
        AuthenticationMgr authenticationMgr = starRocksAssert.getCtx().getGlobalStateMgr().getAuthenticationMgr();

        userWithOperate = createUser(authenticationMgr, "fpOperateUser");
        starRocksAssert.getCtx().setCurrentUserIdentity(UserIdentity.ROOT);
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "grant operate on system to fpOperateUser", starRocksAssert.getCtx()), starRocksAssert.getCtx());

        userWithoutOperate = createUser(authenticationMgr, "fpPlainUser");
    }

    private static UserIdentity createUser(AuthenticationMgr authenticationMgr, String name) throws Exception {
        CreateUserStmt stmt = (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(
                "CREATE USER '" + name + "' IDENTIFIED BY ''", starRocksAssert.getCtx());
        authenticationMgr.createUser(stmt);
        UserRef user = stmt.getUser();
        return new UserIdentity(user.getUser(), user.getHost(), user.isDomain());
    }

    private static void ctxTo(UserIdentity user) {
        starRocksAssert.getCtx().setCurrentUserIdentity(user);
        starRocksAssert.getCtx().setQualifiedUser(user.getUser());
    }

    @Test
    public void testFailPointStatementsDeniedWithoutOperate() throws Exception {
        ctxTo(userWithoutOperate);
        for (String sql : FAILPOINT_SQLS) {
            StatementBase stmt = UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());
            // reportAccessDenied converts the AccessDeniedException into an ErrorReportException.
            Assertions.assertThrows(ErrorReportException.class,
                    () -> Authorizer.check(stmt, starRocksAssert.getCtx()),
                    "expected access denied for: " + sql);
        }
    }

    @Test
    public void testFailPointStatementsAllowedWithOperate() throws Exception {
        ctxTo(userWithOperate);
        for (String sql : FAILPOINT_SQLS) {
            StatementBase stmt = UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());
            Assertions.assertDoesNotThrow(() -> Authorizer.check(stmt, starRocksAssert.getCtx()),
                    "expected access granted for: " + sql);
        }
    }
}
