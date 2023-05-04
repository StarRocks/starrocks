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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/catalog/InfoSchemaDbTest.java

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

package com.starrocks.catalog;

import com.starrocks.catalog.system.info.InfoSchemaDb;
import com.starrocks.catalog.system.starrocks.GrantsTo;
import com.starrocks.privilege.AuthorizationManager;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.CreateRoleStmt;
import com.starrocks.sql.ast.CreateUserStmt;
import com.starrocks.sql.ast.GrantPrivilegeStmt;
import com.starrocks.sql.ast.RevokePrivilegeStmt;
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.thrift.TGetGrantsToRolesOrUserItem;
import com.starrocks.thrift.TGetGrantsToRolesOrUserRequest;
import com.starrocks.thrift.TGetGrantsToRolesOrUserResponse;
import com.starrocks.thrift.TGrantsToType;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class InfoSchemaDbTest {
    ConnectContext ctx;
    GlobalStateMgr globalStateMgr;
    AuthorizationManager authorizationManager;

    @Before
    public void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();

        ctx = UtFrameUtils.initCtxForNewPrivilege(UserIdentity.ROOT);
        StarRocksAssert starRocksAssert = new StarRocksAssert(ctx);
        globalStateMgr = starRocksAssert.getCtx().getGlobalStateMgr();
        globalStateMgr.getAuthorizationManager().initBuiltinRolesAndUsers();

        authorizationManager = globalStateMgr.getAuthorizationManager();

        starRocksAssert.withDatabase("db");
        String createTblStmtStr = "(k1 varchar(32), k2 varchar(32), k3 varchar(32), k4 int) "
                + "AGGREGATE KEY(k1, k2,k3,k4) distributed by hash(k1) buckets 3 properties('replication_num' = '1');";
        starRocksAssert.withTable("create table db.tbl " + createTblStmtStr);
        starRocksAssert.withView("create view db.v as select * from db.tbl");

        CreateUserStmt createUserStmt = (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(
                "create user test_user", ctx);
        globalStateMgr.getAuthenticationManager().createUser(createUserStmt);
        createUserStmt = (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(
                "create user test_user2", ctx);
        globalStateMgr.getAuthenticationManager().createUser(createUserStmt);

        CreateRoleStmt createRoleStmt = (CreateRoleStmt) UtFrameUtils.parseStmtWithNewParser(
                "create role test_role", ctx);
        globalStateMgr.getAuthorizationManager().createRole(createRoleStmt);
    }

    @Test
    public void testNormal() throws IOException {
        Database db = new InfoSchemaDb();

        Assert.assertFalse(db.createTable(null));
        Assert.assertFalse(db.createMaterializedWithLock(null, false));
        Assert.assertFalse(db.createTableWithLock(null, false));
        db.dropTable("authors");
        db.dropTableWithLock("authors");
        db.write(null);
        Assert.assertNull(db.getTable("authors"));
    }

    @Test
    public void testInitRole() throws Exception {
        TGetGrantsToRolesOrUserRequest request = new TGetGrantsToRolesOrUserRequest();
        request.setType(TGrantsToType.ROLE);
        TGetGrantsToRolesOrUserResponse response = GrantsTo.getGrantsTo(request);
        System.out.println(response);

        TGetGrantsToRolesOrUserItem item = new TGetGrantsToRolesOrUserItem();
        item.setGrantee("root");
        item.setObject_catalog("default_catalog");
        item.setObject_database("db");
        item.setObject_type("DATABASE");
        item.setPrivilege_type("DROP");
        item.setIs_grantable(false);
        Assert.assertTrue(response.grants_to.contains(item));

        item = new TGetGrantsToRolesOrUserItem();
        item.setGrantee("root");
        item.setObject_type("SYSTEM");
        item.setPrivilege_type("CREATE GLOBAL FUNCTION");
        item.setIs_grantable(false);
        Assert.assertTrue(response.grants_to.contains(item));

        item = new TGetGrantsToRolesOrUserItem();
        item.setGrantee("root");
        item.setObject_catalog("default_catalog");
        item.setObject_database("db");
        item.setObject_name("v");
        item.setObject_type("VIEW");
        item.setPrivilege_type("DROP");
        item.setIs_grantable(false);
        Assert.assertTrue(response.grants_to.contains(item));
    }

    @Test
    public void testGrantUserOnDB() throws Exception {
        String sql = "grant DROP on database db to test_user";
        GrantPrivilegeStmt grantStmt = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        authorizationManager.grant(grantStmt);

        TGetGrantsToRolesOrUserRequest request = new TGetGrantsToRolesOrUserRequest();
        request.setType(TGrantsToType.USER);

        TGetGrantsToRolesOrUserItem item = new TGetGrantsToRolesOrUserItem();
        item.setGrantee("'test_user'@'%'");
        item.setObject_catalog("default_catalog");
        item.setObject_database("db");
        item.setObject_type("DATABASE");
        item.setPrivilege_type("DROP");
        item.setIs_grantable(false);
        Assert.assertTrue(GrantsTo.getGrantsTo(request).grants_to.contains(item));

        sql = "revoke DROP on database db from test_user";
        RevokePrivilegeStmt revokePrivilegeStmt = (RevokePrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        authorizationManager.revoke(revokePrivilegeStmt);
        Assert.assertFalse(GrantsTo.getGrantsTo(request).grants_to.contains(item));

        sql = "grant drop on all databases to test_user";
        grantStmt = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        authorizationManager.grant(grantStmt);
        item = new TGetGrantsToRolesOrUserItem();
        item.setGrantee("'test_user'@'%'");
        item.setObject_catalog("default_catalog");
        item.setObject_database("db");
        item.setObject_type("DATABASE");
        item.setPrivilege_type("DROP");
        item.setIs_grantable(false);
        Assert.assertTrue(GrantsTo.getGrantsTo(request).grants_to.contains(item));

        sql = "revoke DROP on all databases from test_user";
        revokePrivilegeStmt = (RevokePrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        authorizationManager.revoke(revokePrivilegeStmt);
        Assert.assertFalse(GrantsTo.getGrantsTo(request).grants_to.contains(item));
    }

    @Test
    public void testGrantUserOnTable() throws Exception {
        String sql = "grant select on Table db.tbl to test_user";
        GrantPrivilegeStmt grantStmt = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        authorizationManager.grant(grantStmt);

        TGetGrantsToRolesOrUserRequest request = new TGetGrantsToRolesOrUserRequest();
        request.setType(TGrantsToType.USER);

        TGetGrantsToRolesOrUserItem item = new TGetGrantsToRolesOrUserItem();
        item.setGrantee("'test_user'@'%'");
        item.setObject_catalog("default_catalog");
        item.setObject_database("db");
        item.setObject_name("tbl");
        item.setObject_type("TABLE");
        item.setPrivilege_type("SELECT");
        item.setIs_grantable(false);
        Assert.assertTrue(GrantsTo.getGrantsTo(request).grants_to.contains(item));

        sql = "revoke select on db.tbl from test_user";
        RevokePrivilegeStmt revokePrivilegeStmt = (RevokePrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        authorizationManager.revoke(revokePrivilegeStmt);
        Assert.assertFalse(GrantsTo.getGrantsTo(request).grants_to.contains(item));
    }

    @Test
    public void testGrantUserOnView() throws Exception {
        String sql = "grant select on view db.v to test_user";
        GrantPrivilegeStmt grantStmt = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        authorizationManager.grant(grantStmt);

        TGetGrantsToRolesOrUserRequest request = new TGetGrantsToRolesOrUserRequest();
        request.setType(TGrantsToType.USER);

        TGetGrantsToRolesOrUserItem item = new TGetGrantsToRolesOrUserItem();
        item.setGrantee("'test_user'@'%'");
        item.setObject_catalog("default_catalog");
        item.setObject_database("db");
        item.setObject_name("v");
        item.setObject_type("VIEW");
        item.setPrivilege_type("SELECT");
        item.setIs_grantable(false);
        Assert.assertTrue(GrantsTo.getGrantsTo(request).grants_to.contains(item));

        sql = "revoke select on view db.v from test_user";
        RevokePrivilegeStmt revokePrivilegeStmt = (RevokePrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        authorizationManager.revoke(revokePrivilegeStmt);
        Assert.assertFalse(GrantsTo.getGrantsTo(request).grants_to.contains(item));

        sql = "grant drop on all views in database db to test_user";
        grantStmt = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        authorizationManager.grant(grantStmt);
        item = new TGetGrantsToRolesOrUserItem();
        item.setGrantee("'test_user'@'%'");
        item.setObject_catalog("default_catalog");
        item.setObject_database("db");
        item.setObject_name("v");
        item.setObject_type("VIEW");
        item.setPrivilege_type("DROP");
        item.setIs_grantable(false);
        Assert.assertTrue(GrantsTo.getGrantsTo(request).grants_to.contains(item));

        sql = "revoke DROP on all views in database db from test_user";
        revokePrivilegeStmt = (RevokePrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        authorizationManager.revoke(revokePrivilegeStmt);
        Assert.assertFalse(GrantsTo.getGrantsTo(request).grants_to.contains(item));
    }

    @Test
    public void testGrantUserOnUser() throws Exception {
        String sql = "grant impersonate on user test_user2 to test_user";
        GrantPrivilegeStmt grantStmt = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        authorizationManager.grant(grantStmt);

        TGetGrantsToRolesOrUserRequest request = new TGetGrantsToRolesOrUserRequest();
        request.setType(TGrantsToType.USER);

        TGetGrantsToRolesOrUserItem item = new TGetGrantsToRolesOrUserItem();
        item.setGrantee("'test_user'@'%'");
        item.setObject_name("'test_user2'@'%'");
        item.setObject_type("USER");
        item.setPrivilege_type("IMPERSONATE");
        item.setIs_grantable(false);
        Assert.assertTrue(GrantsTo.getGrantsTo(request).grants_to.contains(item));

        sql = "revoke impersonate on user test_user2 from test_user";
        RevokePrivilegeStmt revokePrivilegeStmt = (RevokePrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        authorizationManager.revoke(revokePrivilegeStmt);
        Assert.assertFalse(GrantsTo.getGrantsTo(request).grants_to.contains(item));
    }
}