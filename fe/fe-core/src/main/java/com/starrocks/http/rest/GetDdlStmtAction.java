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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/http/rest/GetDdlStmtAction.java

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

package com.starrocks.http.rest;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.common.DdlException;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.http.ActionController;
import com.starrocks.http.BaseRequest;
import com.starrocks.http.BaseResponse;
import com.starrocks.http.IllegalArgException;
import com.starrocks.privilege.AccessDeniedException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.AstToStringBuilder;
import com.starrocks.sql.ast.UserIdentity;
import io.netty.handler.codec.http.HttpMethod;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;

/*
 * used to get a table's ddl stmt
 * eg:
 *  fe_host:http_port/api/_get_ddl?db=xxx&tbl=yyy
 */
public class GetDdlStmtAction extends RestBaseAction {
    private static final Logger LOG = LogManager.getLogger(GetDdlStmtAction.class);
    private static final String DB_PARAM = "db";
    private static final String TABLE_PARAM = "tbl";

    public GetDdlStmtAction(ActionController controller) {
        super(controller);
    }

    public static void registerAction(ActionController controller) throws IllegalArgException {
        GetDdlStmtAction action = new GetDdlStmtAction(controller);
        controller.registerHandler(HttpMethod.GET, "/api/_get_ddl", action);
    }

    @Override
    public void executeWithoutPassword(BaseRequest request, BaseResponse response)
            throws DdlException, AccessDeniedException {
        UserIdentity currentUser = ConnectContext.get().getCurrentUserIdentity();
        checkUserOwnsAdminRole(currentUser);

        String dbName = request.getSingleParameter(DB_PARAM);
        String tableName = request.getSingleParameter(TABLE_PARAM);

        if (Strings.isNullOrEmpty(dbName) || Strings.isNullOrEmpty(tableName)) {
            throw new DdlException("Missing params. Need database name and Table name");
        }

        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbName);
        if (db == null) {
            throw new DdlException("Database[" + dbName + "] does not exist");
        }

        List<String> createTableStmt = Lists.newArrayList();
        List<String> addPartitionStmt = Lists.newArrayList();
        List<String> createRollupStmt = Lists.newArrayList();

        Locker locker = new Locker();
        locker.lockDatabase(db.getId(), LockType.READ);
        try {
            Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), tableName);
            if (table == null) {
                throw new DdlException("Table[" + tableName + "] does not exist");
            }

            AstToStringBuilder.getDdlStmt(table, createTableStmt, addPartitionStmt, createRollupStmt, true,
                    false /* show password */);

        } finally {
            locker.unLockDatabase(db.getId(), LockType.READ);
        }

        Map<String, List<String>> results = Maps.newHashMap();
        results.put("TABLE", createTableStmt);
        results.put("PARTITION", addPartitionStmt);
        results.put("ROLLUP", createRollupStmt);

        // to json response
        sendResultByJson(request, response, results);
    }
}