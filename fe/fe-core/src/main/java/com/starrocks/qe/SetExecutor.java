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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/qe/SetExecutor.java

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

package com.starrocks.qe;

import com.starrocks.authentication.UserAuthenticationInfo;
import com.starrocks.common.DdlException;
import com.starrocks.mysql.privilege.AuthPlugin;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.SetStmtAnalyzer;
import com.starrocks.sql.ast.SetListItem;
import com.starrocks.sql.ast.SetPassVar;
import com.starrocks.sql.ast.SetStmt;
import com.starrocks.sql.ast.SystemVariable;
import com.starrocks.sql.ast.UserVariable;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

// Set executor
public class SetExecutor {
    private final ConnectContext ctx;
    private final SetStmt stmt;

    public SetExecutor(ConnectContext ctx, SetStmt stmt) {
        this.ctx = ctx;
        this.stmt = stmt;
    }

    private void setVariablesOfAllType(SetListItem var) throws DdlException {
        if (var instanceof SystemVariable) {
            ctx.modifySystemVariable((SystemVariable) var, false);
        } else if (var instanceof UserVariable) {
            UserVariable userVariable = (UserVariable) var;
            SetStmtAnalyzer.calcuteUserVariable(userVariable);

            if (userVariable.getEvaluatedExpression() == null) {
                userVariable.deriveUserVariableExpressionResult(ctx);
            }

            ctx.modifyUserVariableCopyInWrite(userVariable);
        } else if (var instanceof SetPassVar) {
            // Set password
            SetPassVar setPassVar = (SetPassVar) var;
            UserAuthenticationInfo userAuthenticationInfo = GlobalStateMgr.getCurrentState()
                    .getAuthenticationMgr()
                    .getUserAuthenticationInfoByUserIdentity(setPassVar.getUserIdent());
            if (null == userAuthenticationInfo) {
                throw new DdlException("authentication info for user " + setPassVar.getUserIdent() + " not found");
            }
            if (!userAuthenticationInfo.getAuthPlugin().equals(AuthPlugin.Server.MYSQL_NATIVE_PASSWORD.name())) {
                throw new DdlException("only allow set password for native user, current user: " +
                        setPassVar.getUserIdent() + ", AuthPlugin: " + userAuthenticationInfo.getAuthPlugin());
            }
            userAuthenticationInfo.setPassword(setPassVar.getPassword());
            GlobalStateMgr.getCurrentState().getAuthenticationMgr()
                    .alterUser(setPassVar.getUserIdent(), userAuthenticationInfo, null);
        }
    }

    /**
     * SetExecutor will set the session variables and password
     *
     * @throws DdlException
     */
    public void execute() throws DdlException {
        Map<String, UserVariable> clonedUserVars = new ConcurrentHashMap<>();
        boolean hasUserVar = stmt.getSetListItems().stream().anyMatch(var -> var instanceof UserVariable);
        boolean executeSuccess = true;
        if (hasUserVar) {
            clonedUserVars.putAll(ctx.getUserVariables());
            ctx.modifyUserVariablesCopyInWrite(clonedUserVars);
        }
        try {
            for (SetListItem var : stmt.getSetListItems()) {
                setVariablesOfAllType(var);
            }
        } catch (Throwable e) {
            if (hasUserVar) {
                executeSuccess = false;
            }
            throw e;
        } finally {
            //If the set sql contains more than one user variable,
            //the atomicity of the modification of this set of variables must be ensured.
            if (hasUserVar) {
                ctx.resetUserVariableCopyInWrite();
                if (executeSuccess) {
                    ctx.modifyUserVariables(clonedUserVars);
                }
            }
        }
    }
}
