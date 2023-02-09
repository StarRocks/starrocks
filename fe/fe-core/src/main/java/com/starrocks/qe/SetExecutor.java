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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.analysis.NullLiteral;
import com.starrocks.analysis.Subquery;
import com.starrocks.authentication.UserAuthenticationInfo;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.DdlException;
import com.starrocks.common.Pair;
import com.starrocks.common.Status;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.StatementPlanner;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.SetNamesVar;
import com.starrocks.sql.ast.SetPassVar;
import com.starrocks.sql.ast.SetStmt;
import com.starrocks.sql.ast.SetTransaction;
import com.starrocks.sql.ast.SetType;
import com.starrocks.sql.ast.SetVar;
import com.starrocks.sql.ast.UserVariable;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.thrift.TResultBatch;
import com.starrocks.thrift.TResultSinkType;
import com.starrocks.thrift.TVariableData;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;

// Set executor
public class SetExecutor {
    private final ConnectContext ctx;
    private final SetStmt stmt;

    public SetExecutor(ConnectContext ctx, SetStmt stmt) {
        this.ctx = ctx;
        this.stmt = stmt;
    }

    private void setVariablesOfAllType(SetVar var) throws DdlException {
        if (var instanceof SetPassVar) {
            // Set password
            SetPassVar setPassVar = (SetPassVar) var;
            if (GlobalStateMgr.getCurrentState().isUsingNewPrivilege()) {
                UserAuthenticationInfo userAuthenticationInfo = GlobalStateMgr.getCurrentState()
                        .getAuthenticationManager()
                        .getUserAuthenticationInfoByUserIdentity(setPassVar.getUserIdent());
                if (null == userAuthenticationInfo) {
                    throw new DdlException("authentication info for user "
                            + setPassVar.getUserIdent() + " not found");
                }
                userAuthenticationInfo.setPassword(setPassVar.getPassword());
                GlobalStateMgr.getCurrentState().getAuthenticationManager().
                        updateUserWithAuthenticationInfo(
                                setPassVar.getUserIdent(), userAuthenticationInfo);
            } else {
                ctx.getGlobalStateMgr().getAuth().setPassword(setPassVar);
            }
        } else if (var instanceof SetNamesVar) {
            // do nothing
        } else if (var instanceof SetTransaction) {
            // do nothing
        } else {
            if (var.getType().equals(SetType.USER)) {
                UserVariable userVariable = (UserVariable) var;
                if (userVariable.getResolvedExpression() == null) {
                    deriveExpressionResult(userVariable);
                }

                ctx.modifyUserVariable(userVariable);
            } else {
                ctx.modifySessionVariable(var, false);
            }
        }
    }

    private boolean isSessionVar(SetVar var) {
        return !(var instanceof SetPassVar
                || var instanceof SetNamesVar
                || var instanceof SetTransaction);
    }

    /**
     * SetExecutor will set the session variables and password
     *
     * @throws DdlException
     */
    public void execute() throws DdlException {
        for (SetVar var : stmt.getSetVars()) {
            setVariablesOfAllType(var);
        }
    }

    /**
     * This method is only called after a set statement is forward to the leader.
     * In this case, the follower should change this session variable as well.
     */
    public void setSessionVars() throws DdlException {
        for (SetVar var : stmt.getSetVars()) {
            if (isSessionVar(var)) {
                VariableMgr.setVar(ctx.getSessionVariable(), var, true);
            }
        }
    }

    private void deriveExpressionResult(UserVariable userVariable) {
        QueryStatement queryStatement = ((Subquery) userVariable.getExpression()).getQueryStatement();
        ExecPlan execPlan = StatementPlanner.plan(queryStatement,
                ConnectContext.get(), TResultSinkType.VARIABLE);
        StmtExecutor executor = new StmtExecutor(ctx, queryStatement);
        Pair<List<TResultBatch>, Status> sqlResult = executor.executeStmtWithExecPlan(ctx, execPlan);
        if (!sqlResult.second.ok()) {
            throw new SemanticException(sqlResult.second.getErrorMsg());
        } else {
            try {
                List<TVariableData> result = deserializerVariableData(sqlResult.first);
                LiteralExpr resultExpr;
                if (result.isEmpty()) {
                    resultExpr = new NullLiteral();
                } else {
                    Preconditions.checkState(result.size() == 1);
                    if (result.get(0).isIsNull()) {
                        resultExpr = new NullLiteral();
                    } else {
                        Type userVariableType = userVariable.getExpression().getType();
                        //JSON type will be stored as string type
                        if (userVariableType.isJsonType()) {
                            userVariableType = Type.VARCHAR;
                        }
                        resultExpr = LiteralExpr.create(
                                StandardCharsets.UTF_8.decode(result.get(0).result).toString(), userVariableType);
                    }
                }
                userVariable.setResolvedExpression(resultExpr);
            } catch (TException | AnalysisException e) {
                throw new SemanticException(e.getMessage());
            }
        }

        if (ctx.getState().getStateType() == QueryState.MysqlStateType.ERR) {
            throw new SemanticException(ctx.getState().getErrorMessage());
        }
    }

    private static List<TVariableData> deserializerVariableData(List<TResultBatch> sqlResult) throws TException {
        List<TVariableData> statistics = Lists.newArrayList();

        TDeserializer deserializer = new TDeserializer(new TCompactProtocol.Factory());
        for (TResultBatch resultBatch : sqlResult) {
            for (ByteBuffer bb : resultBatch.rows) {
                TVariableData sd = new TVariableData();
                byte[] bytes = new byte[bb.limit() - bb.position()];
                bb.get(bytes);
                deserializer.deserialize(sd, bytes);
                statistics.add(sd);
            }
        }

        return statistics;
    }
}
