// This file is made available under Elastic License 2.0.
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

import com.google.common.collect.Lists;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.analysis.SetNamesVar;
import com.starrocks.analysis.SetPassVar;
import com.starrocks.analysis.SetStmt;
import com.starrocks.analysis.SetTransaction;
import com.starrocks.analysis.SetType;
import com.starrocks.analysis.SetVar;
import com.starrocks.analysis.StatementBase;
import com.starrocks.analysis.Subquery;
import com.starrocks.catalog.Database;
import com.starrocks.common.DdlException;
import com.starrocks.common.Pair;
import com.starrocks.common.Status;
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.analyzer.AnalyzerUtils;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.SetUserDefineVar;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.Optimizer;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.PhysicalPropertySet;
import com.starrocks.sql.optimizer.transformer.LogicalPlan;
import com.starrocks.sql.optimizer.transformer.RelationTransformer;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.sql.plan.PlanFragmentBuilder;
import com.starrocks.statistic.StatisticUtils;
import com.starrocks.thrift.TResultBatch;
import com.starrocks.thrift.TVariableData;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

// Set executor
public class SetExecutor {
    private static final Logger LOG = LogManager.getLogger(SetExecutor.class);

    private ConnectContext ctx;
    private SetStmt stmt;

    public SetExecutor(ConnectContext ctx, SetStmt stmt) {
        this.ctx = ctx;
        this.stmt = stmt;
    }

    private void setVariablesOfAllType(SetVar var) throws DdlException {
        if (var instanceof SetPassVar) {
            // Set password
            SetPassVar setPassVar = (SetPassVar) var;
            ctx.getGlobalStateMgr().getAuth().setPassword(setPassVar);
        } else if (var instanceof SetNamesVar) {
            // do nothing
            return;
        } else if (var instanceof SetTransaction) {
            // do nothing
            return;
        } else {
            if (var.getType().equals(SetType.USER)) {
                SetUserDefineVar setUserDefineVar = (SetUserDefineVar) var;
                if (setUserDefineVar.getResolvedExpression() == null) {
                    ConnectContext context = StatisticUtils.buildConnectContext();
                    Map<String, Database> dbs;
                    Expr expr = setUserDefineVar.getExpression();
                    StatementBase parsedStmt = ((Subquery) expr).getQueryStatement();
                    dbs = AnalyzerUtils.collectAllDatabase(context, parsedStmt);

                    try {
                        ExecPlan execPlan = getExecutePlan(dbs, context, parsedStmt, true, true);
                        List<TResultBatch> sqlResult = executeStmt(context, execPlan).first;
                        List<TVariableData> result = deserializerStatisticData(sqlResult);
                        LiteralExpr literalExpr = LiteralExpr.create(result.get(0).result,
                                execPlan.getOutputExprs().get(0).getType());
                        setUserDefineVar.setResolvedExpression(literalExpr);
                    } catch (Exception e) {
                        throw new SemanticException(e.getMessage());
                    }

                    if (context.getState().getStateType() == QueryState.MysqlStateType.ERR) {
                        throw new SemanticException(context.getState().getErrorMessage());
                    }
                }

                ctx.modifySessionVariable(setUserDefineVar, false);
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


    // Lock all database before analyze
    private static void lock(Map<String, Database> dbs) {
        if (dbs == null) {
            return;
        }
        for (Database db : dbs.values()) {
            db.readLock();
        }
    }

    // unLock all database after analyze
    private static void unLock(Map<String, Database> dbs) {
        if (dbs == null) {
            return;
        }
        for (Database db : dbs.values()) {
            db.readUnlock();
        }
    }

    private static ExecPlan getExecutePlan(Map<String, Database> dbs, ConnectContext context,
                                           StatementBase parsedStmt, boolean isStatistic, boolean isLockDb) {
        ExecPlan execPlan;
        try {
            if (isLockDb) {
                lock(dbs);
            }

            Analyzer.analyze(parsedStmt, context);

            ColumnRefFactory columnRefFactory = new ColumnRefFactory();
            LogicalPlan logicalPlan = new RelationTransformer(columnRefFactory, context).transform(
                    ((QueryStatement) parsedStmt).getQueryRelation());

            Optimizer optimizer = new Optimizer();
            OptExpression optimizedPlan = optimizer.optimize(
                    context,
                    logicalPlan.getRoot(),
                    new PhysicalPropertySet(),
                    new ColumnRefSet(logicalPlan.getOutputColumn()),
                    columnRefFactory);

            execPlan = new PlanFragmentBuilder()
                    .createVariablePhysicalPlan(optimizedPlan, context, logicalPlan.getOutputColumn(),
                            columnRefFactory, isStatistic);
        } finally {
            if (isLockDb) {
                unLock(dbs);
            }
        }
        return execPlan;
    }

    private static Pair<List<TResultBatch>, Status> executeStmt(ConnectContext context, ExecPlan plan) throws Exception {
        Coordinator coord = new Coordinator(context, plan.getFragments(), plan.getScanNodes(), plan.getDescTbl().toThrift());
        QeProcessorImpl.INSTANCE.registerQuery(context.getExecutionId(), coord);
        List<TResultBatch> sqlResult = Lists.newArrayList();
        try {
            coord.exec();
            RowBatch batch;
            do {
                batch = coord.getNext();
                if (batch.getBatch() != null) {
                    sqlResult.add(batch.getBatch());
                }
            } while (!batch.isEos());
        } finally {
            QeProcessorImpl.INSTANCE.unregisterQuery(context.getExecutionId());
        }
        return Pair.create(sqlResult, coord.getExecStatus());
    }

    private static List<TVariableData> deserializerStatisticData(List<TResultBatch> sqlResult) throws TException {
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
