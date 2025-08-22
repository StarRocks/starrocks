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

package com.starrocks.connector;

import com.starrocks.analysis.ParseNode;
import com.starrocks.analysis.TableName;
import com.starrocks.common.DdlException;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.epack.sql.ast.ApplyMaskingPolicyClause;
import com.starrocks.epack.sql.ast.ApplyRowAccessPolicyClause;
import com.starrocks.epack.sql.ast.AstVisitorEPack;
import com.starrocks.epack.sql.ast.RevokeMaskingPolicyClause;
import com.starrocks.epack.sql.ast.RevokeRowAccessPolicyClause;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.AlterClause;
import com.starrocks.sql.ast.AlterTableStmt;

import java.util.ArrayList;
import java.util.List;

public class ConnectorAlterTableExecutor implements AstVisitorEPack<Void, ConnectContext> {
    protected AlterTableStmt stmt;
    protected final TableName tableName;
    protected List<Runnable> actions;
    ConnectContext context;

    public ConnectorAlterTableExecutor(AlterTableStmt stmt, ConnectContext context) {
        this.stmt = stmt;
        tableName = stmt.getTbl();
        actions = new ArrayList<>();
        this.context = context;
    }

    public void applyClauses() throws DdlException {
        List<AlterClause> alterClauses = stmt.getAlterClauseList();
        try {
            for (AlterClause c : alterClauses) {
                visit(c, context);
            }
        } catch (StarRocksConnectorException e) {
            throw new DdlException(e.getMessage(), e.getCause());
        }
    }

    public void execute() throws DdlException {
        applyClauses();
    }

    @Override
    public Void visit(ParseNode node, ConnectContext context) {
        node.accept(this, context);
        for (Runnable r : actions) {
            r.run();
        }
        return null;
    }

    @Override
    public Void visitNode(ParseNode node, ConnectContext context) {
        throw new StarRocksConnectorException(
                "This connector doesn't support alter table: " + tableName + " with operation: " + node.toString());
    }

    @Override
    public Void visitApplyMaskingPolicyClause(ApplyMaskingPolicyClause clause, ConnectContext context) {
        actions.add(() -> {
            ApplyMaskingPolicyClause applyMaskingPolicyClause = (ApplyMaskingPolicyClause) clause;
            GlobalStateMgr.getCurrentState().getSecurityPolicyManager().applyMaskingPolicyContext(
                    context,
                    tableName,
                    applyMaskingPolicyClause.getMaskingColumn(),
                    applyMaskingPolicyClause.getWithColumnMaskingPolicy());
        });
        return null;
    }

    @Override
    public Void visitRevokeMaskingPolicyClause(RevokeMaskingPolicyClause clause, ConnectContext context) {
        actions.add(() -> {
            RevokeMaskingPolicyClause revokeMaskingPolicyClause = (RevokeMaskingPolicyClause) clause;
            GlobalStateMgr.getCurrentState().getSecurityPolicyManager().revokeMaskingPolicyContext(
                    context,
                    tableName.getCatalog(), tableName.getDb(), tableName.getTbl(),
                    revokeMaskingPolicyClause.getMaskingColumn());
        });
        return null;
    }

    @Override
    public Void visitApplyRowAccessPolicyClause(ApplyRowAccessPolicyClause clause, ConnectContext context) {
        actions.add(() -> {
            ApplyRowAccessPolicyClause modifyRowAccessPolicyClause = (ApplyRowAccessPolicyClause) clause;
            GlobalStateMgr.getCurrentState().getSecurityPolicyManager().applyRowAccessPolicyContext(
                    context,
                    tableName, modifyRowAccessPolicyClause.getRowAccessPolicyContext());
        });
        return null;
    }

    @Override
    public Void visitRevokeRowAccessPolicyClause(RevokeRowAccessPolicyClause clause, ConnectContext context) {
        actions.add(() -> {
            if (clause.isRevokeAll()) {
                GlobalStateMgr.getCurrentState().getSecurityPolicyManager().revokeALLRowAccessPolicyContext(
                        context,
                        tableName.getCatalog(), tableName.getDb(), tableName.getTbl());

            } else {
                GlobalStateMgr.getCurrentState().getSecurityPolicyManager().revokeRowAccessPolicyContext(
                        context,
                        tableName.getCatalog(), tableName.getDb(), tableName.getTbl(),
                        clause.getPolicyName());
            }
        });
        return null;
    }
}
