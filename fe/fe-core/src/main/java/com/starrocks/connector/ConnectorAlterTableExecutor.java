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

import com.starrocks.alter.AlterOperations;
import com.starrocks.analysis.ParseNode;
import com.starrocks.analysis.TableName;
import com.starrocks.common.DdlException;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.AlterClause;
import com.starrocks.sql.ast.AlterTableStmt;
import com.starrocks.sql.ast.AstVisitor;

import java.util.ArrayList;
import java.util.List;

public class ConnectorAlterTableExecutor extends AstVisitor<Void, ConnectContext> {
    protected AlterTableStmt stmt;
    protected final TableName tableName;
    protected List<Runnable> actions;

    public ConnectorAlterTableExecutor(AlterTableStmt stmt) {
        this.stmt = stmt;
        tableName = stmt.getTbl();
        actions = new ArrayList<>();
    }

    public void checkConflict() throws DdlException {
        List<AlterClause> alterClauses = stmt.getOps();
        AlterOperations currentAlterOps = new AlterOperations();
        currentAlterOps.checkConflict(alterClauses);
    }

    public void applyClauses() throws DdlException {
        List<AlterClause> alterClauses = stmt.getOps();
        try {
            for (AlterClause c : alterClauses) {
                visit(c, null);
            }
        } catch (StarRocksConnectorException e) {
            throw new DdlException(e.getMessage(), e.getCause());
        }
    }

    public void execute() throws DdlException {
        checkConflict();
        applyClauses();
    }

    @Override
    public Void visit(ParseNode node, ConnectContext context) {
        super.visit(node, context);
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
}
