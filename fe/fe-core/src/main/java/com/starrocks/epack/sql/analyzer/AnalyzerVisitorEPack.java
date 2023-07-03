// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.sql.analyzer;

import com.starrocks.epack.sql.ast.AlterPolicyStmt;
import com.starrocks.epack.sql.ast.CreatePolicyStmt;
import com.starrocks.epack.sql.ast.DropPolicyStmt;
import com.starrocks.epack.sql.ast.ShowCreatePolicyStmt;
import com.starrocks.epack.sql.ast.ShowPolicyStmt;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.AnalyzerVisitor;

public class AnalyzerVisitorEPack extends AnalyzerVisitor {
    // ---------------------------------------- Security Policy Statement ---------------------------------------------------

    @Override
    public Void visitCreatePolicyStatement(CreatePolicyStmt stmt, ConnectContext context) {
        SecurityPolicyAnalyzer.analyze(stmt, context);
        return null;
    }

    @Override
    public Void visitDropPolicyStatement(DropPolicyStmt stmt, ConnectContext context) {
        SecurityPolicyAnalyzer.analyze(stmt, context);
        return null;
    }

    @Override
    public Void visitAlterPolicyStatement(AlterPolicyStmt stmt, ConnectContext context) {
        SecurityPolicyAnalyzer.analyze(stmt, context);
        return null;
    }

    @Override
    public Void visitShowPolicyStatement(ShowPolicyStmt stmt, ConnectContext context) {
        SecurityPolicyAnalyzer.analyze(stmt, context);
        return null;
    }

    @Override
    public Void visitShowCreatePolicyStatement(ShowCreatePolicyStmt stmt, ConnectContext context) {
        SecurityPolicyAnalyzer.analyze(stmt, context);
        return null;
    }
}