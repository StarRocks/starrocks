// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.sql.analyzer;

import com.starrocks.epack.sql.ast.AlterFailoverGroupAddStmt;
import com.starrocks.epack.sql.ast.AlterFailoverGroupPrimaryStmt;
import com.starrocks.epack.sql.ast.AlterFailoverGroupRefreshStmt;
import com.starrocks.epack.sql.ast.AlterFailoverGroupRemoveStmt;
import com.starrocks.epack.sql.ast.AlterFailoverGroupResumeStmt;
import com.starrocks.epack.sql.ast.AlterFailoverGroupSetStmt;
import com.starrocks.epack.sql.ast.AlterFailoverGroupSuspendStmt;
import com.starrocks.epack.sql.ast.AlterPolicyStmt;
import com.starrocks.epack.sql.ast.AlterSecurityIntegrationStatement;
import com.starrocks.epack.sql.ast.CreatePolicyStmt;
import com.starrocks.epack.sql.ast.CreatePrimaryFailoverGroupStmt;
import com.starrocks.epack.sql.ast.CreateRoleMappingStatement;
import com.starrocks.epack.sql.ast.CreateSecondaryFailoverGroupStmt;
import com.starrocks.epack.sql.ast.CreateSecurityIntegrationStatement;
import com.starrocks.epack.sql.ast.CreateWarehouseStmt;
import com.starrocks.epack.sql.ast.DescribeFailoverGroupStmt;
import com.starrocks.epack.sql.ast.DropFailoverGroupStmt;
import com.starrocks.epack.sql.ast.DropPolicyStmt;
import com.starrocks.epack.sql.ast.DropRoleMappingStatement;
import com.starrocks.epack.sql.ast.DropSecurityIntegrationStatement;
import com.starrocks.epack.sql.ast.DropWarehouseStmt;
import com.starrocks.epack.sql.ast.ResumeWarehouseStmt;
import com.starrocks.epack.sql.ast.SetWarehouseStmt;
import com.starrocks.epack.sql.ast.ShowClustersStmt;
import com.starrocks.epack.sql.ast.ShowCreatePolicyStmt;
import com.starrocks.epack.sql.ast.ShowCreateSecurityIntegrationStatement;
import com.starrocks.epack.sql.ast.ShowFailoverGroupsStmt;
import com.starrocks.epack.sql.ast.ShowPolicyStmt;
import com.starrocks.epack.sql.ast.ShowWarehousesStmt;
import com.starrocks.epack.sql.ast.SuspendWarehouseStmt;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.AnalyzerVisitor;

public class AnalyzerVisitorEPack extends AnalyzerVisitor {

    // ---------------------------------------- Security Policy Statement ------------------------------------------

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

    // ---------------------------------------- Security Integration Statement -------------------------------------

    @Override
    public Void visitCreateSecurityIntegrationStatement(CreateSecurityIntegrationStatement statement,
                                                        ConnectContext context) {
        SecurityIntegrationStatementAnalyzer.analyze(statement, context);
        return null;
    }

    @Override
    public Void visitAlterSecurityIntegrationStatement(AlterSecurityIntegrationStatement statement,
                                                       ConnectContext context) {
        SecurityIntegrationStatementAnalyzer.analyze(statement, context);
        return null;
    }

    @Override
    public Void visitDropSecurityIntegrationStatement(DropSecurityIntegrationStatement statement,
                                                      ConnectContext context) {
        SecurityIntegrationStatementAnalyzer.analyze(statement, context);
        return null;
    }

    @Override
    public Void visitShowCreateSecurityIntegrationStatement(ShowCreateSecurityIntegrationStatement statement,
                                                            ConnectContext context) {
        SecurityIntegrationStatementAnalyzer.analyze(statement, context);
        return null;
    }

    @Override
    public Void visitCreateRoleMappingStatement(CreateRoleMappingStatement statement, ConnectContext context) {
        RoleMappingStatementAnalyzer.analyze(statement, context);
        return null;
    }

    @Override
    public Void visitDropRoleMappingStatement(DropRoleMappingStatement statement,
                                              ConnectContext context) {
        RoleMappingStatementAnalyzer.analyze(statement, context);
        return null;
    }

    // ---------------------------------------- Warehouse Statement ---------------------------------------------------

    @Override
    public Void visitCreateWarehouseStatement(CreateWarehouseStmt statement, ConnectContext context) {
        WarehouseAnalyzer.analyze(statement, context);
        return null;
    }

    @Override
    public Void visitSuspendWarehouseStatement(SuspendWarehouseStmt statement, ConnectContext context) {
        WarehouseAnalyzer.analyze(statement, context);
        return null;
    }

    @Override
    public Void visitResumeWarehouseStatement(ResumeWarehouseStmt statement, ConnectContext context) {
        WarehouseAnalyzer.analyze(statement, context);
        return null;
    }

    @Override
    public Void visitDropWarehouseStatement(DropWarehouseStmt statement, ConnectContext context) {
        WarehouseAnalyzer.analyze(statement, context);
        return null;
    }

    @Override
    public Void visitSetWarehouseStatement(SetWarehouseStmt stmt, ConnectContext session) {
        WarehouseAnalyzer.analyze(stmt, session);
        return null;
    }

    @Override
    public Void visitShowWarehousesStatement(ShowWarehousesStmt stmt, ConnectContext context) {
        WarehouseAnalyzer.analyze(stmt, context);
        return null;
    }

    @Override
    public Void visitShowClusterStatement(ShowClustersStmt stmt, ConnectContext context) {
        WarehouseAnalyzer.analyze(stmt, context);
        return null;
    }

    // ---------------------------------------- Failover Group Statement --------------------------------------------

    @Override
    public Void visitCreatePrimaryFailoverGroupStatement(CreatePrimaryFailoverGroupStmt statement, ConnectContext context) {
        FailoverGroupAnalyzer.analyze(statement, context);
        return null;
    }

    @Override
    public Void visitCreateSecondaryFailoverGroupStatement(CreateSecondaryFailoverGroupStmt statement, ConnectContext context) {
        FailoverGroupAnalyzer.analyze(statement, context);
        return null;
    }

    @Override
    public Void visitDropFailoverGroupStatement(DropFailoverGroupStmt statement, ConnectContext context) {
        FailoverGroupAnalyzer.analyze(statement, context);
        return null;
    }

    @Override
    public Void visitShowFailoverGroupsStatement(ShowFailoverGroupsStmt statement, ConnectContext context) {
        FailoverGroupAnalyzer.analyze(statement, context);
        return null;
    }

    @Override
    public Void visitDescribeFailoverGroupStatement(DescribeFailoverGroupStmt statement, ConnectContext context) {
        FailoverGroupAnalyzer.analyze(statement, context);
        return null;
    }

    @Override
    public Void visitAlterFailoverGroupSetStatement(AlterFailoverGroupSetStmt statement, ConnectContext context) {
        FailoverGroupAnalyzer.analyze(statement, context);
        return null;
    }

    @Override
    public Void visitAlterFailoverGroupAddStatement(AlterFailoverGroupAddStmt statement, ConnectContext context) {
        FailoverGroupAnalyzer.analyze(statement, context);
        return null;
    }

    @Override
    public Void visitAlterFailoverGroupRemoveStatement(AlterFailoverGroupRemoveStmt statement, ConnectContext context) {
        FailoverGroupAnalyzer.analyze(statement, context);
        return null;
    }

    @Override
    public Void visitAlterFailoverGroupRefreshStatement(AlterFailoverGroupRefreshStmt statement, ConnectContext context) {
        FailoverGroupAnalyzer.analyze(statement, context);
        return null;
    }

    @Override
    public Void visitAlterFailoverGroupPrimaryStatement(AlterFailoverGroupPrimaryStmt statement, ConnectContext context) {
        FailoverGroupAnalyzer.analyze(statement, context);
        return null;
    }

    @Override
    public Void visitAlterFailoverGroupSuspendStatement(AlterFailoverGroupSuspendStmt statement, ConnectContext context) {
        FailoverGroupAnalyzer.analyze(statement, context);
        return null;
    }

    @Override
    public Void visitAlterFailoverGroupResumeStatement(AlterFailoverGroupResumeStmt statement, ConnectContext context) {
        FailoverGroupAnalyzer.analyze(statement, context);
        return null;
    }
}