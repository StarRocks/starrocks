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
import com.starrocks.epack.sql.ast.AstVisitorEPack;
import com.starrocks.epack.sql.ast.CreatePasswordPolicyStmt;
import com.starrocks.epack.sql.ast.CreatePolicyStmt;
import com.starrocks.epack.sql.ast.CreatePrimaryFailoverGroupStmt;
import com.starrocks.epack.sql.ast.CreateRoleMappingStatement;
import com.starrocks.epack.sql.ast.CreateSecondaryFailoverGroupStmt;
import com.starrocks.epack.sql.ast.DescribeFailoverGroupStmt;
import com.starrocks.epack.sql.ast.DropFailoverGroupStmt;
import com.starrocks.epack.sql.ast.DropPasswordPolicyStmt;
import com.starrocks.epack.sql.ast.DropPolicyStmt;
import com.starrocks.epack.sql.ast.DropRoleMappingStatement;
import com.starrocks.epack.sql.ast.SetPasswordPolicyStmt;
import com.starrocks.epack.sql.ast.ShowCreatePasswordPolicyStmt;
import com.starrocks.epack.sql.ast.ShowCreatePolicyStmt;
import com.starrocks.epack.sql.ast.ShowFailoverGroupsStmt;
import com.starrocks.epack.sql.ast.ShowPasswordPolicyStmt;
import com.starrocks.epack.sql.ast.ShowPolicyStmt;
import com.starrocks.epack.sql.ast.UnsetPasswordPolicyStmt;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.analyzer.ClusterSnapshotAnalyzer;
import com.starrocks.sql.ast.BaseGrantRevokePrivilegeStmt;
import com.starrocks.sql.ast.BaseGrantRevokeRoleStmt;
import com.starrocks.sql.ast.CreateMaterializedViewStatement;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.sql.ast.CreateViewStmt;
import com.starrocks.sql.ast.RestoreTableFromSnapshotStmt;
import com.starrocks.sql.automv.analysis.TunespaceAnalyzer;
import com.starrocks.sql.automv.ast.AlterTunespaceStmt;
import com.starrocks.sql.automv.ast.CreateTunespaceStmt;
import com.starrocks.sql.automv.ast.ShowRecommendationsStmt;
import com.starrocks.sql.automv.ast.SubmitRecommendationsTaskStmt;

public class AnalyzerVisitorEPack extends Analyzer.AnalyzerVisitor implements AstVisitorEPack<Void, ConnectContext> {

    private static final AnalyzerVisitorEPack INSTANCE = new AnalyzerVisitorEPack();

    public static AnalyzerVisitorEPack getInstance() {
        return INSTANCE;
    }

    // ---------------------------------------- Database Statement -----------------------------------------------------

    @Override
    public Void visitCreateTableStatement(CreateTableStmt statement, ConnectContext context) {
        CreateTableAnalyzerEPack.analyze(statement, context);
        return null;
    }

    @Override
    public Void visitCreateViewStatement(CreateViewStmt statement, ConnectContext session) {
        ViewAnalyzerEPack.analyze(statement, session);
        return null;
    }

    @Override
    public Void visitCreateMaterializedViewStatement(CreateMaterializedViewStatement statement,
                                                     ConnectContext context) {
        MaterializedViewAnalyzerEPack.analyze(statement, context);
        return null;
    }

    // ---------------------------------------- Privilege Statement ------------------------------------------------

    @Override
    public Void visitGrantRevokePrivilegeStatement(BaseGrantRevokePrivilegeStmt statement, ConnectContext session) {
        AuthorizationAnalyzerEPack.analyze(statement, session);
        return null;
    }

    @Override
    public Void visitGrantRevokeRoleStatement(BaseGrantRevokeRoleStmt statement, ConnectContext session) {
        AuthorizationAnalyzerEPack.analyze(statement, session);
        return null;
    }

    // ---------------------------------------- Security Policy Statement ------------------------------------------

    @Override
    public Void visitCreatePolicyStatement(CreatePolicyStmt statement, ConnectContext context) {
        SecurityPolicyAnalyzer.analyze(statement, context);
        return null;
    }

    @Override
    public Void visitDropPolicyStatement(DropPolicyStmt statement, ConnectContext context) {
        SecurityPolicyAnalyzer.analyze(statement, context);
        return null;
    }

    @Override
    public Void visitAlterPolicyStatement(AlterPolicyStmt statement, ConnectContext context) {
        SecurityPolicyAnalyzer.analyze(statement, context);
        return null;
    }

    @Override
    public Void visitShowPolicyStatement(ShowPolicyStmt statement, ConnectContext context) {
        SecurityPolicyAnalyzer.analyze(statement, context);
        return null;
    }

    @Override
    public Void visitShowCreatePolicyStatement(ShowCreatePolicyStmt statement, ConnectContext context) {
        SecurityPolicyAnalyzer.analyze(statement, context);
        return null;
    }

    @Override
    public Void visitCreatePasswordPolicyStatement(CreatePasswordPolicyStmt statement, ConnectContext context) {
        SecurityPolicyAnalyzer.analyze(statement, context);
        return null;
    }

    @Override
    public Void visitDropPasswordPolicyStatement(DropPasswordPolicyStmt statement, ConnectContext context) {
        SecurityPolicyAnalyzer.analyze(statement, context);
        return null;
    }

    @Override
    public Void visitShowPasswordPolicyStatement(ShowPasswordPolicyStmt statement, ConnectContext context) {
        SecurityPolicyAnalyzer.analyze(statement, context);
        return null;
    }

    @Override
    public Void visitShowCreatePasswordPolicyStatement(ShowCreatePasswordPolicyStmt statement, ConnectContext context) {
        SecurityPolicyAnalyzer.analyze(statement, context);
        return null;
    }

    @Override
    public Void visitSetPasswordPolicyStatement(SetPasswordPolicyStmt statement, ConnectContext context) {
        SecurityPolicyAnalyzer.analyze(statement, context);
        return null;
    }

    @Override
    public Void visitUnsetPasswordPolicyStatement(UnsetPasswordPolicyStmt statement, ConnectContext context) {
        SecurityPolicyAnalyzer.analyze(statement, context);
        return null;
    }

    // ---------------------------------------- Security Integration Statement -------------------------------------

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

    // tunespace
    @Override
    public Void visitCreateTunespaceStmt(CreateTunespaceStmt node, ConnectContext context) {
        return TunespaceAnalyzer.analyze(node, context);
    }

    @Override
    public Void visitAlterTunespaceStmt(AlterTunespaceStmt node, ConnectContext context) {
        return TunespaceAnalyzer.analyze(node, context);
    }

    @Override
    public Void visitShowRecommendationsStmt(ShowRecommendationsStmt node, ConnectContext context) {
        return TunespaceAnalyzer.analyze(node, context);
    }

    @Override
    public Void visitSubmitRecommendationsTaskStmt(SubmitRecommendationsTaskStmt node, ConnectContext context) {
        return TunespaceAnalyzer.analyze(node, context);
    }

    @Override
    public Void visitRestoreTableFromSnapshotStatement(RestoreTableFromSnapshotStmt statement, ConnectContext context) {
        ClusterSnapshotAnalyzer.analyze(statement, context);
        return null;
    }
}