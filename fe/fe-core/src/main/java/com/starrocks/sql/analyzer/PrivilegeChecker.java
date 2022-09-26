// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.analyzer;

import com.starrocks.analysis.AddSqlBlackListStmt;
import com.starrocks.analysis.BackupStmt;
import com.starrocks.analysis.CompoundPredicate;
import com.starrocks.analysis.CreateRepositoryStmt;
import com.starrocks.analysis.CreateRoleStmt;
import com.starrocks.analysis.CreateRoutineLoadStmt;
import com.starrocks.analysis.DelSqlBlackListStmt;
import com.starrocks.analysis.DeleteStmt;
import com.starrocks.analysis.DropRepositoryStmt;
import com.starrocks.analysis.DropRoleStmt;
import com.starrocks.analysis.DropUserStmt;
import com.starrocks.analysis.PauseRoutineLoadStmt;
import com.starrocks.analysis.ResourcePattern;
import com.starrocks.analysis.RestoreStmt;
import com.starrocks.analysis.ResumeRoutineLoadStmt;
import com.starrocks.analysis.SetUserPropertyStmt;
import com.starrocks.analysis.SetUserPropertyVar;
import com.starrocks.analysis.SetVar;
import com.starrocks.analysis.ShowGrantsStmt;
import com.starrocks.analysis.ShowRestoreStmt;
import com.starrocks.analysis.ShowRolesStmt;
import com.starrocks.analysis.ShowRoutineLoadStmt;
import com.starrocks.analysis.ShowSqlBlackListStmt;
import com.starrocks.analysis.StatementBase;
import com.starrocks.analysis.StopRoutineLoadStmt;
import com.starrocks.analysis.TableName;
import com.starrocks.analysis.TablePattern;
import com.starrocks.analysis.TableRef;
import com.starrocks.analysis.UserIdentity;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Table;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.Pair;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.mysql.privilege.Auth;
import com.starrocks.mysql.privilege.PrivBitSet;
import com.starrocks.mysql.privilege.PrivPredicate;
import com.starrocks.mysql.privilege.Privilege;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.CatalogMgr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.AdminCancelRepairTableStmt;
import com.starrocks.sql.ast.AdminCheckTabletsStmt;
import com.starrocks.sql.ast.AdminRepairTableStmt;
import com.starrocks.sql.ast.AdminSetConfigStmt;
import com.starrocks.sql.ast.AdminSetReplicaStatusStmt;
import com.starrocks.sql.ast.AdminShowConfigStmt;
import com.starrocks.sql.ast.AdminShowReplicaDistributionStmt;
import com.starrocks.sql.ast.AdminShowReplicaStatusStmt;
import com.starrocks.sql.ast.AlterDatabaseQuotaStmt;
import com.starrocks.sql.ast.AlterDatabaseRename;
import com.starrocks.sql.ast.AlterMaterializedViewStmt;
import com.starrocks.sql.ast.AlterResourceGroupStmt;
import com.starrocks.sql.ast.AlterResourceStmt;
import com.starrocks.sql.ast.AlterSystemStmt;
import com.starrocks.sql.ast.AlterTableStmt;
import com.starrocks.sql.ast.AlterViewStmt;
import com.starrocks.sql.ast.AnalyzeStmt;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.BaseCreateAlterUserStmt;
import com.starrocks.sql.ast.BaseGrantRevokePrivilegeStmt;
import com.starrocks.sql.ast.BaseGrantRevokeRoleStmt;
import com.starrocks.sql.ast.CTERelation;
import com.starrocks.sql.ast.CancelAlterTableStmt;
import com.starrocks.sql.ast.CancelRefreshMaterializedViewStmt;
import com.starrocks.sql.ast.CreateAnalyzeJobStmt;
import com.starrocks.sql.ast.CreateDbStmt;
import com.starrocks.sql.ast.CreateFunctionStmt;
import com.starrocks.sql.ast.CreateMaterializedViewStatement;
import com.starrocks.sql.ast.CreateResourceGroupStmt;
import com.starrocks.sql.ast.CreateResourceStmt;
import com.starrocks.sql.ast.CreateTableLikeStmt;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.sql.ast.CreateViewStmt;
import com.starrocks.sql.ast.DescribeStmt;
import com.starrocks.sql.ast.DropDbStmt;
import com.starrocks.sql.ast.DropFunctionStmt;
import com.starrocks.sql.ast.DropHistogramStmt;
import com.starrocks.sql.ast.DropMaterializedViewStmt;
import com.starrocks.sql.ast.DropResourceGroupStmt;
import com.starrocks.sql.ast.DropResourceStmt;
import com.starrocks.sql.ast.DropTableStmt;
import com.starrocks.sql.ast.ExecuteAsStmt;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.JoinRelation;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.RecoverDbStmt;
import com.starrocks.sql.ast.RecoverTableStmt;
import com.starrocks.sql.ast.RefreshMaterializedViewStatement;
import com.starrocks.sql.ast.RefreshTableStmt;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.SetOperationRelation;
import com.starrocks.sql.ast.ShowAlterStmt;
import com.starrocks.sql.ast.ShowAuthenticationStmt;
import com.starrocks.sql.ast.ShowBackendsStmt;
import com.starrocks.sql.ast.ShowBackupStmt;
import com.starrocks.sql.ast.ShowBrokerStmt;
import com.starrocks.sql.ast.ShowComputeNodesStmt;
import com.starrocks.sql.ast.ShowCreateDbStmt;
import com.starrocks.sql.ast.ShowCreateTableStmt;
import com.starrocks.sql.ast.ShowDataStmt;
import com.starrocks.sql.ast.ShowDeleteStmt;
import com.starrocks.sql.ast.ShowFrontendsStmt;
import com.starrocks.sql.ast.ShowFunctionsStmt;
import com.starrocks.sql.ast.ShowIndexStmt;
import com.starrocks.sql.ast.ShowMaterializedViewStmt;
import com.starrocks.sql.ast.ShowPartitionsStmt;
import com.starrocks.sql.ast.ShowProcStmt;
import com.starrocks.sql.ast.ShowTableStatusStmt;
import com.starrocks.sql.ast.ShowTabletStmt;
import com.starrocks.sql.ast.ShowUserPropertyStmt;
import com.starrocks.sql.ast.SubqueryRelation;
import com.starrocks.sql.ast.TableRelation;
import com.starrocks.sql.ast.TruncateTableStmt;
import com.starrocks.sql.ast.UpdateStmt;
import com.starrocks.sql.ast.ViewRelation;
import com.starrocks.sql.common.MetaUtils;
import com.starrocks.statistic.StatsConstants;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

public class PrivilegeChecker {
    public static void check(StatementBase statement, ConnectContext session) {
        if (session.getGlobalStateMgr().isUsingNewPrivilege()) {
            PrivilegeCheckerV2.check(statement, session);
            return;
        }
        new PrivilegeCheckerVisitor().check(statement, session);
    }

    public static boolean checkTblPriv(ConnectContext context,
                                       TableName tableName,
                                       PrivPredicate predicate) {
        return checkTblPriv(context, tableName.getCatalog(),
                tableName.getDb(), tableName.getTbl(), predicate);
    }

    public static boolean checkTblPriv(ConnectContext context,
                                       String catalogName,
                                       String dbName,
                                       String tableName,
                                       PrivPredicate predicate) {
        return !CatalogMgr.isInternalCatalog(catalogName) ||
                GlobalStateMgr.getCurrentState().getAuth().checkTblPriv(
                        context, dbName, tableName, predicate);
    }

    public static boolean checkDbPriv(ConnectContext context,
                                      String catalogName,
                                      String dbName,
                                      PrivPredicate predicate) {
        return !CatalogMgr.isInternalCatalog(catalogName) ||
                GlobalStateMgr.getCurrentState().getAuth().checkDbPriv(context, dbName, predicate);
    }

    private static class PrivilegeCheckerVisitor extends AstVisitor<Void, ConnectContext> {
        public void check(StatementBase statement, ConnectContext session) {
            visit(statement, session);
        }

        @Override
        public Void visitShowComputeNodes(ShowComputeNodesStmt statement, ConnectContext session) {
            if (!GlobalStateMgr.getCurrentState().getAuth().checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)
                    && !GlobalStateMgr.getCurrentState().getAuth().checkGlobalPriv(ConnectContext.get(),
                    PrivPredicate.OPERATOR)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ADMIN/OPERATOR");
            }
            return null;
        }

        @Override
        public Void visitCreateTableStatement(CreateTableStmt statement, ConnectContext session) {
            String dbName = statement.getDbTbl().getDb();
            String tableName = statement.getDbTbl().getTbl();
            if (!GlobalStateMgr.getCurrentState().getAuth()
                    .checkTblPriv(session, dbName, tableName, PrivPredicate.CREATE)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "CREATE");
            }
            return null;
        }

        @Override
        public Void visitCreateTableLikeStatement(CreateTableLikeStmt statement, ConnectContext session) {
            if (!GlobalStateMgr.getCurrentState().getAuth().checkTblPriv(session, statement.getExistedDbName(),
                    statement.getExistedTableName(), PrivPredicate.SELECT)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "SELECT");
            }

            if (!GlobalStateMgr.getCurrentState().getAuth().checkTblPriv(session, statement.getDbName(),
                    statement.getTableName(), PrivPredicate.CREATE)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "CREATE");
            }
            return null;
        }

        @Override
        public Void visitAlterTableStatement(AlterTableStmt statement, ConnectContext session) {
            if (!checkTblPriv(session, statement.getTbl(), PrivPredicate.ALTER)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "Alter");
            }
            return null;
        }

        @Override
        public Void visitShowIndexStmt(ShowIndexStmt statement, ConnectContext session) {
            if (!checkTblPriv(session, statement.getTableName(), PrivPredicate.SHOW)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, session.getQualifiedUser(),
                        statement.getTableName().toString());
            }
            return null;
        }

        @Override
        public Void visitCancelAlterTableStatement(CancelAlterTableStmt statement, ConnectContext session) {
            TableName dbTableName = statement.getDbTableName();
            if (!GlobalStateMgr.getCurrentState().getAuth().checkTblPriv(ConnectContext.get(), dbTableName.getDb(),
                    dbTableName.getTbl(),
                    PrivPredicate.ALTER)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "CANCEL ALTER TABLE",
                        ConnectContext.get().getQualifiedUser(),
                        ConnectContext.get().getRemoteIP(),
                        dbTableName.getTbl());
            }
            return null;
        }

        @Override
        public Void visitAlterResourceGroupStatement(AlterResourceGroupStmt statement, ConnectContext session) {
            if (!GlobalStateMgr.getCurrentState().getAuth().checkGlobalPriv(session, PrivPredicate.ADMIN)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ALTER RESOURCE_GROUP");
            }
            return null;
        }

        @Override
        public Void visitAlterViewStatement(AlterViewStmt statement, ConnectContext session) {
            TableName tableName = statement.getTableName();
            if (!checkTblPriv(session, tableName, PrivPredicate.ALTER)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "ALTER VIEW",
                        session.getQualifiedUser(), session.getRemoteIP(), tableName.getTbl());
            }
            check(statement.getQueryStatement(), session);
            return null;
        }

        @Override
        public Void visitAdminSetConfigStatement(AdminSetConfigStmt statement, ConnectContext session) {
            if (!GlobalStateMgr.getCurrentState().getAuth().checkGlobalPriv(session, PrivPredicate.ADMIN)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ADMIN");
            }
            return null;
        }

        @Override
        public Void visitAdminSetReplicaStatusStatement(AdminSetReplicaStatusStmt statement, ConnectContext session) {
            if (!GlobalStateMgr.getCurrentState().getAuth().checkGlobalPriv(session, PrivPredicate.ADMIN)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ADMIN");
            }
            return null;
        }

        @Override
        public Void visitAdminShowConfigStatement(AdminShowConfigStmt statement, ConnectContext session) {
            if (!GlobalStateMgr.getCurrentState().getAuth()
                    .checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ADMIN");
            }
            return null;
        }

        @Override
        public Void visitAdminShowReplicaDistributionStatement(AdminShowReplicaDistributionStmt statement,
                                                               ConnectContext session) {
            if (!GlobalStateMgr.getCurrentState().getAuth()
                    .checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ADMIN");
            }
            return null;
        }

        @Override
        public Void visitAdminShowReplicaStatusStatement(AdminShowReplicaStatusStmt statement, ConnectContext session) {
            if (!GlobalStateMgr.getCurrentState().getAuth().checkGlobalPriv(session, PrivPredicate.ADMIN)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ADMIN");
            }
            return null;
        }

        @Override
        public Void visitAdminRepairTableStatement(AdminRepairTableStmt statement, ConnectContext session) {
            if (!GlobalStateMgr.getCurrentState().getAuth().checkGlobalPriv(session, PrivPredicate.ADMIN)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ADMIN");
            }
            return null;
        }

        @Override
        public Void visitAdminCheckTabletsStatement(AdminCheckTabletsStmt statement, ConnectContext session) {
            if (!GlobalStateMgr.getCurrentState().getAuth().checkGlobalPriv(session, PrivPredicate.ADMIN)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ADMIN");
            }
            return null;
        }

        @Override
        public Void visitAdminCancelRepairTableStatement(AdminCancelRepairTableStmt statement, ConnectContext session) {
            if (!GlobalStateMgr.getCurrentState().getAuth().checkGlobalPriv(session, PrivPredicate.ADMIN)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ADMIN");
            }
            return null;
        }

        @Override
        public Void visitShowUserPropertyStmt(ShowUserPropertyStmt statement, ConnectContext session) {
            if (!GlobalStateMgr.getCurrentState().getAuth()
                    .checkGlobalPriv(ConnectContext.get(), PrivPredicate.GRANT)) {
                try {
                    ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "GRANT");
                } catch (AnalysisException e) {
                    throw new SemanticException(e.getMessage());
                }
            }
            return null;
        }

        @Override
        public Void visitSetUserPropertyStmt(SetUserPropertyStmt statement, ConnectContext session) {
            if (statement.getPropertyList() == null || statement.getPropertyList().isEmpty()) {
                throw new SemanticException("Empty properties");
            }

            boolean isSelf = statement.getUser().equals(ConnectContext.get().getQualifiedUser());
            try {
                for (SetVar var : statement.getPropertyList()) {
                    ((SetUserPropertyVar) var).analyze(isSelf);
                }
            } catch (AnalysisException e) {
                throw new SemanticException(e.getMessage());
            }

            return null;
        }

        @Override
        public Void visitCreateViewStatement(CreateViewStmt statement, ConnectContext session) {
            TableName tableName = statement.getTableName();
            if (!checkTblPriv(session, tableName, PrivPredicate.CREATE)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "CREATE");
            }

            check(statement.getQueryStatement(), session);
            return null;
        }

        @Override
        public Void visitCreateResourceGroupStatement(CreateResourceGroupStmt statement, ConnectContext session) {
            if (!GlobalStateMgr.getCurrentState().getAuth().checkGlobalPriv(session, PrivPredicate.ADMIN)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR,
                        "CREATE RESOURCE_GROUP");
            }
            return null;
        }

        @Override
        public Void visitDropTableStmt(DropTableStmt statement, ConnectContext session) {
            if (!checkTblPriv(session, statement.getTbl(), PrivPredicate.DROP)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "DROP");
            }
            return null;
        }

        @Override
        public Void visitAddSqlBlackListStatement(AddSqlBlackListStmt stmt, ConnectContext context) {
            if (!GlobalStateMgr.getCurrentState().getAuth().checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ADMIN");
            }
            return null;
        }

        @Override
        public Void visitDelSqlBlackListStatement(DelSqlBlackListStmt stmt, ConnectContext context) {
            if (!GlobalStateMgr.getCurrentState().getAuth().checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ADMIN");
            }
            return null;
        }

        @Override
        public Void visitShowSqlBlackListStatement(ShowSqlBlackListStmt stmt, ConnectContext context) {
            if (!GlobalStateMgr.getCurrentState().getAuth().checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ADMIN");
            }
            return null;
        }

        @Override
        public Void visitCreateResourceStatement(CreateResourceStmt statement,
                                                 ConnectContext session) {
            if (!GlobalStateMgr.getCurrentState().getAuth().checkGlobalPriv(session, PrivPredicate.ADMIN)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ADMIN");
            }
            return null;
        }

        @Override
        public Void visitDropResourceStatement(DropResourceStmt statement,
                                               ConnectContext session) {
            if (!GlobalStateMgr.getCurrentState().getAuth().checkGlobalPriv(session, PrivPredicate.ADMIN)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ADMIN");
            }
            return null;
        }

        @Override
        public Void visitAlterResourceStatement(AlterResourceStmt statement,
                                                ConnectContext session) {
            if (!GlobalStateMgr.getCurrentState().getAuth().checkGlobalPriv(session, PrivPredicate.ADMIN)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ADMIN");
            }
            return null;
        }

        public Void visitRecoverTableStatement(RecoverTableStmt statement, ConnectContext session) {
            if (!GlobalStateMgr.getCurrentState().getAuth().checkTblPriv(ConnectContext.get(), statement.getDbName(),
                    statement.getTableName(),
                    PrivPredicate.of(PrivBitSet.of(Privilege.ALTER_PRIV,
                                    Privilege.CREATE_PRIV,
                                    Privilege.ADMIN_PRIV),
                            CompoundPredicate.Operator.OR))) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "RECOVERY",
                        ConnectContext.get().getQualifiedUser(),
                        ConnectContext.get().getRemoteIP(),
                        statement.getTableName());
            }
            return null;
        }

        public Void visitTruncateTableStatement(TruncateTableStmt statement, ConnectContext session) {
            TableRef tblRef = statement.getTblRef();
            if (!GlobalStateMgr.getCurrentState().getAuth().checkTblPriv(ConnectContext.get(), tblRef.getName().getDb(),
                    tblRef.getName().getTbl(), PrivPredicate.LOAD)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "LOAD");
            }
            return null;
        }

        @Override
        public Void visitDropResourceGroupStatement(DropResourceGroupStmt statement, ConnectContext session) {
            if (!GlobalStateMgr.getCurrentState().getAuth().checkGlobalPriv(session, PrivPredicate.ADMIN)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "DROP RESOURCE_GROUP");
            }
            return null;
        }

        @Override
        public Void visitInsertStatement(InsertStmt statement, ConnectContext session) {
            TableName tableName = statement.getTableName();
            if (!checkTblPriv(session, tableName, PrivPredicate.LOAD)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "LOAD",
                        session.getQualifiedUser(), session.getRemoteIP(), tableName.getTbl());
            }
            check(statement.getQueryStatement(), session);
            return null;
        }

        @Override
        public Void visitQueryStatement(QueryStatement stmt, ConnectContext session) {
            new TablePrivilegeChecker(session).visit(stmt);
            return null;
        }

        @Override
        public Void visitShowTableStatusStmt(ShowTableStatusStmt statement, ConnectContext session) {
            String db = statement.getDb();
            if (!GlobalStateMgr.getCurrentState().getAuth().checkDbPriv(session, db, PrivPredicate.SHOW)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_DB_ACCESS_DENIED, session.getQualifiedUser(), db);
            }
            return null;
        }

        @Override
        public Void visitShowTabletStmt(ShowTabletStmt statement, ConnectContext session) {
            if (!GlobalStateMgr.getCurrentState().getAuth()
                    .checkGlobalPriv(session, PrivPredicate.ADMIN)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "SHOW TABLET");
            }
            return null;
        }

        @Override
        public Void visitShowAlterStmt(ShowAlterStmt statement, ConnectContext session) {
            String db = statement.getDbName();
            if (!GlobalStateMgr.getCurrentState().getAuth().checkDbPriv(ConnectContext.get(), db, PrivPredicate.SHOW)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_DB_ACCESS_DENIED, session.getQualifiedUser(), db);
            }
            return null;
        }

        @Override
        public Void visitShowDeleteStmt(ShowDeleteStmt statement, ConnectContext session) {
            String db = statement.getDbName();
            if (!GlobalStateMgr.getCurrentState().getAuth().checkDbPriv(ConnectContext.get(), db, PrivPredicate.SHOW)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_DB_ACCESS_DENIED, session.getQualifiedUser(), db);
            }
            return null;
        }

        @Override
        public Void visitCreateMaterializedViewStatement(CreateMaterializedViewStatement statement,
                                                         ConnectContext session) {
            if (!checkTblPriv(session, statement.getTableName(), PrivPredicate.CREATE)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "CREATE");
            }
            check(statement.getQueryStatement(), session);
            return null;
        }

        @Override
        public Void visitDropMaterializedViewStatement(DropMaterializedViewStmt statement, ConnectContext session) {
            if (!checkTblPriv(ConnectContext.get(), statement.getDbMvName(), PrivPredicate.DROP)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "DROP");
            }
            return null;
        }

        @Override
        public Void visitAlterMaterializedViewStatement(AlterMaterializedViewStmt statement,
                                                        ConnectContext context) {
            if (!GlobalStateMgr.getCurrentState().getAuth()
                    .checkTblPriv(ConnectContext.get(), statement.getMvName().getDb(),
                            statement.getMvName().getTbl(), PrivPredicate.ALTER)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ALTER");
            }
            return null;
        }

        @Override
        public Void visitRefreshMaterializedViewStatement(RefreshMaterializedViewStatement statement,
                                                          ConnectContext context) {
            if (!checkTblPriv(ConnectContext.get(), statement.getMvName(), PrivPredicate.ALTER)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ALTER");
            }
            return null;

        }

        @Override
        public Void visitCancelRefreshMaterializedViewStatement(CancelRefreshMaterializedViewStmt statement,
                                                                ConnectContext context) {
            if (!checkTblPriv(ConnectContext.get(), statement.getMvName(), PrivPredicate.ALTER)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ALTER");
            }
            return null;
        }

        @Override
        public Void visitAlterSystemStmt(AlterSystemStmt statement, ConnectContext session) {
            if (!GlobalStateMgr.getCurrentState().getAuth()
                    .checkGlobalPriv(ConnectContext.get(), PrivPredicate.OPERATOR)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR,
                        session.getQualifiedUser());
            }
            return null;

        }

        @Override
        public Void visitCreateAlterUserStmt(BaseCreateAlterUserStmt statement, ConnectContext context) {
            // check if current user has GRANT priv on GLOBAL or DATABASE level.
            if (!GlobalStateMgr.getCurrentState().getAuth()
                    .checkHasPriv(context, PrivPredicate.GRANT, Auth.PrivLevel.GLOBAL, Auth.PrivLevel.DATABASE)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "GRANT");
            }
            return null;
        }

        @Override
        public Void visitDropRoleStatement(DropRoleStmt statement, ConnectContext context) {
            // check if current user has GRANT priv on GLOBAL level.
            if (!GlobalStateMgr.getCurrentState().getAuth().checkGlobalPriv(context, PrivPredicate.GRANT)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "CREATE USER");
            }
            return null;
        }

        @Override
        public Void visitCreateRoleStatement(CreateRoleStmt statement, ConnectContext context) {
            // check if current user has GRANT priv on GLOBAL level.
            if (!GlobalStateMgr.getCurrentState().getAuth().checkGlobalPriv(context, PrivPredicate.GRANT)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "CREATE USER");
            }
            return null;
        }

        @Override
        public Void visitShowRolesStatement(ShowRolesStmt statement, ConnectContext context) {
            if (!GlobalStateMgr.getCurrentState().getAuth().checkGlobalPriv(context, PrivPredicate.GRANT)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "GRANT");
            }
            return null;
        }

        @Override
        public Void visitDropUserStatement(DropUserStmt statement, ConnectContext context) {
            // only user with GLOBAL level's GRANT_PRIV can drop user.
            if (!GlobalStateMgr.getCurrentState().getAuth().checkGlobalPriv(context, PrivPredicate.GRANT)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "DROP USER");
            }
            return null;
        }

        @Override
        public Void visitGrantRevokeRoleStatement(BaseGrantRevokeRoleStmt statement, ConnectContext session) {
            // check if current user has GRANT priv on GLOBAL level.
            if (!GlobalStateMgr.getCurrentState().getAuth().checkGlobalPriv(
                    ConnectContext.get(), PrivPredicate.GRANT)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "GRANT");
            }
            return null;
        }

        @Override
        public Void visitGrantRevokePrivilegeStatement(BaseGrantRevokePrivilegeStmt stmt, ConnectContext session) {
            if (stmt.getRole() != null || stmt.getPrivType().equals("USER")) {
                if (!GlobalStateMgr.getCurrentState().getAuth().checkGlobalPriv(
                        session, PrivPredicate.GRANT)) {
                    ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "GRANT");
                }
            } else {
                if (stmt.getPrivType().equals("TABLE")) {
                    TablePattern tblPattern = stmt.getTblPattern();
                    if (tblPattern.getPrivLevel() == Auth.PrivLevel.GLOBAL) {
                        if (!GlobalStateMgr.getCurrentState().getAuth()
                                .checkGlobalPriv(ConnectContext.get(), PrivPredicate.GRANT)) {
                            ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "GRANT");
                        }
                    } else if (tblPattern.getPrivLevel() == Auth.PrivLevel.DATABASE) {
                        if (!GlobalStateMgr.getCurrentState().getAuth()
                                .checkDbPriv(ConnectContext.get(), tblPattern.getQuolifiedDb(), PrivPredicate.GRANT)) {
                            ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "GRANT");
                        }
                    } else {
                        // table level
                        if (!GlobalStateMgr.getCurrentState().getAuth()
                                .checkTblPriv(ConnectContext.get(), tblPattern.getQuolifiedDb(), tblPattern.getTbl(),
                                        PrivPredicate.GRANT)) {
                            ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "GRANT");
                        }
                    }
                } else {
                    ResourcePattern resourcePattern = stmt.getResourcePattern();
                    if (resourcePattern.getPrivLevel() == Auth.PrivLevel.GLOBAL) {
                        if (!GlobalStateMgr.getCurrentState().getAuth()
                                .checkGlobalPriv(ConnectContext.get(), PrivPredicate.GRANT)) {
                            ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "GRANT");
                        }
                    } else {
                        if (!GlobalStateMgr.getCurrentState().getAuth()
                                .checkResourcePriv(ConnectContext.get(), resourcePattern.getResourceName(),
                                        PrivPredicate.GRANT)) {
                            ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "GRANT");
                        }
                    }
                }
            }
            return null;
        }

        @Override
        public Void visitShowGrantsStatement(ShowGrantsStmt statement, ConnectContext session) {
            // if show all grants, or show other user's grants, need global GRANT priv.
            UserIdentity self = session.getCurrentUserIdentity();
            if (statement.isAll() || !self.equals(statement.getUserIdent())) {
                if (!GlobalStateMgr.getCurrentState().getAuth()
                        .checkGlobalPriv(ConnectContext.get(), PrivPredicate.GRANT)) {
                    ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "GRANT");
                }
            }
            return null;
        }

        @Override
        public Void visitExecuteAsStatement(ExecuteAsStmt stmt, ConnectContext session) {
            // check if current user has IMPERSONATE priv
            if (!GlobalStateMgr.getCurrentState().getAuth().canImpersonate(
                    session.getCurrentUserIdentity(), stmt.getToUser())) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "IMPERSONATE");
            }
            return null;
        }

        @Override
        public Void visitShowMaterializedViewStmt(ShowMaterializedViewStmt statement, ConnectContext session) {
            String db = statement.getDb();
            if (!GlobalStateMgr.getCurrentState().getAuth().checkDbPriv(session, db, PrivPredicate.SHOW)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_DB_ACCESS_DENIED, "SHOW MATERIALIZED VIEW",
                        session.getQualifiedUser(),
                        session.getRemoteIP(),
                        db);
            }
            return null;
        }

        @Override
        public Void visitUpdateStatement(UpdateStmt statement, ConnectContext session) {
            // For now, the `update` operation requires the `LOAD` privilege.
            // TODO We're planning to refactor the whole privilege framework to align with mainstream databases such as
            //      MySQL by fine-grained administrative permissions.
            TableName tableName = statement.getTableName();
            if (!checkTblPriv(session, tableName, PrivPredicate.LOAD)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "LOAD",
                        session.getQualifiedUser(), session.getRemoteIP(), tableName.getTbl());
            }
            return null;
        }

        @Override
        public Void visitDeleteStatement(DeleteStmt statement, ConnectContext session) {
            // For now, the `delete` operation requires the `LOAD` privilege.
            // TODO We're planning to refactor the whole privilege framework to align with mainstream databases such as
            //      MySQL by fine-grained administrative permissions.
            TableName tableName = statement.getTableName();
            if (!checkTblPriv(session, tableName, PrivPredicate.LOAD)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "LOAD",
                        session.getQualifiedUser(), session.getRemoteIP(), tableName.getTbl());
            }
            return null;
        }

        @Override
        public Void visitRefreshTableStatement(RefreshTableStmt statement, ConnectContext context) {
            TableName tableName = statement.getTableName();
            MetaUtils.normalizationTableName(context, tableName);
            if (!checkTblPriv(ConnectContext.get(), tableName.getCatalog(),
                    tableName.getDb(), tableName.getTbl(), PrivPredicate.ALTER)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR,
                        "REFRESH EXTERNAL TABLE",
                        ConnectContext.get().getQualifiedUser(),
                        ConnectContext.get().getRemoteIP(),
                        tableName.getTbl());
            }
            return null;
        }

        @Override
        public Void visitShowCreateTableStmt(ShowCreateTableStmt statement, ConnectContext session) {
            if (!GlobalStateMgr.getCurrentState().getAuth()
                    .checkTblPriv(ConnectContext.get(), statement.getDb(), statement.getTable(),
                            PrivPredicate.SHOW)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "SHOW CREATE TABLE",
                        ConnectContext.get().getQualifiedUser(),
                        ConnectContext.get().getRemoteIP(),
                        statement.getTable());
            }
            return null;
        }

        @Override
        public Void visitAnalyzeStatement(AnalyzeStmt statement, ConnectContext session) {
            TableName tableName = statement.getTableName();
            if (!checkTblPriv(session, tableName, PrivPredicate.SELECT)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "SELECT",
                        session.getQualifiedUser(), session.getRemoteIP(), tableName.getTbl());
            }

            if (!checkTblPriv(session, tableName, PrivPredicate.LOAD)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "LOAD",
                        session.getQualifiedUser(), session.getRemoteIP(), tableName.getTbl());
            }
            return null;
        }

        @Override
        public Void visitCreateAnalyzeJobStatement(CreateAnalyzeJobStmt statement, ConnectContext session) {
            if (statement.getDbId() == StatsConstants.DEFAULT_ALL_ID) {
                List<Long> dbIds = GlobalStateMgr.getCurrentState().getDbIds();
                for (Long dbId : dbIds) {
                    Database db = GlobalStateMgr.getCurrentState().getDb(dbId);
                    if (!checkDbPriv(session, InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                            db.getOriginName(),
                            PrivPredicate.SELECT)) {
                        ErrorReport.reportSemanticException(ErrorCode.ERR_DB_ACCESS_DENIED, "SELECT",
                                session.getQualifiedUser(), session.getRemoteIP(),
                                db.getOriginName());
                    }

                    if (!checkDbPriv(session, InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                            db.getOriginName(),
                            PrivPredicate.LOAD)) {
                        ErrorReport.reportSemanticException(ErrorCode.ERR_DB_ACCESS_DENIED, "LOAD",
                                session.getQualifiedUser(), session.getRemoteIP(),
                                db.getOriginName());
                    }
                }
            } else if (StatsConstants.DEFAULT_ALL_ID == statement.getTableId()
                    && StatsConstants.DEFAULT_ALL_ID != statement.getDbId()) {
                Database db = GlobalStateMgr.getCurrentState().getDb(statement.getDbId());
                for (Table table : db.getTables()) {
                    TableName tableName = new TableName(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                            db.getFullName(), table.getName());
                    if (!checkTblPriv(session, tableName, PrivPredicate.SELECT)) {
                        ErrorReport.reportSemanticException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "SELECT",
                                session.getQualifiedUser(), session.getRemoteIP(), tableName.getTbl());
                    }

                    if (!checkTblPriv(session, tableName, PrivPredicate.LOAD)) {
                        ErrorReport.reportSemanticException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "LOAD",
                                session.getQualifiedUser(), session.getRemoteIP(), tableName.getTbl());
                    }
                }
            } else if (StatsConstants.DEFAULT_ALL_ID != statement.getTableId()
                    && StatsConstants.DEFAULT_ALL_ID != statement.getDbId()) {
                TableName tableName = statement.getTableName();

                if (!checkTblPriv(session, tableName, PrivPredicate.SELECT)) {
                    ErrorReport.reportSemanticException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "SELECT",
                            session.getQualifiedUser(), session.getRemoteIP(), tableName.getTbl());
                }

                if (!checkTblPriv(session, tableName, PrivPredicate.LOAD)) {
                    ErrorReport.reportSemanticException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "LOAD",
                            session.getQualifiedUser(), session.getRemoteIP(), tableName.getTbl());
                }
            }

            return null;
        }

        @Override
        public Void visitAlterDbQuotaStmt(AlterDatabaseQuotaStmt statement, ConnectContext session) {
            String dbName = statement.getDbName();
            if (!GlobalStateMgr.getCurrentState().getAuth()
                    .checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_DB_ACCESS_DENIED, session.getQualifiedUser(), dbName);
            }
            return null;
        }

        @Override
        public Void visitDropHistogramStatement(DropHistogramStmt statement, ConnectContext session) {
            TableName tableName = statement.getTableName();

            if (!checkTblPriv(session, tableName, PrivPredicate.LOAD)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "LOAD",
                        session.getQualifiedUser(), session.getRemoteIP(), tableName.getTbl());
            }
            return null;
        }

        @Override
        public Void visitDropFunction(DropFunctionStmt statement, ConnectContext context) {
            // check operation privilege
            if (!GlobalStateMgr.getCurrentState().getAuth()
                    .checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ADMIN");
            }
            return null;
        }

        @Override
        public Void visitCreateFunction(CreateFunctionStmt statement, ConnectContext context) {
            // check operation privilege
            if (!GlobalStateMgr.getCurrentState().getAuth()
                    .checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ADMIN");
            }
            return null;
        }

        @Override
        public Void visitShowCreateDbStatement(ShowCreateDbStmt statement, ConnectContext session) {
            String db = statement.getDb();
            if (!GlobalStateMgr.getCurrentState().getAuth().checkDbPriv(ConnectContext.get(), db,
                    PrivPredicate.of(PrivBitSet.of(Privilege.ADMIN_PRIV,
                                    Privilege.ALTER_PRIV,
                                    Privilege.CREATE_PRIV,
                                    Privilege.DROP_PRIV),
                            CompoundPredicate.Operator.OR))) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_DB_ACCESS_DENIED, session.getQualifiedUser(), db);
            }
            return null;
        }

        @Override
        public Void visitCreateDbStatement(CreateDbStmt statement, ConnectContext session) {
            String dbName = statement.getFullDbName();
            if (!GlobalStateMgr.getCurrentState().getAuth()
                    .checkDbPriv(session, dbName, PrivPredicate.CREATE)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_DB_ACCESS_DENIED, session.getQualifiedUser(), dbName);
            }
            return null;
        }

        @Override
        public Void visitDropDbStatement(DropDbStmt statement, ConnectContext session) {
            String dbName = statement.getDbName();
            if (!GlobalStateMgr.getCurrentState().getAuth().
                    checkDbPriv(ConnectContext.get(), dbName, PrivPredicate.DROP)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_DB_ACCESS_DENIED, session.getQualifiedUser(), dbName);
            }
            return null;
        }

        @Override
        public Void visitAlterDatabaseRename(AlterDatabaseRename statement, ConnectContext session) {
            String dbName = statement.getDbName();
            if (!GlobalStateMgr.getCurrentState().getAuth().checkDbPriv(ConnectContext.get(), dbName,
                    PrivPredicate.of(PrivBitSet.of(Privilege.ADMIN_PRIV,
                                    Privilege.ALTER_PRIV),
                            CompoundPredicate.Operator.OR))) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_DB_ACCESS_DENIED, session.getQualifiedUser(), dbName);
            }
            return null;
        }

        @Override
        public Void visitRecoverDbStmt(RecoverDbStmt statement, ConnectContext session) {
            String dbName = statement.getDbName();
            if (!GlobalStateMgr.getCurrentState().getAuth().checkDbPriv(ConnectContext.get(), dbName,
                    PrivPredicate.of(PrivBitSet.of(Privilege.ALTER_PRIV,
                                    Privilege.CREATE_PRIV,
                                    Privilege.ADMIN_PRIV),
                            CompoundPredicate.Operator.OR))) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_DB_ACCESS_DENIED, session.getQualifiedUser(), dbName);
            }
            return null;
        }

        @Override
        public Void visitShowFunctions(ShowFunctionsStmt statement, ConnectContext context) {
            String dbName = statement.getDbName();
            if (!GlobalStateMgr.getCurrentState().getAuth()
                    .checkDbPriv(ConnectContext.get(), dbName, PrivPredicate.SHOW)) {
                ErrorReport.reportSemanticException(
                        ErrorCode.ERR_DB_ACCESS_DENIED, ConnectContext.get().getQualifiedUser(), dbName);
            }
            return null;
        }

        @Override
        public Void visitShowDataStmt(ShowDataStmt statement, ConnectContext session) {
            String dbName = statement.getDbName();
            Database db = GlobalStateMgr.getCurrentState().getDb(dbName);
            if (db == null) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_BAD_DB_ERROR, dbName);
            }
            db.readLock();
            try {
                String tableName = statement.getTableName();
                List<List<String>> totalRows = statement.getResultRows();
                if (tableName == null) {
                    long totalSize = 0;
                    long totalReplicaCount = 0;

                    // sort by table name
                    List<Table> tables = db.getTables();
                    SortedSet<Table> sortedTables = new TreeSet<>(new Comparator<Table>() {
                        @Override
                        public int compare(Table t1, Table t2) {
                            return t1.getName().compareTo(t2.getName());
                        }
                    });

                    for (Table table : tables) {
                        if (!GlobalStateMgr.getCurrentState().getAuth().checkTblPriv(ConnectContext.get(), dbName,
                                table.getName(),
                                PrivPredicate.SHOW)) {
                            continue;
                        }
                        sortedTables.add(table);
                    }

                    for (Table table : sortedTables) {
                        if (!table.isNativeTable()) {
                            continue;
                        }

                        OlapTable olapTable = (OlapTable) table;
                        long tableSize = olapTable.getDataSize();
                        long replicaCount = olapTable.getReplicaCount();

                        Pair<Double, String> tableSizePair = DebugUtil.getByteUint(tableSize);
                        String readableSize = DebugUtil.DECIMAL_FORMAT_SCALE_3.format(tableSizePair.first) + " "
                                + tableSizePair.second;

                        List<String> row = Arrays.asList(table.getName(), readableSize, String.valueOf(replicaCount));
                        totalRows.add(row);

                        totalSize += tableSize;
                        totalReplicaCount += replicaCount;
                    } // end for tables

                    Pair<Double, String> totalSizePair = DebugUtil.getByteUint(totalSize);
                    String readableSize = DebugUtil.DECIMAL_FORMAT_SCALE_3.format(totalSizePair.first) + " "
                            + totalSizePair.second;
                    List<String> total = Arrays.asList("Total", readableSize, String.valueOf(totalReplicaCount));
                    totalRows.add(total);

                    // quota
                    long quota = db.getDataQuota();
                    long replicaQuota = db.getReplicaQuota();
                    Pair<Double, String> quotaPair = DebugUtil.getByteUint(quota);
                    String readableQuota = DebugUtil.DECIMAL_FORMAT_SCALE_3.format(quotaPair.first) + " "
                            + quotaPair.second;

                    List<String> quotaRow = Arrays.asList("Quota", readableQuota, String.valueOf(replicaQuota));
                    totalRows.add(quotaRow);

                    // left
                    long left = Math.max(0, quota - totalSize);
                    long replicaCountLeft = Math.max(0, replicaQuota - totalReplicaCount);
                    Pair<Double, String> leftPair = DebugUtil.getByteUint(left);
                    String readableLeft = DebugUtil.DECIMAL_FORMAT_SCALE_3.format(leftPair.first) + " "
                            + leftPair.second;
                    List<String> leftRow = Arrays.asList("Left", readableLeft, String.valueOf(replicaCountLeft));
                    totalRows.add(leftRow);
                } else {
                    if (!GlobalStateMgr.getCurrentState().getAuth().checkTblPriv(ConnectContext.get(), dbName,
                            tableName,
                            PrivPredicate.SHOW)) {
                        ErrorReport.reportSemanticException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "SHOW DATA",
                                session.getQualifiedUser(),
                                session.getRemoteIP(),
                                tableName);
                    }

                    Table table = db.getTable(tableName);
                    if (table == null) {
                        ErrorReport.reportAnalysisException(ErrorCode.ERR_BAD_TABLE_ERROR, tableName);
                    }

                    if (table.getType() != Table.TableType.OLAP) {
                        ErrorReport.reportAnalysisException(ErrorCode.ERR_NOT_OLAP_TABLE, tableName);
                    }

                    OlapTable olapTable = (OlapTable) table;
                    int i = 0;
                    long totalSize = 0;
                    long totalReplicaCount = 0;

                    // sort by index name
                    Map<String, Long> indexNames = olapTable.getIndexNameToId();
                    Map<String, Long> sortedIndexNames = new TreeMap<String, Long>();
                    for (Map.Entry<String, Long> entry : indexNames.entrySet()) {
                        sortedIndexNames.put(entry.getKey(), entry.getValue());
                    }

                    for (Long indexId : sortedIndexNames.values()) {
                        long indexSize = 0;
                        long indexReplicaCount = 0;
                        long indexRowCount = 0;
                        for (Partition partition : olapTable.getAllPartitions()) {
                            MaterializedIndex mIndex = partition.getIndex(indexId);
                            indexSize += mIndex.getDataSize();
                            indexReplicaCount += mIndex.getReplicaCount();
                            indexRowCount += mIndex.getRowCount();
                        }

                        Pair<Double, String> indexSizePair = DebugUtil.getByteUint(indexSize);
                        String readableSize = DebugUtil.DECIMAL_FORMAT_SCALE_3.format(indexSizePair.first) + " "
                                + indexSizePair.second;

                        List<String> row = null;
                        if (i == 0) {
                            row = Arrays.asList(tableName,
                                    olapTable.getIndexNameById(indexId),
                                    readableSize, String.valueOf(indexReplicaCount),
                                    String.valueOf(indexRowCount));
                        } else {
                            row = Arrays.asList("",
                                    olapTable.getIndexNameById(indexId),
                                    readableSize, String.valueOf(indexReplicaCount),
                                    String.valueOf(indexRowCount));
                        }

                        totalSize += indexSize;
                        totalReplicaCount += indexReplicaCount;
                        totalRows.add(row);

                        i++;
                    } // end for indices

                    Pair<Double, String> totalSizePair = DebugUtil.getByteUint(totalSize);
                    String readableSize = DebugUtil.DECIMAL_FORMAT_SCALE_3.format(totalSizePair.first) + " "
                            + totalSizePair.second;
                    List<String> row = Arrays.asList("", "Total", readableSize, String.valueOf(totalReplicaCount), "");
                    totalRows.add(row);
                }
            } catch (AnalysisException e) {
                throw new SemanticException(e.getMessage());
            } finally {
                db.readUnlock();
            }
            return null;
        }

        @Override
        public Void visitDescTableStmt(DescribeStmt statement, ConnectContext session) {
            TableName tableName = statement.getDbTableName();
            if (!checkTblPriv(session, tableName, PrivPredicate.SHOW)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "DESCRIBE",
                        session.getQualifiedUser(), session.getRemoteIP(), tableName.getTbl());
            }
            return null;
        }

        @Override
        public Void visitShowProcStmt(ShowProcStmt statement, ConnectContext session) {
            if (!GlobalStateMgr.getCurrentState().getAuth()
                    .checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR,
                        "ADMIN");
            }
            return null;
        }

        @Override
        public Void visitShowPartitionsStmt(ShowPartitionsStmt statement, ConnectContext context) {
            if (!GlobalStateMgr.getCurrentState().getAuth()
                    .checkTblPriv(ConnectContext.get(), statement.getDbName(), statement.getTableName(),
                            PrivPredicate.SHOW)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "SHOW PARTITIONS",
                        context.getQualifiedUser(), context.getRemoteIP(), statement.getTableName());
            }
            return null;
        }

        @Override
        public Void visitCreateRoutineLoadStatement(CreateRoutineLoadStmt statement, ConnectContext session) {
            String db = statement.getDBName();
            String table = statement.getTableName();
            if (!GlobalStateMgr.getCurrentState().getAuth().checkTblPriv(session, db, table, PrivPredicate.LOAD)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_DB_ACCESS_DENIED, session.getQualifiedUser(), db);
            }
            return null;
        }

        @Override
        public Void visitStopRoutineLoadStatement(StopRoutineLoadStmt statement, ConnectContext session) {
            String db = statement.getDbFullName();
            if (!GlobalStateMgr.getCurrentState().getAuth().checkDbPriv(session, db, PrivPredicate.SHOW)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_DB_ACCESS_DENIED, session.getQualifiedUser(), db);
            }
            return null;
        }

        @Override
        public Void visitResumeRoutineLoadStatement(ResumeRoutineLoadStmt statement, ConnectContext session) {
            String db = statement.getDbFullName();
            if (!GlobalStateMgr.getCurrentState().getAuth().checkDbPriv(session, db, PrivPredicate.SHOW)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_DB_ACCESS_DENIED, session.getQualifiedUser(), db);
            }
            return null;
        }

        @Override
        public Void visitPauseRoutineLoadStatement(PauseRoutineLoadStmt statement, ConnectContext session) {
            String db = statement.getDbFullName();
            if (!GlobalStateMgr.getCurrentState().getAuth().checkDbPriv(session, db, PrivPredicate.SHOW)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_DB_ACCESS_DENIED, session.getQualifiedUser(), db);
            }
            return null;
        }

        @Override
        public Void visitShowRoutineLoadStatement(ShowRoutineLoadStmt statement, ConnectContext session) {
            String db = statement.getDbFullName();
            if (!GlobalStateMgr.getCurrentState().getAuth().checkDbPriv(session, db, PrivPredicate.SHOW)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_DB_ACCESS_DENIED, session.getQualifiedUser(), db);
            }
            return null;
        }

        @Override
        public Void visitBackupStmt(BackupStmt statement, ConnectContext context) {
            TableRef tableRef = statement.getTableRefs().get(0);
            if (!GlobalStateMgr.getCurrentState().getAuth().checkDbPriv(ConnectContext.get(),
                    tableRef.getName().getDb(), PrivPredicate.LOAD)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "LOAD");
            }
            return null;
        }

        public Void visitShowBrokerStmt(ShowBrokerStmt statement, ConnectContext context) {
            if (!GlobalStateMgr.getCurrentState().getAuth().checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)
                    && !GlobalStateMgr.getCurrentState().getAuth().checkGlobalPriv(ConnectContext.get(),
                    PrivPredicate.OPERATOR)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ADMIN/OPERATOR");
            }
            return null;
        }

        @Override
        public Void visitShowBackupStmt(ShowBackupStmt showBackupStmt, ConnectContext context) {
            String dbName = showBackupStmt.getDbName();
            if (!GlobalStateMgr.getCurrentState().getAuth()
                    .checkDbPriv(ConnectContext.get(), dbName, PrivPredicate.LOAD)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_DB_ACCESS_DENIED,
                        ConnectContext.get().getQualifiedUser(), dbName);
            }
            return null;
        }

        @Override
        public Void visitRestoreStmt(RestoreStmt statement, ConnectContext context) {
            String dbName = statement.getDbName();
            if (!GlobalStateMgr.getCurrentState().getAuth()
                    .checkDbPriv(ConnectContext.get(), dbName, PrivPredicate.LOAD)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "LOAD");
            }
            return null;
        }

        @Override
        public Void visitShowRestoreStmt(ShowRestoreStmt showRestoreStmt, ConnectContext context) {
            String dbName = showRestoreStmt.getDbName();
            if (!GlobalStateMgr.getCurrentState().getAuth()
                    .checkDbPriv(ConnectContext.get(), dbName, PrivPredicate.LOAD)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_DB_ACCESS_DENIED,
                        ConnectContext.get().getQualifiedUser(), dbName);
            }
            return null;
        }

        @Override
        public Void visitShowAuthenticationStatement(ShowAuthenticationStmt statement, ConnectContext context) {
            if (statement.isAll() || !context.getCurrentUserIdentity().equals(statement.getUserIdent())) {
                if (!GlobalStateMgr.getCurrentState().getAuth()
                        .checkGlobalPriv(ConnectContext.get(), PrivPredicate.GRANT)) {
                    ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "GRANT");
                }
            }
            return null;
        }

        @Override
        public Void visitShowBackendsStmt(ShowBackendsStmt statement, ConnectContext context) {
            if (!GlobalStateMgr.getCurrentState().getAuth().checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)
                    && !GlobalStateMgr.getCurrentState().getAuth().checkGlobalPriv(ConnectContext.get(),
                    PrivPredicate.OPERATOR)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ADMIN/OPERATOR");
            }
            return null;
        }

        @Override
        public Void visitShowFrontendsStmt(ShowFrontendsStmt statement, ConnectContext context) {
            if (!GlobalStateMgr.getCurrentState().getAuth().checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)
                    && !GlobalStateMgr.getCurrentState().getAuth().checkGlobalPriv(ConnectContext.get(),
                    PrivPredicate.OPERATOR)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ADMIN/OPERATOR");
            }
            return null;
        }

        @Override
        public Void visitCreateRepositoryStmt(CreateRepositoryStmt statement, ConnectContext context) {
            if (!GlobalStateMgr.getCurrentState().getAuth().checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ADMIN");
            }
            return null;
        }

        @Override
        public Void visitDropRepositoryStmt(DropRepositoryStmt statement, ConnectContext context) {
            if (!GlobalStateMgr.getCurrentState().getAuth().checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ADMIN");
            }
            return null;
        }
    }

    private static class TablePrivilegeChecker extends AstVisitor<Void, Void> {
        private ConnectContext session;

        public TablePrivilegeChecker(ConnectContext session) {
            this.session = session;
        }

        @Override
        public Void visitQueryStatement(QueryStatement node, Void context) {
            return visit(node.getQueryRelation());
        }

        @Override
        public Void visitSubquery(SubqueryRelation node, Void context) {
            return visit(node.getQueryStatement());
        }

        @Override
        public Void visitView(ViewRelation node, Void context) {
            // if user has select privilege for the view, then there's no need to check base table
            if (checkTblPriv(session, node.getName(), PrivPredicate.SELECT)) {
                return null;
            }
            return visit(node.getQueryStatement());
        }

        @Override
        public Void visitSelect(SelectRelation node, Void context) {
            if (node.hasWithClause()) {
                node.getCteRelations().forEach(this::visit);
            }

            return visit(node.getRelation());
        }

        @Override
        public Void visitSetOp(SetOperationRelation node, Void context) {
            if (node.hasWithClause()) {
                node.getRelations().forEach(this::visit);
            }
            node.getRelations().forEach(this::visit);
            return null;
        }

        @Override
        public Void visitJoin(JoinRelation node, Void context) {
            visit(node.getLeft());
            visit(node.getRight());
            return null;
        }

        @Override
        public Void visitCTE(CTERelation node, Void context) {
            return visit(node.getCteQueryStatement());
        }

        @Override
        public Void visitTable(TableRelation node, Void context) {
            if (!checkTblPriv(session, node.getName(), PrivPredicate.SELECT)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "SELECT",
                        session.getQualifiedUser(), session.getRemoteIP(), node.getTable());
            }
            return null;
        }

    }

}
