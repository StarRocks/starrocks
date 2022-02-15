// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.analyzer;

import com.google.common.base.Strings;
import com.starrocks.analysis.AlterWorkGroupStmt;
import com.starrocks.analysis.CreateWorkGroupStmt;
import com.starrocks.analysis.DdlStmt;
import com.starrocks.analysis.DropWorkGroupStmt;
import com.starrocks.analysis.InlineViewRef;
import com.starrocks.analysis.InsertStmt;
import com.starrocks.analysis.QueryStmt;
import com.starrocks.analysis.SelectStmt;
import com.starrocks.analysis.StatementBase;
import com.starrocks.analysis.TableName;
import com.starrocks.analysis.TableRef;
import com.starrocks.catalog.Catalog;
import com.starrocks.catalog.View;
import com.starrocks.cluster.ClusterNamespace;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.mysql.privilege.Auth;
import com.starrocks.mysql.privilege.PrivPredicate;
import com.starrocks.qe.ConnectContext;

public class PrivilegeChecker {
    public static void check(StatementBase statement, Auth auth, ConnectContext session) throws AnalysisException {
        if (statement instanceof SelectStmt) {
            SelectStmt selectStmt = (SelectStmt) statement;

            for (TableRef tblRef : selectStmt.getTableRefs()) {
                if (tblRef instanceof InlineViewRef) {
                    QueryStmt inlineStmt = ((InlineViewRef) tblRef).getViewStmt();
                    if (selectStmt.hasWithClause()) {
                        inlineStmt.setWithClause(selectStmt.getWithClause());
                    }
                    PrivilegeChecker.check(inlineStmt, auth, session);
                } else {
                    TableName tbName = tblRef.getName();
                    String dbName = tbName.getDb();
                    if (selectStmt.hasWithClause()) {
                        for (View view : selectStmt.getWithClause().getViews()) {
                            if (view.getName().equals(tblRef.getName().toString())) {
                                PrivilegeChecker.check(view.getQueryStmt(), auth, session);
                            }
                        }
                        continue;
                    }
                    if (Strings.isNullOrEmpty(dbName)) {
                        dbName = session.getDatabase();
                    } else {
                        dbName = ClusterNamespace.getFullName(session.getClusterName(), tbName.getDb());
                    }

                    if (!auth.checkTblPriv(session, dbName, tbName.getTbl(), PrivPredicate.SELECT)) {
                        ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "SELECT",
                                session.getQualifiedUser(), session.getRemoteIP(), tbName.getTbl());
                    }
                }
            }
        } else if (statement instanceof InsertStmt) {
            InsertStmt insertStmt = (InsertStmt) statement;
            insertStmt.getTableName().normalization(session);
            TableName tblName = insertStmt.getTableName();

            if (!auth.checkTblPriv(session, tblName.getDb(), tblName.getTbl(), PrivPredicate.LOAD)) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "LOAD",
                        session.getQualifiedUser(), session.getRemoteIP(), tblName.getTbl());
            }
        } else if (statement instanceof DdlStmt) {
            String hintMsg;
            if (statement instanceof CreateWorkGroupStmt) {
                hintMsg = "CREATE RESOURCE_GROUP";
            } else if (statement instanceof DropWorkGroupStmt) {
                hintMsg = "DROP RESOURCE_GROUP";
            } else if (statement instanceof AlterWorkGroupStmt) {
                hintMsg = "ALTER RESOURCE_GROUP";
            } else {
                throw new AnalysisException("Unsupported DdlStmt");
            }
            if (!Catalog.getCurrentCatalog().getAuth().checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_ACCESS_DENIED_ERROR, hintMsg);
            }
        }
    }
}