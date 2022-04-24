// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.analysis;


import com.starrocks.catalog.Catalog;
import com.starrocks.cluster.ClusterNamespace;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.FeNameFormat;
import com.starrocks.common.UserException;
import com.starrocks.mysql.privilege.Auth;
import com.starrocks.mysql.privilege.PrivPredicate;
import com.starrocks.qe.ConnectContext;

// GRANT Role 'role' TO 'user'
public class GrantRoleStmt extends DdlStmt {
    protected String role;
    protected String qualifiedRole = null;
    protected UserIdentity userIdent;

    public GrantRoleStmt(String role, UserIdentity userIdent) {
        this.role = role;
        this.userIdent = userIdent;
    }

    public UserIdentity getUserIdent() {
        return userIdent;
    }

    public String getQualifiedRole() {
        return qualifiedRole;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException, UserException {
        super.analyze(analyzer);
        Auth auth = Catalog.getCurrentCatalog().getAuth();

        // validate user
        userIdent.analyze(analyzer.getClusterName());
        if (!auth.getUserPrivTable().doesUserExist(userIdent)) {
            throw new AnalysisException("user " + userIdent + " not exist!");
        }

        // validate role
        // notice that this method is shared with REVOKE ROLE statement
        FeNameFormat.checkRoleName(role, true /* can be admin */, "Can not granted/revoke role to user");
        qualifiedRole = ClusterNamespace.getFullName(analyzer.getClusterName(), role);
        if (!auth.doesRoleExist(qualifiedRole)) {
            throw new AnalysisException("role " + qualifiedRole + " not exist!");
        }

        // check if current user has GRANT priv on GLOBAL level.
        if (!auth.checkGlobalPriv(ConnectContext.get(), PrivPredicate.GRANT)) {
            // notice that this method is shared with REVOKE ROLE statment
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "GRANT/REVOKE ROLE");
        }
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("GRANT ROLE ").append(role).append(" TO ").append(userIdent);
        return sb.toString();
    }

    @Override
    public String toString() {
        return toSql();
    }
}
