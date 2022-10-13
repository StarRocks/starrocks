// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.starrocks.analysis.RedirectStatus;

/**
 * Representation of a USE [catalog.]db statement.
 * Queries from MySQL client will not generate UseDbStmt, it will be handled by the COM_INIT_DB protocol.
 * Queries from JDBC will be handled by COM_QUERY protocol, it will generate UseDbStmt.
 */
public class UseDbStmt extends StatementBase {
    private final String catalog;
    private final String database;

    public UseDbStmt(String catalog, String database) {
        this.catalog = catalog;
        this.database = database;
    }

    public String getIdentifier() {
        if (catalog == null) {
            return database;
        } else {
            return catalog + "." + database;
        }
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitUseDbStatement(this, context);
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        return RedirectStatus.NO_FORWARD;
    }
}
