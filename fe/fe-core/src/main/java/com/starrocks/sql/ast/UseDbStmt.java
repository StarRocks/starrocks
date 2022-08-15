// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.starrocks.analysis.RedirectStatus;
import com.starrocks.analysis.StatementBase;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Representation of a USE [catalog.]db statement.
 * Queries from MySQL client will not generate UseDbStmt, it will be handled by the COM_INIT_DB protocol.
 * Queries from JDBC will be handled by COM_QUERY protocol, it will generate UseDbStmt.
 */
public class UseDbStmt extends StatementBase {
    private static final Logger LOG = LogManager.getLogger(UseDbStmt.class);
    private final String catalog;
    private final String database;

    public UseDbStmt(String db) {
        this(null, db);
    }

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

    @Override
    public boolean isSupportNewPlanner() {
        return true;
    }
}
