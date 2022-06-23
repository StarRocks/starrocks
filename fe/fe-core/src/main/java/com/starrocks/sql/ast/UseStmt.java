// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.ast;

import com.starrocks.analysis.RedirectStatus;
import com.starrocks.analysis.StatementBase;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Representation of a USE [catalog.]db statement.
 * Queries from MySQL client will not generate UseStmt, it will be handled by the COM_INIT_DB protocol.
 * Queries from JDBC will be handled by COM_QUERY protocol, it will generate UseStmt.
 */
public class UseStmt extends StatementBase {
    private static final Logger LOG = LogManager.getLogger(UseStmt.class);
    private final String catalog;
    private final String database;

    public UseStmt(String db) {
        this(null, db);
    }

    public UseStmt(String catalog, String database) {
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
        return visitor.visitUseStatement(this, context);
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        return RedirectStatus.NO_FORWARD;
    }
}
