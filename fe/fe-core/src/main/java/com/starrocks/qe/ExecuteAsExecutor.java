// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.qe;

import com.google.common.base.Preconditions;
import com.starrocks.sql.ast.ExecuteAsStmt;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ExecuteAsExecutor {
    private static final Logger LOG = LogManager.getLogger(ExecuteAsStmt.class);

    /**
     * Only set current user, won't reset any other context, for example, current database.
     * Because mysql client still think that this session is using old databases and will show such hint,
     * which will only confuse the user
     *
     * MySQL [test_priv]> execute as test1 with no revert;
     * Query OK, 0 rows affected (0.00 sec)
     * MySQL [test_priv]> select * from test_table2;
     * ERROR 1064 (HY000): No database selected
     */
    public static void execute(ExecuteAsStmt stmt, ConnectContext ctx) {
        // only support WITH NO REVERT for now
        Preconditions.checkArgument(!stmt.isAllowRevert());
        LOG.info("{} EXEC AS {} from now on", ctx.getCurrentUserIdentity(), stmt.getToUser());

        ctx.setCurrentUserIdentity(stmt.getToUser());
    }
}
