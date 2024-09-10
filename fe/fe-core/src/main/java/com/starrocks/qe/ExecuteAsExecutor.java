// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


package com.starrocks.qe;

import com.google.common.base.Preconditions;
import com.starrocks.authentication.UserProperty;
import com.starrocks.sql.ast.ExecuteAsStmt;
import com.starrocks.sql.ast.UserIdentity;
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

        UserIdentity user = stmt.getToUser();
        ctx.setCurrentUserIdentity(user);
        ctx.setCurrentRoleIds(user);

        if (!user.isEphemeral()) {
            UserProperty userProperty = ctx.getGlobalStateMgr().getAuthenticationMgr()
                    .getUserProperty(user.getUser());
            ctx.updateByUserProperty(userProperty);
        }
    }
}
