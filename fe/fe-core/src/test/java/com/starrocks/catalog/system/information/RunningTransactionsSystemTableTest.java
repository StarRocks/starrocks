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

package com.starrocks.catalog.system.information;

import com.google.common.collect.Lists;
import com.starrocks.authorization.AccessDeniedException;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.catalog.system.SystemTable;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LocalMetastore;
import com.starrocks.sql.analyzer.Authorizer;
import com.starrocks.thrift.TGetRunningTxnsParams;
import com.starrocks.thrift.TGetRunningTxnsResult;
import com.starrocks.thrift.TRunningTxnInfo;
import com.starrocks.transaction.GlobalTransactionMgr;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class RunningTransactionsSystemTableTest {

    @Test
    public void testSchema() {
        SystemTable table = RunningTransactionsSystemTable.create();
        Assertions.assertEquals("running_transactions", table.getName());

        // The column list and its order must stay in lockstep with the BE scanner's slot ids (1..24).
        Assertions.assertEquals(24, table.getColumns().size());

        // Spot-check the diagnostic-critical columns exist.
        for (String col : new String[] {
                "TXN_ID", "LABEL", "DATABASE_NAME", "TABLE_NAMES", "STATE",
                "PENDING_PUBLISH_MS", "COMMIT_TIME", "IS_NO_OP_PUBLISH", "NO_OP_PUBLISH_REASON"}) {
            Column column = table.getColumn(col);
            Assertions.assertNotNull(column, "missing column " + col);
        }

        // The headline stall column is a BIGINT count of milliseconds.
        Assertions.assertTrue(table.getColumn("PENDING_PUBLISH_MS").getType().isBigint());
    }

    @Test
    public void testRoutedFromLeader() {
        // Running-transaction state is authoritative only on the leader, so the scan must be leader-routed;
        // otherwise a follower FE would answer from its incomplete local running set.
        Assertions.assertTrue(SystemTable.needQueryFromLeader(RunningTransactionsSystemTable.NAME));
    }

    // A row is shown only for a database the querying user can access. Here the user is denied on one of the
    // two databases, so only the other database's transaction survives.
    @Test
    public void testPrivilegeFiltersRowsByDatabase(@Mocked GlobalStateMgr globalStateMgr,
                                                   @Mocked LocalMetastore metastore,
                                                   @Mocked GlobalTransactionMgr txnMgr,
                                                   @Mocked Database dbAllowed,
                                                   @Mocked Database dbDenied) {
        TRunningTxnInfo allowed = new TRunningTxnInfo();
        allowed.setTxn_id(1);
        allowed.setDatabase_id(1001);
        allowed.setLabel("allowed");
        allowed.setState("PREPARE");
        TRunningTxnInfo denied = new TRunningTxnInfo();
        denied.setTxn_id(2);
        denied.setDatabase_id(1002);
        denied.setLabel("denied");
        denied.setState("PREPARE");

        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                minTimes = 0;
                result = globalStateMgr;

                globalStateMgr.getLocalMetastore();
                minTimes = 0;
                result = metastore;

                globalStateMgr.getGlobalTransactionMgr();
                minTimes = 0;
                result = txnMgr;

                txnMgr.getRunningTransactions((Long) any);
                result = Lists.newArrayList(allowed, denied);

                metastore.getDb(1001L);
                minTimes = 0;
                result = dbAllowed;
                metastore.getDb(1002L);
                minTimes = 0;
                result = dbDenied;
                dbAllowed.getFullName();
                minTimes = 0;
                result = "db_allowed";
                dbDenied.getFullName();
                minTimes = 0;
                result = "db_denied";
            }
        };
        new MockUp<Authorizer>() {
            @Mock
            public void checkAnyActionOnOrInDb(ConnectContext context, String catalog, String db)
                    throws AccessDeniedException {
                if ("db_denied".equals(db)) {
                    throw new AccessDeniedException("denied");
                }
            }
        };

        ConnectContext context = new ConnectContext();
        context.setCurrentUserIdentity(UserIdentity.createAnalyzedUserIdentWithIp("alice", "%"));

        TGetRunningTxnsResult result = RunningTransactionsSystemTable.query(new TGetRunningTxnsParams(), context);
        List<TRunningTxnInfo> rows = result.getTxns();
        Assertions.assertEquals(1, rows.size());
        Assertions.assertEquals("db_allowed", rows.get(0).getDatabase_name());
        Assertions.assertEquals("allowed", rows.get(0).getLabel());
    }

    // With no forwarded user identity (a bare context), the view fails closed and returns nothing rather than
    // an unfiltered dump: getRunningTransactions is a FrontendService RPC on the unauthenticated port.
    @Test
    public void testNoIdentityReturnsNoRows(@Mocked GlobalStateMgr globalStateMgr,
                                            @Mocked LocalMetastore metastore) {
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                minTimes = 0;
                result = globalStateMgr;
                globalStateMgr.getLocalMetastore();
                minTimes = 0;
                result = metastore;
            }
        };

        TGetRunningTxnsResult result =
                RunningTransactionsSystemTable.query(new TGetRunningTxnsParams(), new ConnectContext());
        Assertions.assertTrue(result.getTxns().isEmpty());
    }

    // A running transaction whose database was dropped mid-flight cannot be authorized (its name never
    // resolves), so it is hidden even from an authenticated user.
    @Test
    public void testDroppedDatabaseRowHidden(@Mocked GlobalStateMgr globalStateMgr,
                                             @Mocked LocalMetastore metastore,
                                             @Mocked GlobalTransactionMgr txnMgr) {
        TRunningTxnInfo row = new TRunningTxnInfo();
        row.setTxn_id(1);
        row.setDatabase_id(1001);
        row.setLabel("orphan");
        row.setState("COMMITTED");

        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                minTimes = 0;
                result = globalStateMgr;
                globalStateMgr.getLocalMetastore();
                minTimes = 0;
                result = metastore;
                globalStateMgr.getGlobalTransactionMgr();
                minTimes = 0;
                result = txnMgr;
                txnMgr.getRunningTransactions((Long) any);
                result = Lists.newArrayList(row);
                metastore.getDb(1001L);
                minTimes = 0;
                result = null;
            }
        };

        ConnectContext context = new ConnectContext();
        context.setCurrentUserIdentity(UserIdentity.createAnalyzedUserIdentWithIp("alice", "%"));

        TGetRunningTxnsResult result = RunningTransactionsSystemTable.query(new TGetRunningTxnsParams(), context);
        Assertions.assertTrue(result.getTxns().isEmpty());
    }
}
