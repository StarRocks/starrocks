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

package com.starrocks.authorization;

import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableName;
import com.starrocks.common.FeConstants;
import com.starrocks.connector.statistics.ConnectorTableColumnStats;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.StatementPlanner;
import com.starrocks.sql.analyzer.Authorizer;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.optimizer.statistics.CachedStatisticStorage;
import com.starrocks.sql.optimizer.statistics.StatisticStorage;
import com.starrocks.sql.plan.ConnectorPlanTestBase;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * The column pruning {@link ColumnPrivilege} runs for a Ranger-style catalog happens before the statement is
 * authorized, and its plan is thrown away. A statement the check then denies must leave no trace behind: the
 * statistics it looked up on the way must not have queued a collection for the table.
 */
public class ColumnPrivilegeTriggerAnalyzeTest extends ConnectorPlanTestBase {

    private static final String HIVE_CATALOG = "hive0";
    private static final String SQL =
            "SELECT l_orderkey FROM hive0.partitioned_db.lineitem_par WHERE l_partkey > 10";

    /** Every lookup of connector statistics, as whether it was allowed to queue a collection. */
    private static final class RecordingStatisticStorage extends CachedStatisticStorage {
        final List<Boolean> lookups = new ArrayList<>();

        @Override
        public List<ConnectorTableColumnStats> getConnectorTableStatistics(Table table, List<String> columns,
                                                                           boolean queryTriggerAnalyzeEnabled) {
            lookups.add(queryTriggerAnalyzeEnabled);
            return columns.stream().map(c -> ConnectorTableColumnStats.unknown()).collect(Collectors.toList());
        }
    }

    /** Installing any ExternalAccessController is what makes ColumnPrivilege run the optimization. */
    private static ExternalAccessController controller(boolean denyColumns) {
        return new ExternalAccessController() {
            @Override
            public void checkTableAction(ConnectContext context, TableName tableName, PrivilegeType privilegeType) {
            }

            @Override
            public void checkColumnAction(ConnectContext context, TableName tableName, String column,
                                          PrivilegeType privilegeType) throws AccessDeniedException {
                if (denyColumns) {
                    throw new AccessDeniedException();
                }
            }

            @Override
            public void checkMaterializedViewAction(ConnectContext context, TableName tableName,
                                                    PrivilegeType privilegeType) {
            }
        };
    }

    /**
     * Plans with the given controller on the hive catalog and returns the statistics lookups planning made. The
     * internal-statistics lookup is skipped in unit tests, so it is switched back on for the planning only.
     */
    private List<Boolean> lookupsWhilePlanning(boolean denyColumns) throws Exception {
        GlobalStateMgr state = connectContext.getGlobalStateMgr();
        AccessControlProvider provider = Authorizer.getInstance();
        StatisticStorage previousStorage = state.getStatisticStorage();
        RecordingStatisticStorage storage = new RecordingStatisticStorage();
        state.setStatisticStorage(storage);
        provider.setAccessControl(HIVE_CATALOG, controller(denyColumns));
        FeConstants.runningUnitTest = false;
        try {
            StatementBase stmt = UtFrameUtils.parseStmtWithNewParserNotIncludeAnalyzer(SQL, connectContext);
            if (denyColumns) {
                Assertions.assertThrows(Exception.class, () -> StatementPlanner.plan(stmt, connectContext));
            } else {
                StatementPlanner.plan(stmt, connectContext);
            }
        } finally {
            FeConstants.runningUnitTest = true;
            provider.removeAccessControl(HIVE_CATALOG);
            state.setStatisticStorage(previousStorage);
        }
        return storage.lookups;
    }

    @Test
    public void testADeniedStatementNeverAllowsACollectionToBeQueued() throws Exception {
        List<Boolean> lookups = lookupsWhilePlanning(true);
        // The check did look the statistics up -- that is the window this guards -- and never with the trigger on.
        Assertions.assertFalse(lookups.isEmpty(), "the check looked up no connector statistics");
        Assertions.assertTrue(lookups.stream().noneMatch(allowed -> allowed),
                "a statement that is denied must not queue a collection: " + lookups);
    }

    @Test
    public void testAnAuthorizedStatementStillAllowsACollection() throws Exception {
        List<Boolean> lookups = lookupsWhilePlanning(false);
        Assertions.assertFalse(lookups.isEmpty(), "planning looked up no connector statistics");
        Assertions.assertFalse(lookups.get(0), "the check's own pruning must not queue a collection");
        Assertions.assertTrue(lookups.contains(true), "the planning after a passed check still queues one");
    }
}
