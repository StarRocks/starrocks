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

package com.starrocks.epack.authorization;

import com.starrocks.authorization.AccessController;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.TableName;
import com.starrocks.connector.hive.MockedHiveMetadata;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.MetadataMgr;
import com.starrocks.sql.analyzer.Authorizer;
import com.starrocks.sql.plan.ConnectorPlanTestBase;
import com.starrocks.type.IntegerType;
import mockit.Invocation;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The masking / row-access policy lookup runs for every table relation during analysis, i.e. inside the planner
 * meta lock, and building the TableUID it looks up by resolves the database and the table again. For a JDBC
 * catalog that getDb opens a connection -- the dynamic sql-test run reported it for a view over a JDBC table.
 * A catalog with no policy applied to any of its tables must answer without asking it anything.
 */
public class PolicyLookupConnectorIOTest extends ConnectorPlanTestBase {

    @Test
    public void testACatalogWithoutPoliciesIsNotAskedForTheTableUid() {
        String catalog = MockedHiveMetadata.MOCKED_HIVE_CATALOG_NAME;
        AccessController controller = Authorizer.getInstance().getAccessControlOrDefault(catalog);
        // Otherwise the calls below never reach the code under test and the count stays 0 for any reason.
        Assertions.assertInstanceOf(NativeAccessControllerEPack.class, controller);

        AtomicInteger getDbCalls = new AtomicInteger();
        new MockUp<MetadataMgr>() {
            @Mock
            public Database getDb(Invocation invocation, ConnectContext context, String catalogName, String dbName) {
                if (catalog.equals(catalogName)) {
                    getDbCalls.incrementAndGet();
                }
                return invocation.proceed(context, catalogName, dbName);
            }
        };

        TableName tableName = new TableName(catalog, "tpch", "region");
        List<Column> columns = List.of(new Column("r_regionkey", IntegerType.INT));
        Assertions.assertNull(controller.getColumnMaskingPolicy(connectContext, tableName, columns));
        Assertions.assertNull(controller.getRowAccessPolicy(connectContext, tableName));
        Assertions.assertEquals(0, getDbCalls.get(),
                "the policy lookup contacted a catalog none of whose tables has a policy applied");
    }
}
