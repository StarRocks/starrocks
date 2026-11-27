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

package com.starrocks.connector.lance;

import com.starrocks.catalog.LanceTable;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.connector.MockedMetadataMgr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.MetadataMgr;
import com.starrocks.sql.StatementPlanner;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.DropCatalogStmt;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.sql.plan.PlanTestBase;
import com.starrocks.thrift.TLanceScanSplit;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class LanceTimeTravelPlanTest extends PlanTestBase {
    @Test
    public void bindHistoricalColumnsAndPlanEachRequestedVersion() throws Exception {
        String catalog = "lance_time_travel";
        Map<String, String> properties = Map.of("type", "lance", "lance.catalog.warehouse", "/warehouse");
        GlobalStateMgr state = connectContext.getGlobalStateMgr();
        state.getCatalogMgr().createCatalog("lance", catalog, "", properties);
        MetadataMgr original = state.getMetadataMgr();
        try {
            LanceDirectoryCatalog directory = mock(LanceDirectoryCatalog.class);
            when(directory.listTables("/warehouse", "{}")).thenReturn("[\"events\"]");
            when(directory.describeTable("/warehouse", "{}", "events")).thenReturn(description(2, "new_name"));
            when(directory.describeTable("/warehouse", "{}", "events", "1", null)).thenReturn(description(1, "old_name"));
            when(directory.describeTable("/warehouse", "{}", "events", null, "release")).thenReturn(description(2, "new_name"));
            MockedMetadataMgr metadata = new MockedMetadataMgr(state.getLocalMetastore(), state.getConnectorMgr());
            metadata.registerMockedMetadata(catalog, new LanceMetadata(catalog, properties, directory));
            state.setMetadataMgr(metadata);

            String table = catalog + ".`default`.events";
            String sql = "SELECT a.old_name, b.new_name FROM " + table + " FOR VERSION AS OF 1 a JOIN "
                    + table + " FOR VERSION AS OF 'release' b ON a.old_name = b.new_name";
            ExecPlan plan = planQuery(sql);
            Set<Long> versions = new HashSet<>();
            Assertions.assertEquals(2, plan.getScanNodes().size());
            for (var scan : plan.getScanNodes()) {
                TLanceScanSplit split = new TLanceScanSplit();
                byte[] payload = scan.getScanRangeLocations(0).get(0).scan_range.hdfs_scan_range.getLance_split_info();
                new TDeserializer(new TBinaryProtocol.Factory()).deserialize(split, payload);
                long version = ((LanceTable) scan.getDesc().getTable()).getDatasetVersion();
                Assertions.assertEquals(version, split.getDataset_version());
                versions.add(split.getDataset_version());
            }
            Assertions.assertEquals(Set.of(1L, 2L), versions);
            Assertions.assertThrows(SemanticException.class, () -> planQuery("SELECT * FROM " + table + " FOR VERSION ALL"));
        } finally {
            state.setMetadataMgr(original);
            state.getCatalogMgr().dropCatalog(new DropCatalogStmt(catalog));
        }
    }

    private ExecPlan planQuery(String sql) throws Exception {
        // Time travel is supported in SELECT; getExecPlan also tries to create a view from the SQL.
        connectContext.setQueryId(UUIDUtil.genUUID());
        connectContext.setExecutionId(UUIDUtil.toTUniqueId(connectContext.getQueryId()));
        connectContext.setThreadLocalInfo();
        return StatementPlanner.plan(SqlParser.parse(sql, connectContext.getSessionVariable()).get(0), connectContext);
    }

    private static String description(long version, String column) {
        return """
                {"location":"/warehouse/events.lance","version":%d,"schema":{"fields":[
                {"name":"%s","nullable":true,"type":{"name":"int","bitWidth":64,"isSigned":true},"children":[]}]}}
                """.formatted(version, column);
    }
}
