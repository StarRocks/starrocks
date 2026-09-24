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

package com.starrocks.sql.plan;

import com.starrocks.connector.ConnectorProperties;
import com.starrocks.connector.ConnectorType;
import com.starrocks.connector.HdfsEnvironment;
import com.starrocks.connector.MockedMetadataMgr;
import com.starrocks.connector.paimon.PaimonMetadata;
import com.starrocks.planner.ScanNode;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.THdfsScanRange;
import com.starrocks.thrift.TScanRangeLocations;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.CloseableIterator;
import org.apache.paimon.utils.InstantiationUtil;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.File;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Plans actual SQL, then reads its emitted scan ranges and applies the residual before LIMIT. */
class PaimonLimitPredicatePlanTest extends ConnectorPlanTestBase {
    private static FileStoreTable table;

    @BeforeAll
    static void createLimitTable() throws Exception {
        String warehouse = new File(temp, "limit-warehouse").toURI().toString();
        Catalog catalog = CatalogFactory.createCatalog(
                CatalogContext.create(Options.fromMap(Map.of("warehouse", warehouse))));
        catalog.createDatabase("db", false);
        Identifier identifier = Identifier.create("db", "t");
        catalog.createTable(identifier, Schema.newBuilder().column("id", DataTypes.INT())
                .column("p", DataTypes.INT()).column("s", DataTypes.STRING()).partitionKeys("p", "s")
                .option("bucket", "-1").option("file.format", "avro").option("write-only", "true")
                .option("manifest.merge-min-count", "1000").option("scan.manifest.parallelism", "1").build(), false);
        table = (FileStoreTable) catalog.getTable(identifier);
        for (int i = 0; i < 20; i++) {
            BatchWriteBuilder builder = table.newBatchWriteBuilder();
            try (BatchTableWrite write = builder.newWrite(); BatchTableCommit commit = builder.newCommit()) {
                for (int j = 0; j < 4; j++) {
                    // Every tested WHERE matches only the last file, not the first 76 rows.
                    write.write(GenericRow.of(i * 4 + j, i == 19 ? 76 : -1,
                            BinaryString.fromString(i == 19 ? "target" : "early")));
                }
                commit.commit(write.prepareCommit());
            }
        }
        Map<String, String> properties = Map.of("type", "paimon", "paimon.catalog.type", "filesystem",
                "paimon.catalog.warehouse", warehouse);
        GlobalStateMgr.getCurrentState().getCatalogMgr().createCatalog("paimon", "paimon_limit", "", properties);
        ((MockedMetadataMgr) GlobalStateMgr.getCurrentState().getMetadataMgr()).registerMockedMetadata("paimon_limit",
                new PaimonMetadata("paimon_limit", new HdfsEnvironment(), catalog,
                        new ConnectorProperties(ConnectorType.PAIMON, properties)));
    }

    @ParameterizedTest
    @CsvSource({
            "p = id, 1", "p = id, 10",
            "p = abs(p), 1", "p = abs(p), 10",
            "s LIKE '%get', 1", "s LIKE '%get', 10",
            "p >= -1 AND s LIKE '%get', 1", "p >= -1 AND s LIKE '%get', 10",
            "id >= 76, 1", "id >= 76, 10",
            "p >= -1 AND id >= 76, 1", "p >= -1 AND id >= 76, 10"
    })
    void testWhereIsAppliedBeforeLimit(String where, int limit) throws Exception {
        connectContext.getSessionVariable().setPaimonReaderMode("JNI");
        ExecPlan plan = getExecPlan("SELECT id, p, s FROM paimon_limit.db.t WHERE " + where + " LIMIT " + limit);
        assertThat(plan.getScanNodes()).hasSize(1);
        ScanNode scan = plan.getScanNodes().get(0);
        List<Integer> result = new ArrayList<>();
        for (TScanRangeLocations location : scan.getScanRangeLocations(0)) {
            THdfsScanRange range = location.getScan_range().getHdfs_scan_range();
            Split split = decode(range.getPaimon_split_info());
            List<Predicate> pushed = decode(range.getJni_predicate_info());
            try (CloseableIterator<InternalRow> rows = table.newReadBuilder().withFilter(pushed).newRead()
                    .executeFilter().createReader(split).toCloseableIterator()) {
                while (rows.hasNext() && result.size() < limit) {
                    InternalRow row = rows.next();
                    boolean matches = where.equals("p = id")
                            ? row.getInt(1) == row.getInt(0) : row.getInt(0) >= 76;
                    if (matches) {
                        result.add(row.getInt(0));
                    }
                }
            }
            if (result.size() == limit) {
                break;
            }
        }
        assertThat(result).as("WHERE must be evaluated before LIMIT: %s LIMIT %s", where, limit)
                .hasSize(Math.min(limit, where.equals("p = id") ? 1 : 4))
                .doesNotHaveDuplicates().allMatch(id -> id >= 76 && id < 80);
    }

    private static <T> T decode(String value) throws Exception {
        return InstantiationUtil.deserializeObject(Base64.getUrlDecoder().decode(value),
                PaimonLimitPredicatePlanTest.class.getClassLoader());
    }
}
