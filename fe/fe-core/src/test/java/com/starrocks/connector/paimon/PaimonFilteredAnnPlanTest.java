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

package com.starrocks.connector.paimon;

import com.starrocks.catalog.PaimonTable;
import com.starrocks.catalog.Table;
import com.starrocks.connector.CatalogConnectorMetadata;
import com.starrocks.connector.index.ConnectorIndexMetadata;
import com.starrocks.connector.index.ConnectorIndexType;
import com.starrocks.connector.index.TopNIndexCondition;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.operator.physical.PhysicalPaimonScanOperator;
import com.starrocks.sql.plan.PlanTestBase;
import mockit.Mock;
import mockit.MockUp;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Real SQL optimizer and catalog wrappers; only the advertised indexes and unavailable BE are substituted. */
public class PaimonFilteredAnnPlanTest extends PlanTestBase {
    @TempDir
    public static java.nio.file.Path warehouse;
    private final AtomicInteger indexCalls = new AtomicInteger();
    private static final String CATALOG = "filtered_ann_plan";
    private static final String QUERY = "select id, approx_l2_distance(embedding, cast([1,2] as array<float>)) as score "
            + "from " + CATALOG + ".db.vectors ";

    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        try (Catalog catalog = CatalogFactory.createCatalog(CatalogContext.create(new Path(warehouse.toUri())))) {
            catalog.createDatabase("db", true);
            catalog.createTable(Identifier.create("db", "vectors"), Schema.newBuilder()
                    .column("id", DataTypes.INT())
                    .column("pt", DataTypes.STRING())
                    .column("embedding", DataTypes.ARRAY(DataTypes.FLOAT().notNull()).notNull())
                    .column("nullable_embedding", DataTypes.ARRAY(DataTypes.FLOAT().notNull()))
                    .column("nullable_elements", DataTypes.ARRAY(DataTypes.FLOAT()).notNull())
                    .partitionKeys("pt")
                    .option("bucket", "-1")
                    .option("row-tracking.enabled", "true")
                    .option("data-evolution.enabled", "true")
                    .build(), false);
            org.apache.paimon.table.Table table = catalog.getTable(Identifier.create("db", "vectors"));
            // Selected partition a has non-contiguous global row IDs, separated by partition b.
            for (int i = 0; i < 3; i++) {
                BatchWriteBuilder builder = table.newBatchWriteBuilder();
                try (BatchTableWrite write = builder.newWrite(); BatchTableCommit commit = builder.newCommit()) {
                    write.write(GenericRow.of(i, BinaryString.fromString(i == 1 ? "b" : "a"),
                            new GenericArray(new Float[] {1.0f, 2.0f}), null,
                            new GenericArray(new Float[] {1.0f, null})));
                    commit.commit(write.prepareCommit());
                }
            }
        }
        GlobalStateMgr.getCurrentState().getCatalogMgr().createCatalog("paimon", CATALOG, "", Map.of(
                "type", "paimon", "paimon.catalog.type", "filesystem",
                "paimon.catalog.warehouse", warehouse.toUri().toString()));
    }

    @BeforeEach
    public void prepareIndexes() {
        connectContext.getSessionVariable().setPaimonGlobalIndexScanStage(2);
        new MockUp<PaimonMetadata>() {
            @Mock
            public ConnectorIndexMetadata getIndexMetadata(Table table) {
                return ConnectorIndexMetadata.of(Map.of("id", Set.of(ConnectorIndexType.RANGE, ConnectorIndexType.BITMAP),
                        "embedding", Set.of(ConnectorIndexType.VECTOR),
                        "nullable_embedding", Set.of(ConnectorIndexType.VECTOR),
                        "nullable_elements", Set.of(ConnectorIndexType.VECTOR)));
            }
        };
        new MockUp<PaimonGlobalIndexService>() {
            @Mock
            public PaimonGlobalIndexResult evaluate() {
                indexCalls.incrementAndGet();
                throw new IllegalStateException("Simulate an old BE rejecting protocol v3");
            }
        };
    }

    @AfterEach
    public void restoreStage() {
        connectContext.getSessionVariable().setPaimonGlobalIndexScanStage(1);
    }

    @Test
    public void testPartitionAndScalarReachTopNThroughProductionWrappers() throws Exception {
        assertInstanceOf(CatalogConnectorMetadata.class, GlobalStateMgr.getCurrentState().getMetadataMgr()
                .getOptionalMetadata(CATALOG).orElseThrow());
        PhysicalPaimonScanOperator scan = scan(QUERY + "where pt='a' and id>=0 order by score limit 1 offset 1");
        TopNIndexCondition condition = assertInstanceOf(TopNIndexCondition.class, scan.getIndexCondition());
        assertNotNull(condition.getPredicate());
        assertNotNull(condition.getPartitionPredicate());
        assertEquals(Map.of("id", ConnectorIndexType.RANGE, "embedding", ConnectorIndexType.VECTOR),
                condition.getRequiredIndexes());
        assertEquals(2, condition.getCandidateLimit());
        assertTrue(indexCalls.get() > 0);
        assertNotNull(scan.getPredicate()); // A failed/old BE still executes the complete WHERE and original TopN.
        PaimonTable table = (PaimonTable) scan.getTable();
        long snapshot = table.getNativeTable().latestSnapshot().orElseThrow().id();
        assertEquals(List.of(List.of(0L, 0L), List.of(2L, 2L)),
                PaimonTopNFilter.partitionRanges(table, snapshot, condition.getPartitionPredicate()));
    }

    @Test
    public void testPartitionOnlyDoesNotRequireScalarIndex() throws Exception {
        PhysicalPaimonScanOperator scan = scan(QUERY + "where pt='a' order by score limit 2");
        TopNIndexCondition condition = assertInstanceOf(TopNIndexCondition.class, scan.getIndexCondition());
        assertNull(condition.getPredicate());
        assertNotNull(condition.getPartitionPredicate());
        assertEquals(Map.of("embedding", ConnectorIndexType.VECTOR), condition.getRequiredIndexes());
    }

    @Test
    public void testResidualsKeepOrdinaryTopN() throws Exception {
        for (String where : List.of("substr(pt,1,1)='a'", "id>=0 and substr(pt,1,1)='a'",
                "pt='a' or id=1", "id>=0 and array_length(embedding)>1")) {
            assertFalse(scan(QUERY + "where " + where + " order by score limit 1").getIndexCondition()
                    instanceof TopNIndexCondition, where);
        }
    }

    @Test
    public void testStagesZeroAndOneKeepOrdinaryTopN() throws Exception {
        for (int stage : List.of(0, 1)) {
            connectContext.getSessionVariable().setPaimonGlobalIndexScanStage(stage);
            assertNull(scan(QUERY + "where pt='a' and id>0 order by score limit 1").getIndexCondition());
        }
    }

    @Test
    public void testNativeNullableVectorsAndElementsKeepOrdinaryTopN() throws Exception {
        for (String column : List.of("nullable_embedding", "nullable_elements")) {
            assertFalse(scan(QUERY.replace("embedding", column)
                    + "where id>=0 order by score nulls first limit 100").getIndexCondition() instanceof TopNIndexCondition);
            assertFalse(scan(QUERY.replace("embedding", column)
                    + "where pt='a' order by score nulls last limit 100").getIndexCondition() instanceof TopNIndexCondition);
        }
    }

    private PhysicalPaimonScanOperator scan(String sql) throws Exception {
        return findScan(getExecPlan(sql).getPhysicalPlan());
    }

    private static PhysicalPaimonScanOperator findScan(OptExpression expression) {
        if (expression.getOp() instanceof PhysicalPaimonScanOperator scan) {
            return scan;
        }
        for (OptExpression child : expression.getInputs()) {
            PhysicalPaimonScanOperator scan = findScan(child);
            if (scan != null) {
                return scan;
            }
        }
        return null;
    }
}
