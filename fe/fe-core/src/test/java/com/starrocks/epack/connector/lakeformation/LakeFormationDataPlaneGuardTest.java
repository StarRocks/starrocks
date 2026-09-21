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

package com.starrocks.epack.connector.lakeformation;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.HiveTable;
import com.starrocks.catalog.Table;
import com.starrocks.connector.RemoteFileOperations;
import com.starrocks.connector.hive.HiveMetastoreOperations;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.type.IntegerType;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.lakeformation.LakeFormationClient;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The data plane is closed in this version, and it is closed by two different mechanisms: the entry points
 * that receive a Table object check its type, and the ones that only receive names consult the memo and deny
 * by default. Both are exercised here, including the case that motivated strengthening the first one - a
 * caller handing back a plain HiveTable it obtained earlier.
 */
public class LakeFormationDataPlaneGuardTest {

    private static final String CATALOG = "lf";
    private static final LakeFormationTableIdentity IDENTITY =
            new LakeFormationTableIdentity(CATALOG, null, "us-west-2", "db", "t");

    @Mocked
    private GlobalStateMgr globalStateMgr;
    @Mocked
    private GlueClient glueClient;
    @Mocked
    private LakeFormationClient lakeFormationClient;
    /** super.clear() invalidates both of these; nothing else in this fixture touches them. */
    @Mocked
    private HiveMetastoreOperations hmsOps;
    @Mocked
    private RemoteFileOperations fileOps;

    private ConnectContext context;

    @BeforeEach
    public void setUp() {
        context = new ConnectContext();
        context.setQueryId(UUID.randomUUID());
        context.setThreadLocalInfo();
    }

    @AfterEach
    public void tearDown() {
        ConnectContext.remove();
    }

    /** See LakeFormationHiveMetadataTest: recorded per test, because a cascaded Resource cannot be cast. */
    private void stubResourceLookup() {
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState().getResourceMgr().getResource(anyString);
                result = null;
                minTimes = 0;
            }
        };
    }

    private static LakeFormationCatalogProperties properties() {
        Map<String, String> raw = new HashMap<>();
        raw.put("hive.metastore.type", "glue");
        raw.put("catalog.access.control", "lakeformation");
        raw.put("aws.lakeformation.session_tag_value", "starrocks");
        raw.put("aws.glue.region", "us-west-2");
        return LakeFormationCatalogProperties.from("hive", raw);
    }

    private LakeFormationHiveMetadata metadata() {
        return new LakeFormationHiveMetadata(CATALOG, null, hmsOps, fileOps, null, Optional.empty(), null, null,
                new LakeFormationMetadataGateway(glueClient, lakeFormationClient, properties()), properties());
    }

    private static HiveTable physicalTable() {
        List<Column> schema = new ArrayList<>();
        schema.add(new Column("id", IntegerType.INT));
        return HiveTable.builder()
                .setId(1L)
                .setTableName("t")
                .setCatalogName(CATALOG)
                .setHiveDbName("db")
                .setHiveTableName("t")
                .setTableLocation("s3://bucket/db/t")
                .setCreateTime(1600000000L)
                .setFullSchema(schema)
                .setPartitionColumnNames(new ArrayList<>())
                .setDataColumnNames(new ArrayList<>(List.of("id")))
                .setProperties(new HashMap<>())
                .setSerdeProperties(new HashMap<>())
                .build();
    }

    private static LakeFormationHiveTable authorizedTable() {
        return LakeFormationHiveTable.of(physicalTable(),
                List.of(new Column("id", IntegerType.INT)), IDENTITY);
    }

    // ---- entry points that receive the table object -----------------------------------------------------

    @Test
    public void testEveryTableFacingDataEntryPointIsRefused() {
        stubResourceLookup();
        LakeFormationHiveMetadata metadata = metadata();
        Table table = authorizedTable();

        assertThrows(LakeFormationTableAccessException.class, () -> metadata.getRemoteFiles(table, null));
        assertThrows(LakeFormationTableAccessException.class, () -> metadata.getRemoteFilesAsync(table, null));
        assertThrows(LakeFormationTableAccessException.class, () -> metadata.getPartitions(table, List.of()));
        assertThrows(LakeFormationTableAccessException.class,
                () -> metadata.getTableStatistics(null, table, Map.of(), List.of(), null, -1, null));
        assertThrows(LakeFormationTableAccessException.class,
                () -> metadata.refreshTable("db", table, List.of(), false));
        assertThrows(LakeFormationTableAccessException.class,
                () -> metadata.getHivePartitionDataInfos(authorizedTable(), List.of(), 1));
    }

    /**
     * Statistics collection swallows every exception from super and degrades to unknown statistics, so the
     * refusal has to happen before super is reached or it would silently turn into a worse plan instead of
     * an error.
     */
    @Test
    public void testStatisticsRefusalIsNotSwallowed() {
        stubResourceLookup();
        LakeFormationTableAccessException e = assertThrows(LakeFormationTableAccessException.class,
                () -> metadata().getTableStatistics(null, authorizedTable(), Map.of(), List.of(), null, -1, null));
        assertTrue(e.getMessage().contains("collect statistics"), e.getMessage());
    }

    /**
     * The case the type check alone would miss. A caller that kept the plain HiveTable it was given earlier
     * - the statistics collector does exactly this - would otherwise walk straight through, and refreshTable
     * is the one entry point whose result lands in the cross-query table cache.
     */
    @Test
    public void testRefreshIsRefusedEvenWhenHandedThePlainPhysicalTable() throws Exception {
        stubResourceLookup();
        LakeFormationHiveMetadata metadata = metadata();
        seedMemo(metadata, LakeFormationTableResolution.authorized(authorizedTable()));

        assertThrows(LakeFormationTableAccessException.class,
                () -> metadata.refreshTable("db", physicalTable(), List.of(), false));
    }

    /**
     * An explicitly unregistered table keeps behaving exactly as it does without Lake Formation. super is
     * not wired in this fixture, so the assertion is "the refusal is not a Lake Formation one" rather than
     * "nothing is thrown".
     */
    @Test
    public void testUnregisteredTableIsNotRefusedByLakeFormation() throws Exception {
        LakeFormationHiveMetadata metadata = metadata();
        seedMemo(metadata, LakeFormationTableResolution.unregistered());
        assertNotRefusedByLakeFormation(() -> metadata.listPartitionNames("db", "t", null));
    }

    // ---- entry points that only receive names ----------------------------------------------------------

    /**
     * Deny by default. An empty memo does not mean "not registered", it means nobody resolved this table on
     * this instance - the normal state on any freshly created metadata instance.
     */
    @Test
    public void testNameOnlyEntryPointsDenyWhenTheMemoIsEmpty() {
        LakeFormationHiveMetadata metadata = metadata();
        assertThrows(LakeFormationTableAccessException.class,
                () -> metadata.listPartitionNames("db", "t", null));
        assertThrows(LakeFormationTableAccessException.class,
                () -> metadata.listPartitionNamesByValue("db", "t", List.of()));
    }

    @Test
    public void testCloudConfigurationIsRefusedBeforeAnyTableIsInvolved() {
        assertThrows(LakeFormationTableAccessException.class, () -> metadata().getCloudConfiguration());
    }

    /**
     * clear() drops the memo, so a table that was memoized as unregistered stops being allowed through.
     * The instance is reused across queries when there is no query id, which is exactly when a stale
     * "unregistered" answer would be dangerous.
     */
    @Test
    public void testClearDropsTheMemoAndDenyByDefaultReturns() throws Exception {
        LakeFormationHiveMetadata metadata = metadata();
        seedMemo(metadata, LakeFormationTableResolution.unregistered());
        assertNotRefusedByLakeFormation(() -> metadata.listPartitionNames("db", "t", null));

        metadata.clear();

        assertThrows(LakeFormationTableAccessException.class,
                () -> metadata.listPartitionNames("db", "t", null));
    }

    /** Anything other than a Lake Formation refusal means the call got past the guard, which is the point. */
    private static void assertNotRefusedByLakeFormation(Runnable call) {
        try {
            call.run();
        } catch (LakeFormationTableAccessException e) {
            throw new AssertionError("must not be refused by Lake Formation: " + e.getMessage(), e);
        } catch (RuntimeException expected) {
            // super is not wired in this fixture
        }
    }

    @SuppressWarnings("unchecked")
    private static void seedMemo(LakeFormationHiveMetadata metadata, LakeFormationTableResolution resolution)
            throws Exception {
        java.lang.reflect.Field field = LakeFormationHiveMetadata.class.getDeclaredField("memo");
        field.setAccessible(true);
        Map<LakeFormationTableIdentity, LakeFormationTableResolution> memo =
                (Map<LakeFormationTableIdentity, LakeFormationTableResolution>) field.get(metadata);
        memo.put(IDENTITY, resolution);
    }
}
