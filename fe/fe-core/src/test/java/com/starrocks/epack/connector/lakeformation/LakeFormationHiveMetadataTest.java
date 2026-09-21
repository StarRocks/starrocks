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

import com.starrocks.catalog.Table;
import com.starrocks.connector.hive.HiveClassNames;
import com.starrocks.connector.hive.HiveMetadata;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.Column;
import software.amazon.awssdk.services.glue.model.GetUnfilteredTableMetadataResponse;
import software.amazon.awssdk.services.glue.model.SerDeInfo;
import software.amazon.awssdk.services.glue.model.StorageDescriptor;
import software.amazon.awssdk.services.lakeformation.LakeFormationClient;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class LakeFormationHiveMetadataTest {

    private static final String CATALOG = "lf";

    @Mocked
    private GlobalStateMgr globalStateMgr;
    @Mocked
    private GlueClient glueClient;
    @Mocked
    private LakeFormationClient lakeFormationClient;

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

    /**
     * Recorded only by the tests that actually build a table. HiveTable.getProperties() looks the table's
     * resource up, and a cascaded mock would hand back a Resource that cannot be cast to HiveResource; a
     * recorded expectation the other tests never reach would itself fail, so it cannot go in setUp.
     */
    private void stubResourceLookup() {
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState().getResourceMgr().getResource(anyString);
                result = null;
                minTimes = 0;
            }
        };
    }

    /** Only aws.glue.catalog_id is left out, because absent is the common configuration. */
    private static LakeFormationCatalogProperties properties() {
        Map<String, String> raw = new HashMap<>();
        raw.put("hive.metastore.type", "glue");
        raw.put("catalog.access.control", "lakeformation");
        raw.put("aws.lakeformation.session_tag_value", "starrocks");
        raw.put("aws.glue.region", "us-west-2");
        return LakeFormationCatalogProperties.from("hive", raw);
    }

    private static GetUnfilteredTableMetadataResponse registeredParquetTable() {
        return GetUnfilteredTableMetadataResponse.builder()
                .isRegisteredWithLakeFormation(true)
                .authorizedColumns("id")
                .table(software.amazon.awssdk.services.glue.model.Table.builder()
                        .name("t")
                        .databaseName("db")
                        .tableType("EXTERNAL_TABLE")
                        .retention(0)
                        .storageDescriptor(StorageDescriptor.builder()
                                .location("s3://bucket/db/t")
                                .compressed(false)
                                .numberOfBuckets(0)
                                .storedAsSubDirectories(false)
                                .inputFormat(HiveClassNames.MAPRED_PARQUET_INPUT_FORMAT_CLASS)
                                .serdeInfo(SerDeInfo.builder()
                                        .serializationLibrary(HiveClassNames.PARQUET_HIVE_SERDE_CLASS).build())
                                .columns(Column.builder().name("id").type("int").build(),
                                        Column.builder().name("ssn").type("int").build())
                                .build())
                        .build())
                .build();
    }

    private LakeFormationHiveMetadata metadataAnswering(AtomicInteger calls,
                                                       GetUnfilteredTableMetadataResponse response,
                                                       RuntimeException failure) {
        LakeFormationMetadataGateway gateway =
                new LakeFormationMetadataGateway(glueClient, lakeFormationClient, properties());
        new MockUp<LakeFormationMetadataGateway>() {
            @Mock
            public AuthorizedTableMetadata getTableMetadata(LakeFormationTableIdentity identity,
                                                            LakeFormationQuerySession session) {
                calls.incrementAndGet();
                if (failure != null) {
                    throw failure;
                }
                return AuthorizedTableMetadata.from(response);
            }
        };
        return new LakeFormationHiveMetadata(CATALOG, null, null, null, null,
                java.util.Optional.empty(), null, null, gateway, properties());
    }

    @Test
    public void testResolvesOncePerTableWithinOneQuery() {
        stubResourceLookup();
        AtomicInteger calls = new AtomicInteger();
        LakeFormationHiveMetadata metadata = metadataAnswering(calls, registeredParquetTable(), null);

        Table first = metadata.getTable(context, "db", "t");
        Table second = metadata.getTable(context, "db", "t");

        assertEquals(1, calls.get(), "the second getTable must come from the memo");
        assertSame(first, second);
        assertInstanceOf(LakeFormationHiveTable.class, first);
    }

    /** Hive and Glue treat names case insensitively, so two spellings are one table and one resolution. */
    @Test
    public void testTableNameCaseDoesNotCauseASecondResolution() {
        stubResourceLookup();
        AtomicInteger calls = new AtomicInteger();
        LakeFormationHiveMetadata metadata = metadataAnswering(calls, registeredParquetTable(), null);

        metadata.getTable(context, "db", "t");
        metadata.getTable(context, "DB", "T");

        assertEquals(1, calls.get());
    }

    /**
     * A failure has to stay a failure for the rest of the query. Retrying would let a transient error turn
     * into a different authorization answer halfway through planning.
     */
    @Test
    public void testAuthorizationFailureIsStickyAndDoesNotFallBack() {
        AtomicInteger calls = new AtomicInteger();
        LakeFormationHiveMetadata metadata = metadataAnswering(calls, null,
                new LakeFormationTableAccessException("boom"));

        assertThrows(LakeFormationTableAccessException.class, () -> metadata.getTable(context, "db", "t"));
        assertThrows(LakeFormationTableAccessException.class, () -> metadata.getTable(context, "db", "t"));
        assertEquals(1, calls.get(), "a memoized failure must not be retried");
    }

    /** The projection narrows the schema; the physical name lists stay as they were. */
    @Test
    public void testAuthorizedTableExposesOnlyTheAuthorizedColumns() {
        stubResourceLookup();
        LakeFormationHiveMetadata metadata =
                metadataAnswering(new AtomicInteger(), registeredParquetTable(), null);
        LakeFormationHiveTable table = (LakeFormationHiveTable) metadata.getTable(context, "db", "t");

        assertEquals(1, table.getFullSchema().size());
        assertEquals("id", table.getFullSchema().get(0).getName());
        assertTrue(table.isColumnAuthorized("id"));
        assertTrue(table.getDataColumnNames().contains("ssn"), "name lists stay physical");
    }

    @Test
    public void testUnregisteredTableTakesTheOrdinaryPath() {
        AtomicInteger superCalls = new AtomicInteger();
        new MockUp<HiveMetadata>() {
            @Mock
            public Table getTable(ConnectContext ctx, String dbName, String tblName) {
                superCalls.incrementAndGet();
                return null;
            }
        };
        LakeFormationHiveMetadata metadata = metadataAnswering(new AtomicInteger(),
                GetUnfilteredTableMetadataResponse.builder()
                        .isRegisteredWithLakeFormation(false)
                        .authorizedColumns("id")
                        .build(), null);

        metadata.getTable(context, "db", "t");
        assertEquals(1, superCalls.get(), "an unregistered table must fall through to HiveMetadata");
    }

    /**
     * Refusing rather than guessing. A response with no flag at all is not the same as "not registered",
     * and reading it that way would take an unregistered path for a table that may well be governed.
     */
    @Test
    public void testMissingRegisteredFlagFailsClosed() {
        LakeFormationHiveMetadata metadata = metadataAnswering(new AtomicInteger(),
                GetUnfilteredTableMetadataResponse.builder().authorizedColumns("id").build(), null);

        LakeFormationTableAccessException e = assertThrows(LakeFormationTableAccessException.class,
                () -> metadata.getTable(context, "db", "t"));
        assertTrue(e.getMessage().contains("IsRegisteredWithLakeFormation"), e.getMessage());
    }

    /** Data credentials are never handed out in this version, and this one is reached before any guard. */
    @Test
    public void testCloudConfigurationIsRefused() {
        LakeFormationHiveMetadata metadata =
                metadataAnswering(new AtomicInteger(), registeredParquetTable(), null);
        assertThrows(LakeFormationTableAccessException.class, metadata::getCloudConfiguration);
    }

    /** Deny by default: an empty memo means nobody resolved this table here, not that it is unregistered. */
    @Test
    public void testPartitionListingIsRefusedWhenNothingResolvedTheTable() {
        LakeFormationHiveMetadata metadata =
                metadataAnswering(new AtomicInteger(), registeredParquetTable(), null);
        assertThrows(LakeFormationTableAccessException.class,
                () -> metadata.listPartitionNames("db", "t", null));
    }

    @Test
    public void testMutatorsAreRefused() {
        LakeFormationHiveMetadata metadata =
                metadataAnswering(new AtomicInteger(), registeredParquetTable(), null);
        assertThrows(LakeFormationTableAccessException.class,
                () -> metadata.createDb(context, "db", new HashMap<>()));
        assertThrows(LakeFormationTableAccessException.class, () -> metadata.dropDb(context, "db", true));
        assertThrows(LakeFormationTableAccessException.class, () -> metadata.dropTable(context, null));
        assertThrows(LakeFormationTableAccessException.class, () -> metadata.alterTable(context, null));
        assertThrows(LakeFormationTableAccessException.class,
                () -> metadata.finishSink("db", "t", null, null));
    }
}
