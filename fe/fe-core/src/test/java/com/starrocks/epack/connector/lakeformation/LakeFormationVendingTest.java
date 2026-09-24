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
import com.starrocks.catalog.UserIdentity;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.connector.GetRemoteFilesParams;
import com.starrocks.connector.QueryScopedCredentials;
import com.starrocks.connector.RemoteFileInfo;
import com.starrocks.connector.RemoteFileInfoSource;
import com.starrocks.connector.RemoteFileOperations;
import com.starrocks.connector.TableLoadPurpose;
import com.starrocks.connector.hive.Partition;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.Column;
import software.amazon.awssdk.services.glue.model.GetUnfilteredTableMetadataResponse;
import software.amazon.awssdk.services.glue.model.SerDeInfo;
import software.amazon.awssdk.services.glue.model.StorageDescriptor;
import software.amazon.awssdk.services.glue.model.UnfilteredPartition;
import software.amazon.awssdk.services.lakeformation.LakeFormationClient;
import software.amazon.awssdk.services.lakeformation.model.GetTemporaryGlueTableCredentialsResponse;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static com.starrocks.connector.hive.HiveClassNames.MAPRED_PARQUET_INPUT_FORMAT_CLASS;
import static com.starrocks.connector.hive.HiveClassNames.PARQUET_HIVE_SERDE_CLASS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Acquiring credentials is part of resolving a table, not something a later scan discovers it needs. */
public class LakeFormationVendingTest {

    private static final String CATALOG = "lf";
    private static final String ACCESS_KEY = "AKIAEXAMPLETESTKEY";
    private static final String SECRET_KEY = "wJalrXUtnFEMIsecretKEYtestVALUE";
    private static final String SESSION_TOKEN = "FwoGZXIvYXdzTESTsessionTOKENvalue";
    private static final String AUTHORIZATION_ID = "query-authorization-id-from-lake-formation";

    @Mocked
    private GlobalStateMgr globalStateMgr;
    @Mocked
    private GlueClient glueClient;
    @Mocked
    private LakeFormationClient lakeFormationClient;

    private ConnectContext context;
    private LakeFormationQueryScope.Scope scope;

    /** Every vend request this test issued, so the tests can count them and read what was sent. */
    private final List<String> requestedAuthorizationIds = new ArrayList<>();
    private final List<String> requestedTableArns = new ArrayList<>();

    @BeforeEach
    public void setUp() {
        context = new ConnectContext();
        context.setQueryId(UUID.randomUUID());
        // StmtExecutor sets this before it plans, so a vend always has one. Building a context without it
        // was constructing a shape that cannot occur, and it hid the fact that the attempt check used to
        // treat a missing id as a match.
        context.setExecutionId(UUIDUtil.toTUniqueId(UUID.randomUUID()));
        context.setCurrentUserIdentity(UserIdentity.ROOT);
        context.setThreadLocalInfo();
        scope = LakeFormationQueryScope.open(context);
    }

    @AfterEach
    public void tearDown() {
        if (scope != null) {
            scope.close();
        }
        // The handover registry is keyed by query id and lives beyond any one metadata instance, so one
        // test's statement would otherwise still be visible to the next one's.
        LakeFormationPlannedTables.forgetAll();
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
        // Needed to build the table ARN a credential request is addressed to.
        raw.put("aws.glue.catalog_id", "123456789012");
        return LakeFormationCatalogProperties.from("hive", raw);
    }

    /**
     * Set while building a table that must report partition columns. Without it the fixture describes an
     * **unpartitioned** table, and getPartitions answers such a table with its single table partition -
     * which is the right answer there, but not what a `dt=...` listing is testing.
     */
    private static final ThreadLocal<Boolean> PARTITIONED = ThreadLocal.withInitial(() -> false);

    private static GetUnfilteredTableMetadataResponse registeredTable(String name) {
        if (PARTITIONED.get()) {
            return registeredPartitionedTable(name);
        }
        return GetUnfilteredTableMetadataResponse.builder()
                .isRegisteredWithLakeFormation(true)
                .authorizedColumns("id")
                .queryAuthorizationId(AUTHORIZATION_ID)
                .table(software.amazon.awssdk.services.glue.model.Table.builder()
                        .name(name)
                        .databaseName("db")
                        .catalogId("123456789012")
                        .tableType("EXTERNAL_TABLE")
                        .retention(0)
                        .storageDescriptor(StorageDescriptor.builder()
                                .location("s3://bucket/db/" + name)
                                .compressed(false)
                                .numberOfBuckets(0)
                                .storedAsSubDirectories(false)
                                .inputFormat(MAPRED_PARQUET_INPUT_FORMAT_CLASS)
                                .serdeInfo(SerDeInfo.builder()
                                        .serializationLibrary(PARQUET_HIVE_SERDE_CLASS).build())
                                .columns(Column.builder().name("id").type("int").build(),
                                        Column.builder().name("ssn").type("int").build())
                                .build())
                        .build())
                .build();
    }

    private static GetUnfilteredTableMetadataResponse registeredPartitionedTable(String name) {
        return GetUnfilteredTableMetadataResponse.builder()
                .isRegisteredWithLakeFormation(true)
                .authorizedColumns("id", "dt")
                .queryAuthorizationId(AUTHORIZATION_ID)
                .table(software.amazon.awssdk.services.glue.model.Table.builder()
                        .name(name)
                        .databaseName("db")
                        .catalogId("123456789012")
                        .tableType("EXTERNAL_TABLE")
                        .retention(0)
                        .partitionKeys(Column.builder().name("dt").type("string").build())
                        .storageDescriptor(StorageDescriptor.builder()
                                .location("s3://bucket/db/" + name)
                                .compressed(false)
                                .numberOfBuckets(0)
                                .storedAsSubDirectories(false)
                                .inputFormat(MAPRED_PARQUET_INPUT_FORMAT_CLASS)
                                .serdeInfo(SerDeInfo.builder()
                                        .serializationLibrary(PARQUET_HIVE_SERDE_CLASS).build())
                                .columns(Column.builder().name("id").type("int").build(),
                                        Column.builder().name("ssn").type("int").build())
                                .build())
                        .build())
                .build();
    }

    /**
     * @param vendFailure thrown instead of answering, to exercise the paths where Lake Formation says no
     */
    private LakeFormationHiveMetadata metadata(AtomicInteger metadataCalls, AtomicInteger vendCalls,
                                               RuntimeException vendFailure) {
        new MockUp<LakeFormationMetadataGateway>() {
            @Mock
            public AuthorizedTableMetadata getTableMetadata(LakeFormationTableIdentity identity,
                                                            LakeFormationQuerySession session) {
                metadataCalls.incrementAndGet();
                return AuthorizedTableMetadata.from(registeredTable(identity.tableName()));
            }

            @Mock
            public LakeFormationTableAccess vendTableCredentials(LakeFormationTableIdentity identity,
                                                                 String tableArn,
                                                                 String queryAuthorizationId,
                                                                 LakeFormationQuerySession session) {
                vendCalls.incrementAndGet();
                requestedAuthorizationIds.add(queryAuthorizationId);
                requestedTableArns.add(tableArn);
                if (vendFailure != null) {
                    throw vendFailure;
                }
                return LakeFormationTableAccess.from(identity, queryAuthorizationId,
                        GetTemporaryGlueTableCredentialsResponse.builder()
                                .accessKeyId(ACCESS_KEY)
                                .secretAccessKey(SECRET_KEY)
                                .sessionToken(SESSION_TOKEN)
                                .expiration(Instant.now().plus(1, ChronoUnit.HOURS))
                                .build());
            }
        };
        return metadata(null);
    }

    private LakeFormationHiveMetadata metadata(LakeFormationPartitionReader partitionReader) {
        return new LakeFormationHiveMetadata(CATALOG, null, null, null, null, Optional.empty(), null, null,
                new LakeFormationMetadataGateway(glueClient, lakeFormationClient, properties()), properties(),
                partitionReader, Map.of(), new Configuration(false), null, false);
    }

    @Test
    public void testResolvingForDataAccessVendsOnceAndPublishesAReadableTable() {
        stubResourceLookup();
        AtomicInteger metadataCalls = new AtomicInteger();
        AtomicInteger vendCalls = new AtomicInteger();

        Table table = metadata(metadataCalls, vendCalls, null).getTable(context, "db", "t");

        assertEquals(1, metadataCalls.get());
        assertEquals(1, vendCalls.get());
        assertNotNull(((LakeFormationHiveTable) table).currentQueryScopedCredentials().orElse(null),
                "a table published for data access must already carry credentials");
    }

    /**
     * The authorization is passed back exactly as Lake Formation issued it. Asking to be authorized again
     * would be a second decision at a second point in time, and it could differ from the one the plan was
     * built on.
     */
    @Test
    public void testTheAuthorizationLakeFormationIssuedIsUsedVerbatim() {
        stubResourceLookup();
        metadata(new AtomicInteger(), new AtomicInteger(), null).getTable(context, "db", "t");

        assertEquals(List.of(AUTHORIZATION_ID), requestedAuthorizationIds);
        assertEquals(List.of("arn:aws:glue:us-west-2:123456789012:table/db/t"), requestedTableArns);
    }

    /** A self-join asks for the same table twice; it is one table, so it gets one credential. */
    @Test
    public void testTheSameTableTwiceInOneQueryVendsOnce() {
        stubResourceLookup();
        AtomicInteger vendCalls = new AtomicInteger();
        LakeFormationHiveMetadata metadata = metadata(new AtomicInteger(), vendCalls, null);

        Table first = metadata.getTable(context, "db", "t");
        Table second = metadata.getTable(context, "db", "t");

        assertEquals(1, vendCalls.get());
        assertSame(first, second, "one table in one query must be one object");
    }

    /** A join across two tables gets two credentials: each table is authorized on its own. */
    @Test
    public void testTwoTablesInOneQueryGetTheirOwnCredentials() {
        stubResourceLookup();
        AtomicInteger vendCalls = new AtomicInteger();
        LakeFormationHiveMetadata metadata = metadata(new AtomicInteger(), vendCalls, null);

        metadata.getTable(context, "db", "t");
        metadata.getTable(context, "db", "other");

        assertEquals(2, vendCalls.get());
        assertEquals(List.of("arn:aws:glue:us-west-2:123456789012:table/db/t",
                        "arn:aws:glue:us-west-2:123456789012:table/db/other"),
                requestedTableArns);
    }

    /**
     * The failure mode that matters most: when credentials cannot be obtained the table is refused, not
     * served with whatever the catalog itself can reach. Deploying Lake Formation is exactly the decision
     * that the cluster's own identity must not be what reads the data.
     */
    @Test
    public void testAFailedVendRefusesTheTableAndNeverFallsBack() {
        stubResourceLookup();
        LakeFormationHiveMetadata metadata = metadata(new AtomicInteger(), new AtomicInteger(),
                new LakeFormationTableAccessException("Lake Formation refused to issue credentials"));

        LakeFormationTableAccessException e = assertThrows(LakeFormationTableAccessException.class,
                () -> metadata.getTable(context, "db", "t"));
        assertTrue(e.getMessage().contains("refused to issue credentials"), e.getMessage());
        assertNoSecret(e.getMessage());
    }

    /** DESC only shows a schema, so it must not make anyone pay for read access. */
    @Test
    public void testResolvingForMetadataOnlyDoesNotVend() {
        stubResourceLookup();
        AtomicInteger vendCalls = new AtomicInteger();

        Table table = metadata(new AtomicInteger(), vendCalls, null)
                .getTable(context, "db", "t", TableLoadPurpose.METADATA_ONLY);

        assertEquals(0, vendCalls.get());
        assertTrue(((LakeFormationHiveTable) table).currentQueryScopedCredentials().isEmpty(),
                "a metadata-only table must not look scannable");
    }

    /**
     * The upgrade the design forbids. A metadata-only table stays metadata-only; a caller that needs to
     * read resolves it again, which is a separate decision with its own credential request.
     */
    @Test
    public void testAMetadataOnlyTableIsNeverUpgradedInPlace() {
        stubResourceLookup();
        AtomicInteger vendCalls = new AtomicInteger();
        LakeFormationHiveMetadata metadata = metadata(new AtomicInteger(), vendCalls, null);

        Table described = metadata.getTable(context, "db", "t", TableLoadPurpose.METADATA_ONLY);
        Table scanned = metadata.getTable(context, "db", "t", TableLoadPurpose.DATA_ACCESS);

        assertEquals(1, vendCalls.get(), "only the data-access resolution may vend");
        assertTrue(((LakeFormationHiveTable) described).currentQueryScopedCredentials().isEmpty(),
                "the metadata-only table must not have acquired credentials behind its back");
        assertNotNull(((LakeFormationHiveTable) scanned).currentQueryScopedCredentials().orElse(null));
    }

    /** Registered but no authorization returned is an incomplete answer, not a reason to improvise one. */
    @Test
    public void testAMissingAuthorizationIdFailsClosed() {
        stubResourceLookup();
        new MockUp<LakeFormationMetadataGateway>() {
            @Mock
            public AuthorizedTableMetadata getTableMetadata(LakeFormationTableIdentity identity,
                                                            LakeFormationQuerySession session) {
                return AuthorizedTableMetadata.from(registeredTable("t").toBuilder()
                        .queryAuthorizationId(null).build());
            }
        };
        LakeFormationHiveMetadata metadata = new LakeFormationHiveMetadata(CATALOG, null, null, null, null,
                Optional.empty(), null, null,
                new LakeFormationMetadataGateway(glueClient, lakeFormationClient, properties()), properties(),
                null, Map.of(), null, null, false);

        LakeFormationTableAccessException e = assertThrows(LakeFormationTableAccessException.class,
                () -> metadata.getTable(context, "db", "t"));
        assertTrue(e.getMessage().contains("no QueryAuthorizationId"), e.getMessage());
    }

    private static void assertNoSecret(String text) {
        assertFalse(text.contains(ACCESS_KEY), "an access key leaked into: " + text);
        assertFalse(text.contains(SECRET_KEY), "a secret key leaked into: " + text);
        assertFalse(text.contains(SESSION_TOKEN), "a session token leaked into: " + text);
    }

    /**
     * ANALYZE resolves its table twice: while planning, inside the attempt, and again at execution time to
     * hand it to the collection worker. The second resolution gets the first one's table back - the same
     * object, no second authorization, no second credential.
     */
    @Test
    public void testATableAuthorizedWhilePlanningIsHandedBackOnceTheAttemptIsOver() {
        stubResourceLookup();
        AtomicInteger metadataCalls = new AtomicInteger();
        AtomicInteger vendCalls = new AtomicInteger();
        LakeFormationHiveMetadata metadata = metadata(metadataCalls, vendCalls, null);
        Table planned = metadata.getTable(context, "db", "t");
        scope.close();
        scope = null;

        assertSame(planned, metadata.getTable(context, "db", "t"));
        assertEquals(1, metadataCalls.get());
        assertEquals(1, vendCalls.get(), "handing the table back costs nothing");
        // Only what this statement authorized: any other table is refused exactly as before.
        assertThrows(LakeFormationTableAccessException.class, () -> metadata.getTable(context, "db", "other"));
    }

    /** Dropping statistics resolves the table after planning is over, and asks for metadata only. */
    @Test
    public void testDroppingStatisticsResolvesWithoutACredential() {
        stubResourceLookup();
        AtomicInteger metadataCalls = new AtomicInteger();
        AtomicInteger vendCalls = new AtomicInteger();
        LakeFormationHiveMetadata metadata = metadata(metadataCalls, vendCalls, null);
        scope.close();
        scope = null;

        Table table = metadata.getTable(context, "db", "t", TableLoadPurpose.METADATA_ONLY);

        assertNotNull(table, "the statement has to be able to find out what to drop");
        assertEquals(1, metadataCalls.get(), "authorized once, on the one-shot path");
        assertEquals(0, vendCalls.get(), "and Hive metadata needs no credential for it");
        assertTrue(((LakeFormationHiveTable) table).currentQueryScopedCredentials().isEmpty(),
                "nothing on it to scan with");
        // The control: the same table, the same absent attempt, asking for data access instead.
        assertThrows(LakeFormationTableAccessException.class,
                () -> metadata.getTable(context, "db", "t", TableLoadPurpose.DATA_ACCESS),
                "a data-access request outside an attempt stays refused");
    }

    /** The handover has to survive the metadata instance, because the planner does not keep it. */
    @Test
    public void testTheHandoverSurvivesTheInstanceThePlannerDrops() {
        stubResourceLookup();
        AtomicInteger metadataCalls = new AtomicInteger();
        AtomicInteger vendCalls = new AtomicInteger();
        LakeFormationHiveMetadata whilePlanning = metadata(metadataCalls, vendCalls, null);
        Table planned = whilePlanning.getTable(context, "db", "t");
        scope.close();
        scope = null;

        // What removeQueryMetadata leaves behind: the same statement, resolving through a new instance.
        LakeFormationHiveMetadata atExecutionTime = metadata(metadataCalls, vendCalls, null);

        assertSame(planned, atExecutionTime.getTable(context, "db", "t"));
        assertEquals(1, metadataCalls.get(), "authorized once, while the statement was planned");
        assertEquals(1, vendCalls.get(), "handing the table back costs nothing");
    }

    /** And what the handover hands back cannot be scanned with. */
    @Test
    public void testWhatTheHandoverHandsBackCannotBeScannedWith() {
        stubResourceLookup();
        LakeFormationHiveMetadata metadata = metadata(new AtomicInteger(), new AtomicInteger(), null);
        Table planned = metadata.getTable(context, "db", "t");
        assertNotNull(((LakeFormationHiveTable) planned).currentQueryScopedCredentials().orElse(null),
                "while the attempt runs the planned table does carry credentials");
        scope.close();
        scope = null;

        Table handedBack = metadata.getTable(context, "db", "t");

        assertTrue(((LakeFormationHiveTable) handedBack).currentQueryScopedCredentials().isEmpty(),
                "once the attempt is over there is nothing left on it to scan with");
    }

    /**
     * The handover answers a data-access resolution only. A schema reader outside an attempt keeps taking
     * the one-shot path it always took, rather than being handed a table another purpose authorized -
     * purposes are separate entries everywhere else in this design and must stay separate here.
     */
    @Test
    public void testASchemaReaderIsNotServedFromTheDataAccessHandover() {
        stubResourceLookup();
        AtomicInteger metadataCalls = new AtomicInteger();
        LakeFormationHiveMetadata metadata = metadata(metadataCalls, new AtomicInteger(), null);
        metadata.getTable(context, "db", "t");
        scope.close();
        scope = null;

        metadata.getTable(context, "db", "t", TableLoadPurpose.METADATA_ONLY);

        assertEquals(2, metadataCalls.get(),
                "the schema read authorizes on its own rather than reusing the data-access answer");
    }

    /**
     * The collection worker lists partitions before its collecting statement is planned, under the context the statistics
     * subsystem builds for it.
     */
    @Test
    public void testAStatisticsWorkerListsPartitionsWithAnAttemptOfItsOwn(
            @Mocked LakeFormationPartitionSnapshot snapshot) {
        stubResourceLookup();
        scope.close();
        scope = null;
        context.setStatisticsContext(true);
        new Expectations() {
            {
                snapshot.partitionNames();
                result = List.of("dt=2026-09-09");
                minTimes = 0;
            }
        };
        new MockUp<LakeFormationHiveMetadata>() {
            @Mock
            LakeFormationPartitionSnapshot loadPartitions(LakeFormationTableResolution resolution,
                                                          LakeFormationTableIdentity identity) {
                return snapshot;
            }
        };
        AtomicInteger metadataCalls = new AtomicInteger();
        AtomicInteger vendCalls = new AtomicInteger();
        LakeFormationHiveMetadata metadata = metadata(metadataCalls, vendCalls, null);

        List<String> names = metadata.listPartitionNames("db", "t", null);

        assertEquals(List.of("dt=2026-09-09"), names);
        assertEquals(1, metadataCalls.get(), "authorized once, in an attempt of its own");
        assertEquals(1, vendCalls.get(), "and paid for once");
        assertTrue(LakeFormationQueryScope.current().isEmpty(), "which left no trace on this thread");
    }

    /** Stands in for the part that reaches S3, recording which partitions each listing covered. */
    private static List<List<Partition>> recordListings() {
        List<List<Partition>> listed = new ArrayList<>();
        new MockUp<RemoteFileOperations>() {
            @Mock
            public List<RemoteFileInfo> getRemoteFiles(Table table, List<Partition> partitions,
                                                       GetRemoteFilesParams params) {
                listed.add(partitions);
                return List.of();
            }

            @Mock
            public RemoteFileInfoSource getRemoteFilesAsync(Table table, GetRemoteFilesParams params,
                                                            Function<GetRemoteFilesParams, List<Partition>> fn) {
                listed.add(fn.apply(params));
                return new RemoteFileInfoSource() {
                    @Override
                    public RemoteFileInfo getOutput() {
                        return null;
                    }

                    @Override
                    public boolean hasMoreOutput() {
                        return false;
                    }
                };
            }
        };
        return listed;
    }

    /** A governed table is listed on its own lease, through the single partition its storage descriptor gives. */
    @Test
    public void testAnUnpartitionedTableIsListedOnItsOwnLease() {
        stubResourceLookup();
        List<List<Partition>> listed = recordListings();
        metadata(new AtomicInteger(), new AtomicInteger(), null);
        LakeFormationHiveMetadata metadata = metadata(null);
        LakeFormationHiveTable table = (LakeFormationHiveTable) metadata.getTable(context, "db", "t");
        QueryScopedCredentials lease = table.currentQueryScopedCredentials().orElseThrow();
        GetRemoteFilesParams params = GetRemoteFilesParams.newBuilder().setQueryScopedCredentials(lease).build();

        assertEquals(List.of(), metadata.getRemoteFiles(table, params));
        assertFalse(metadata.getRemoteFilesAsync(table, params).hasMoreOutput());
        assertEquals(List.of(1, 1), listed.stream().map(List::size).toList());
        assertEquals(2, metadata.getPartitions(table, List.of("a", "b")).size(),
                "one name in, one partition out, even though Lake Formation authorized no partitions");
    }

    /** Partitions are read once per attempt from Lake Formation and every later answer comes from that read. */
    @Test
    public void testAPartitionedTableIsAnsweredFromOneAuthorizedRead(@Mocked LakeFormationPartitionReader reader) {
        stubResourceLookup();
        StorageDescriptor sd = StorageDescriptor.builder()
                .location("s3://bucket/db/t/dt=2026-09-09").compressed(false).numberOfBuckets(0)
                .storedAsSubDirectories(false).inputFormat(MAPRED_PARQUET_INPUT_FORMAT_CLASS)
                .serdeInfo(SerDeInfo.builder().serializationLibrary(PARQUET_HIVE_SERDE_CLASS).build()).build();
        new Expectations() {
            {
                reader.readAll((LakeFormationTableIdentity) any, (LakeFormationQuerySession) any, anyString);
                result = List.of(UnfilteredPartition.builder()
                        .partition(software.amazon.awssdk.services.glue.model.Partition.builder()
                                .values("2026-09-09").storageDescriptor(sd).build())
                        .authorizedColumns("id").isRegisteredWithLakeFormation(true).build());
                times = 1;
            }
        };
        PARTITIONED.set(true);
        try {
            metadata(new AtomicInteger(), new AtomicInteger(), null);
            LakeFormationHiveMetadata metadata = metadata(reader);
            Table table = metadata.getTable(context, "db", "t");

            assertEquals(List.of("dt=2026-09-09"), metadata.listPartitionNames("db", "t", null));
            assertEquals(List.of("dt=2026-09-09"),
                    metadata.listPartitionNamesByValue("db", "t", List.of(Optional.of("2026-09-09"))));
            assertEquals(1, metadata.getPartitions(table, List.of("dt=2026-09-09", "dt=gone")).size(),
                    "a partition gone since planning is left out for the caller's size check");
        } finally {
            PARTITIONED.remove();
        }
    }
}
