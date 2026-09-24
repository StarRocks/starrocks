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
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.Table;
import com.starrocks.common.tvr.TvrVersionRange;
import com.starrocks.connector.GetRemoteFilesParams;
import com.starrocks.connector.QueryScopedCredentials;
import com.starrocks.connector.RemoteFileInfo;
import com.starrocks.connector.RemoteFileInfoSource;
import com.starrocks.connector.RemoteFileOperations;
import com.starrocks.connector.TableLoadPurpose;
import com.starrocks.connector.hive.HiveMetadata;
import com.starrocks.connector.hive.HiveMetastoreOperations;
import com.starrocks.connector.share.credential.CloudConfigurationConstants;
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.credential.CloudConfigurationFactory;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.statistics.Statistics;
import com.starrocks.type.IntegerType;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.apache.hadoop.conf.Configuration;
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
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The data plane is closed in this version, and it is closed by two different mechanisms: the entry points
 * that receive a Table object check its type, and the ones that only receive names consult the memo and deny
 * by default. Both are exercised here, including the case that motivated strengthening the first one - a
 * caller handing back a plain HiveTable it obtained earlier.
 */
public class LakeFormationDataPlaneGuardTest {

    /**
     * A handle for a table built directly in a test, standing in for what a real planning attempt would
     * have stamped on it. Tests that care about the attempt boundary open a real scope instead.
     */
    private static LakeFormationTableHandle testHandle(LakeFormationTableIdentity identity) {
        return new LakeFormationTableHandle(identity, TableLoadPurpose.DATA_ACCESS, "test-attempt");
    }

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

    private LakeFormationQueryScope.Scope scope;

    @BeforeEach
    public void setUp() {
        context = new ConnectContext();
        context.setQueryId(UUID.randomUUID());
        context.setThreadLocalInfo();
        // The guards read what this attempt authorized, so the tests have to run inside one.
        scope = LakeFormationQueryScope.open(context);
    }

    @AfterEach
    public void tearDown() {
        if (scope != null) {
            scope.close();
        }
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
                new LakeFormationMetadataGateway(glueClient, lakeFormationClient, properties()), properties(),
                null, Map.of(), null, null, false);
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
                List.of(new Column("id", IntegerType.INT)), IDENTITY, testHandle(IDENTITY));
    }

    // ---- entry points that receive the table object -----------------------------------------------------

    /**
     * Listing a governed table without the credentials this query was issued is refused, not served with
     * whatever the catalog itself can reach. The credentials travel in the request, so an empty request is
     * exactly the shape a mis-wired caller would produce.
     */
    @Test
    public void testListingWithoutCapturedCredentialsIsRefused() {
        stubResourceLookup();
        LakeFormationHiveMetadata metadata = metadata();
        Table table = authorizedTable();
        GetRemoteFilesParams noCredentials = GetRemoteFilesParams.newBuilder().build();

        assertThrows(LakeFormationTableAccessException.class,
                () -> metadata.getRemoteFiles(table, noCredentials));
        assertThrows(LakeFormationTableAccessException.class,
                () -> metadata.getRemoteFilesAsync(table, noCredentials));
    }

    /** Still closed in this version: both are materialized view paths that would need their own listing. */
    @Test
    public void testPartitionDataLayoutStaysRefusedForAGovernedTable() {
        stubResourceLookup();
        LakeFormationHiveMetadata metadata = metadata();

        assertThrows(LakeFormationTableAccessException.class,
                () -> metadata.getHivePartitionDataInfos(authorizedTable(), List.of(), 1));
    }

    /**
     * The partition objects of a governed table are served out of what the attempt authorized, which is
     * what a materialized view refresh needs to record the versions it consumed. Nothing authorized on this
     * attempt still means refused - an empty memo is not "not registered".
     */
    @Test
    public void testAGovernedTablesPartitionObjectsNeedAnAuthorizedSnapshot() {
        stubResourceLookup();
        LakeFormationHiveMetadata metadata = metadata();

        assertThrows(LakeFormationTableAccessException.class,
                () -> metadata.getPartitions(authorizedTable(), List.of("dt=2026-09-09")));
    }

    /**
     * A table this catalog does not govern is served the ordinary way here, and the caller that needs that arrives after planning
     * is over:
     */
    @Test
    public void testAnUngovernedTablesPartitionObjectsAreServedWithNoAttempt() {
        LakeFormationHiveMetadata metadata = metadata();

        assertNotRefusedByLakeFormation(() -> metadata.getPartitions(physicalTable(), List.of("dt=2026-09-09")));
    }

    /** The physical twin of a table this attempt authorized is still refused: it carries no authorization. */
    @Test
    public void testThePhysicalTwinOfAGovernedTableIsRefusedItsPartitions() {
        stubResourceLookup();
        LakeFormationHiveMetadata metadata = metadata();
        seedMemo(metadata, LakeFormationTableResolution.metadataAuthorized(authorizedTable(), null));

        assertThrows(LakeFormationTableAccessException.class,
                () -> metadata.getPartitions(physicalTable(), List.of("dt=2026-09-09")));
    }

    /**
     * A governed table's statistics come from HiveMetadata, so they are what the same table gets in an
     * ordinary hive catalog - neither defaulted nor refused here. Counted at the base call: a delegated
     * default and a short-circuited one look the same from the result.
     */
    @Test
    public void testStatisticsOfAGovernedTableReachTheOrdinaryPath() {
        stubResourceLookup();
        AtomicInteger superCalls = new AtomicInteger();
        new MockUp<HiveMetadata>() {
            @Mock
            public Statistics getTableStatistics(OptimizerContext session, Table table,
                                                 Map<ColumnRefOperator, Column> columns,
                                                 List<PartitionKey> partitionKeys, ScalarOperator predicate,
                                                 long limit, TvrVersionRange version) {
                superCalls.incrementAndGet();
                return Statistics.builder().build();
            }
        };

        assertNotNull(metadata().getTableStatistics(null, authorizedTable(), Map.of(), List.of(), null, -1, null));
        assertEquals(1, superCalls.get(), "a governed table's statistics must fall through to HiveMetadata");
    }

    /**
     * The delegation is for the governed type only. A caller handing back the plain HiveTable it obtained
     * earlier is still refused ahead of super, where HiveMetadata's catch-all would otherwise turn the
     * refusal into silently unknown statistics.
     */
    @Test
    public void testStatisticsAreRefusedWhenHandedThePlainPhysicalTable() {
        stubResourceLookup();
        LakeFormationHiveMetadata metadata = metadata();
        seedMemo(metadata, LakeFormationTableResolution.metadataAuthorized(authorizedTable(), null));

        assertThrows(LakeFormationTableAccessException.class,
                () -> metadata.getTableStatistics(null, physicalTable(), Map.of(), List.of(), null, -1, null));
    }

    /**
     * Refreshing a governed table does nothing rather than failing. INSERT ... SELECT refreshes its source
     * tables before planning and does so by default, so a refusal here would make "read a governed table,
     * write the result elsewhere" fail out of the box.
     */
    @Test
    public void testRefreshIsANoOpForAGovernedTable() {
        stubResourceLookup();
        metadata().refreshTable("db", authorizedTable(), List.of(), false);
    }

    /**
     * The case the type check alone would miss. A caller that kept the plain HiveTable it was given earlier
     * - the statistics collector does exactly this - would otherwise walk straight through, and refreshTable
     * is the one entry point whose result lands in the cross-query table cache.
     */
    @Test
    public void testRefreshIsRefusedEvenWhenHandedThePlainPhysicalTable() {
        stubResourceLookup();
        LakeFormationHiveMetadata metadata = metadata();
        seedMemo(metadata, LakeFormationTableResolution.metadataAuthorized(authorizedTable(), null));

        assertThrows(LakeFormationTableAccessException.class,
                () -> metadata.refreshTable("db", physicalTable(), List.of(), false));
    }

    /**
     * An explicitly unregistered table keeps behaving exactly as it does without Lake Formation. super is
     * not wired in this fixture, so the assertion is "the refusal is not a Lake Formation one" rather than
     * "nothing is thrown".
     */
    @Test
    public void testUnregisteredTableIsNotRefusedByLakeFormation() {
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

    /**
     * A table this catalog does not govern is an AWS Glue resource - Lake Formation enforces nothing on it - so it is read from
     * the catalog's own configuration like any other Hive table.
     */
    @Test
    public void testCloudConfigurationServesTheTablesLakeFormationDoesNotGovern() {
        assertNotRefusedByLakeFormation(() -> metadata().getCloudConfiguration());
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

    /**
     * Records a decision in the open attempt, as though Lake Formation had already been asked. Goes through
     * the scope's own resolve() rather than reaching into a field, so the key normalization used everywhere
     * else applies here too.
     */
    private static void seedMemo(LakeFormationHiveMetadata metadata, LakeFormationTableResolution resolution) {
        LakeFormationQueryScope.current().orElseThrow()
                .resolve(IDENTITY, TableLoadPurpose.DATA_ACCESS, key -> resolution);
    }

    /**
     * Credentials of some other kind riding in the request are refused rather than used. Nothing upstream
     * guarantees the object in that slot came from Lake Formation - the field is typed by the neutral
     * interface - so this is the only place the distinction is made.
     */
    @Test
    public void testListingWithCredentialsNotIssuedByLakeFormationIsRefused() {
        stubResourceLookup();
        LakeFormationHiveMetadata metadata = metadata();
        Table table = authorizedTable();
        GetRemoteFilesParams foreignCredentials = GetRemoteFilesParams.newBuilder()
                .setQueryScopedCredentials(new QueryScopedCredentials() {
                    @Override
                    public boolean matches(ConnectContext context) {
                        return true;
                    }

                    @Override
                    public CloudConfiguration cloudConfiguration(ConnectContext context) {
                        return vendedCredential();
                    }

                    @Override
                    public CredentialAdmission admitForExecution(java.time.Instant requestedDeadline) {
                        return new CredentialAdmission(requestedDeadline);
                    }
                })
                .build();

        assertThrows(LakeFormationTableAccessException.class,
                () -> metadata.getRemoteFiles(table, foreignCredentials));
        assertThrows(LakeFormationTableAccessException.class,
                () -> metadata.getRemoteFilesAsync(table, foreignCredentials));
    }

    // ---- the listing's own lifetime -------------------------------------------------------------------

    private static CloudConfiguration vendedCredential() {
        return CloudConfigurationFactory.buildCloudConfigurationForStorage(Map.of(
                CloudConfigurationConstants.AWS_S3_ACCESS_KEY, "AKIAEXAMPLETESTKEY",
                CloudConfigurationConstants.AWS_S3_SECRET_KEY, "wJalrXUtnFEMIsecretKEYtestVALUE",
                CloudConfigurationConstants.AWS_S3_SESSION_TOKEN, "FwoGZXIvYXdzTESTsessionTOKEN",
                CloudConfigurationConstants.AWS_S3_REGION, "us-west-2"));
    }

    private static LakeFormationFileListing openListing() {
        return LakeFormationFileListing.open(vendedCredential(), new Configuration(), null, false);
    }

    /**
     * A listing is opened on one table's vended credentials, and those credentials - not the catalog's -
     * are what its file systems are configured with. Closing it is what releases them.
     */
    @Test
    public void testAListingIsOpenedOnTheVendedCredentialsAndCanBeClosed() {
        LakeFormationFileListing listing = openListing();

        assertNotNull(listing.operations(), "the listing carries the operations the enumeration runs on");

        listing.close();
        listing.close();
    }

    /**
     * An asynchronous listing outlives the call that started it, so the file systems have to stay open until
     * the consumer is done. Ownership therefore travels with the source - and the source releases them the
     * moment there is no more output, without waiting to be closed explicitly.
     */
    @Test
    public void testAnExhaustedAsyncSourceReleasesTheListingByItself() {
        AtomicInteger delegateCloses = new AtomicInteger();
        RemoteFileInfoSource delegate = new RemoteFileInfoSource() {
            private int remaining = 1;

            @Override
            public RemoteFileInfo getOutput() {
                remaining--;
                return null;
            }

            @Override
            public boolean hasMoreOutput() {
                return remaining > 0;
            }

            @Override
            public void close() {
                delegateCloses.incrementAndGet();
            }
        };

        LakeFormationFileListing listing = openListing();
        LakeFormationRemoteFileInfoSource source = new LakeFormationRemoteFileInfoSource(delegate, listing);

        assertTrue(source.hasMoreOutput());
        assertEquals(0, delegateCloses.get(), "nothing is released while output remains");

        source.getOutput();

        assertFalse(source.hasMoreOutput());
        assertEquals(1, delegateCloses.get(), "running out of output is what ends the listing");
    }

    /** A per-bucket credential inherited from the catalog would override the vended one, so it is dropped. */
    @Test
    public void testPerBucketCredentialsFromTheCatalogAreDropped() {
        Configuration base = new Configuration(false);
        base.set("fs.s3a.bucket.my.bucket.access.key", "catalog-key");
        base.set("fs.s3a.bucket.my.bucket.aws.credentials.provider", "catalog-provider");
        base.set("fs.s3a.bucket.my.bucket.assumed.role.arn", "arn:aws:iam::1:role/catalog");
        base.set("fs.s3a.bucket.my.bucket.endpoint", "s3.example.internal");

        Configuration cleaned = LakeFormationFileListing.withoutPerBucketCredentials(base);

        assertNull(cleaned.get("fs.s3a.bucket.my.bucket.access.key"));
        assertNull(cleaned.get("fs.s3a.bucket.my.bucket.aws.credentials.provider"));
        assertNull(cleaned.get("fs.s3a.bucket.my.bucket.assumed.role.arn"));
        assertEquals("s3.example.internal", cleaned.get("fs.s3a.bucket.my.bucket.endpoint"),
                "addressing stays: it says where the bucket is, not who is asking");
    }

    /** A listing that fails is over as well, so it releases before the failure reaches the caller. */
    @Test
    public void testAFailingAsyncSourceReleasesTheListing() {
        AtomicInteger delegateCloses = new AtomicInteger();
        RemoteFileInfoSource delegate = new RemoteFileInfoSource() {
            @Override
            public RemoteFileInfo getOutput() {
                throw new IllegalStateException("listing failed");
            }

            @Override
            public boolean hasMoreOutput() {
                throw new IllegalStateException("listing failed");
            }

            @Override
            public void close() {
                delegateCloses.incrementAndGet();
            }
        };
        LakeFormationRemoteFileInfoSource source = new LakeFormationRemoteFileInfoSource(delegate, openListing());

        assertThrows(IllegalStateException.class, source::hasMoreOutput);
        assertThrows(IllegalStateException.class, source::getOutput);
        assertEquals(1, delegateCloses.get(), "released once, on the first failure");
    }

    /**
     * A delegate that refuses to close must still leave the file systems released - otherwise one bad
     * consumer leaks them for the life of the process.
     */
    @Test
    public void testADelegateThatRefusesToCloseStillReleasesTheListing() {
        RemoteFileInfoSource delegate = new RemoteFileInfoSource() {
            @Override
            public RemoteFileInfo getOutput() {
                return null;
            }

            @Override
            public boolean hasMoreOutput() {
                return false;
            }

            @Override
            public void close() {
                throw new IllegalStateException("consumer already gone");
            }
        };

        LakeFormationRemoteFileInfoSource source =
                new LakeFormationRemoteFileInfoSource(delegate, openListing());

        source.close();
    }

}
