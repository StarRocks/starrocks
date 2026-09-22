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
package com.starrocks.common.lock;

import com.aliyun.odps.Odps;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.table.read.TableReadSessionBuilder;
import com.google.common.collect.ImmutableList;
import com.starrocks.catalog.OdpsTable;
import com.starrocks.common.Config;
import com.starrocks.common.util.concurrent.lock.LockInvariantViolationException;
import com.starrocks.common.util.concurrent.lock.LockManager;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.connector.GetRemoteFilesParams;
import com.starrocks.connector.HdfsEnvironment;
import com.starrocks.connector.RemoteFileOperations;
import com.starrocks.connector.RemoteFileScanContext;
import com.starrocks.connector.RemotePathKey;
import com.starrocks.connector.delta.DeltaLakeJsonHandler;
import com.starrocks.connector.delta.DeltaLakeParquetHandler;
import com.starrocks.connector.delta.TraceDefaultJsonHandler;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.fluss.FlussMetadata;
import com.starrocks.connector.hive.HiveUtils;
import com.starrocks.connector.hudi.HudiRemoteFileIO;
import com.starrocks.connector.iceberg.IcebergCatalog;
import com.starrocks.connector.iceberg.IcebergCatalogType;
import com.starrocks.connector.iceberg.glue.IcebergGlueCatalog;
import com.starrocks.connector.iceberg.hadoop.IcebergHadoopCatalog;
import com.starrocks.connector.iceberg.hive.IcebergHiveCatalog;
import com.starrocks.connector.iceberg.io.IcebergCachingFileIO;
import com.starrocks.connector.iceberg.jdbc.IcebergJdbcCatalog;
import com.starrocks.connector.iceberg.rest.IcebergRESTCatalog;
import com.starrocks.connector.kudu.KuduMetadata;
import com.starrocks.connector.odps.OdpsMetadata;
import com.starrocks.connector.odps.OdpsProperties;
import com.starrocks.server.GlobalStateMgr;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;

/**
 * The doors added for the connectors that had none: delta lake, hudi, odps, fluss, and the iceberg
 * catalogs that do not go through HMS or REST.
 * <p>
 * Each case takes a real metadata lock, runs the check in {@code error} mode, and calls the door. The
 * refusal is the assertion: it proves the guard is reached <em>before</em> the request, which is the
 * whole property -- a guard placed after the wait would report a round trip that already happened.
 * Running in error mode is also what keeps the test offline; none of these calls may actually leave
 * the machine.
 * <p>
 * Reverse verification: removing any one of the guards turns its case from a refusal into a real
 * connection attempt, i.e. a failure (or a hang) rather than a pass.
 */
public class ConnectorTransportGateTest {
    private static final long INTERNAL_DB_ID = 20001L;

    private String savedMode;

    @BeforeEach
    public void setUp() {
        GlobalStateMgr.getCurrentState().setLockManager(new LockManager());
        savedMode = Config.lock_blocking_call_validation_mode;
        Config.lock_blocking_call_validation_mode = "error";
    }

    @AfterEach
    public void tearDown() {
        Config.lock_blocking_call_validation_mode = savedMode;
    }

    /** Runs the door under a database READ lock and asserts it was refused, naming the transport. */
    private void assertRefusedUnderLock(String transport, Executable door) {
        Locker locker = new Locker();
        locker.lockDatabase(INTERNAL_DB_ID, LockType.READ);
        try {
            IllegalStateException e = Assertions.assertThrows(IllegalStateException.class, door);
            Assertions.assertTrue(e.getMessage().contains(transport),
                    "the report should name the transport, but said: " + e.getMessage());
            Assertions.assertTrue(e.getMessage().contains("blocking_call_under_lock"), e.getMessage());
        } finally {
            locker.unLockDatabase(INTERNAL_DB_ID, LockType.READ);
        }
    }

    /**
     * Hudi's only door that does not pass through the HMS client: reading the table's timeline and
     * listing a partition's file slices.
     */
    @Test
    public void testHudiFileListingIsGuarded() {
        HudiRemoteFileIO fileIO = new HudiRemoteFileIO(new Configuration());
        // A well-formed key on purpose: the guard sits after this method's local validation, so a null
        // or location-less key must be refused by that validation and never reach the guard.
        RemotePathKey pathKey = RemotePathKey.of("s3://bucket/db/tbl/dt=20260101", false);
        pathKey.setScanContext(new RemoteFileScanContext("s3://bucket/db/tbl"));
        assertRefusedUnderLock("remote-storage", () -> fileIO.getRemoteFiles(pathKey));
    }

    /** The other half of that: a key with nothing to list is an argument error, not a blocking call. */
    @Test
    public void testHudiWithoutATableLocationIsNotReportedAsABlockingCall() {
        HudiRemoteFileIO fileIO = new HudiRemoteFileIO(new Configuration());
        Locker locker = new Locker();
        locker.lockDatabase(INTERNAL_DB_ID, LockType.READ);
        try {
            Assertions.assertThrows(StarRocksConnectorException.class,
                    () -> fileIO.getRemoteFiles(RemotePathKey.of("s3://bucket/db/tbl/dt=20260101", false)));
        } finally {
            locker.unLockDatabase(INTERNAL_DB_ID, LockType.READ);
        }
    }

    /**
     * Delta reads its log through the kernel, which reaches storage by itself rather than through the
     * FE's file-system layer, so the FE-owned handlers are the only doors there are. Both the cached
     * and the uncached handler are covered, because which one is used is a config switch.
     */
    @Test
    public void testDeltaLakeJsonReadIsGuarded() {
        assertRefusedUnderLock("remote-storage",
                () -> DeltaLakeJsonHandler.readJsonFile("s3://bucket/_delta_log/1.json", new Configuration()));
    }

    @Test
    public void testDeltaLakeCheckpointReadIsGuarded() {
        assertRefusedUnderLock("remote-storage",
                () -> DeltaLakeParquetHandler.readParquetFile("s3://bucket/_delta_log/1.checkpoint.parquet",
                        1L, 1L, null, new Configuration()));
    }

    @Test
    public void testDeltaLakeUncachedJsonReadIsGuarded() {
        TraceDefaultJsonHandler handler = new TraceDefaultJsonHandler(new Configuration());
        assertRefusedUnderLock("remote-storage", () -> {
            CloseableIterator<FileStatus> files =
                    oneFile(FileStatus.of("s3://bucket/_delta_log/1.json", 1L, 1L));
            handler.readJsonFiles(files, null, Optional.empty()).hasNext();
        });
    }

    /**
     * ODPS holds a third-party client and calls it directly, so the doors are the methods that call
     * it. This one is uncached: a security-manager query plus a project listing, every time.
     */
    @Test
    public void testOdpsProjectListingIsGuarded() {
        OdpsMetadata metadata = odpsMetadata();
        assertRefusedUnderLock("odps", () -> metadata.listDbNames(null));
    }

    private static OdpsMetadata odpsMetadata() {
        Odps odps = new Odps(new AliyunAccount("ak", "sk"));
        odps.setEndpoint("http://127.0.0.1");
        odps.setDefaultProject("project");
        Map<String, String> properties = new HashMap<>();
        properties.put(OdpsProperties.ACCESS_ID, "ak");
        properties.put(OdpsProperties.ACCESS_KEY, "sk");
        properties.put(OdpsProperties.ENDPOINT, "http://127.0.0.1");
        properties.put(OdpsProperties.PROJECT, "project");
        properties.put(OdpsProperties.TUNNEL_QUOTA, "pay-as-you-go");
        properties.put(OdpsProperties.SPLIT_POLICY, OdpsProperties.SIZE);
        return new OdpsMetadata(odps, "odps_cat", null, new OdpsProperties(properties));
    }

    /**
     * The ODPS caches load on the calling thread, and {@code OdpsMetadata.get} answers null to anything
     * the loader throws -- so a refusal there used to come back as "no such table", which is a wrong
     * answer rather than a refused call. The refusal has to survive Guava's wrapping.
     */
    @Test
    public void testOdpsCacheLoaderRefusalIsNotTurnedIntoATableThatDoesNotExist() {
        OdpsMetadata metadata = odpsMetadata();
        Locker locker = new Locker();
        locker.lockDatabase(INTERNAL_DB_ID, LockType.READ);
        try {
            Assertions.assertThrows(LockInvariantViolationException.class,
                    () -> metadata.getTable(null, "project", "tbl"));
        } finally {
            locker.unLockDatabase(INTERNAL_DB_ID, LockType.READ);
        }
    }

    /**
     * The mechanism the two cache layers rely on: a refusal buried under someone else's wrapper is still
     * a refusal, and rethrowing it is what keeps a generic catch from turning a lock violation into a
     * storage error or a missing table.
     */
    @Test
    public void testARefusalSurvivesBeingWrapped() {
        LockInvariantViolationException refusal = new LockInvariantViolationException("refused");
        RuntimeException wrapped = new RuntimeException("outer", new IllegalStateException("middle", refusal));

        Assertions.assertTrue(LockInvariantViolationException.isRefusal(wrapped));
        Assertions.assertSame(refusal,
                Assertions.assertThrows(LockInvariantViolationException.class,
                        () -> LockInvariantViolationException.rethrowIfRefusal(wrapped)));

        RuntimeException unrelated = new RuntimeException("outer", new IllegalStateException("not a refusal"));
        Assertions.assertFalse(LockInvariantViolationException.isRefusal(unrelated));
        Assertions.assertDoesNotThrow(() -> LockInvariantViolationException.rethrowIfRefusal(unrelated));

        // A cause chain that loops back on itself. Java forbids initCause(this) but not a -> b -> a, and
        // wrapper layers do build those, so the walk has to stop rather than spin.
        RuntimeException first = new RuntimeException("first");
        RuntimeException second = new RuntimeException("second", first);
        first.initCause(second);
        Assertions.assertFalse(LockInvariantViolationException.isRefusal(first));
        Assertions.assertDoesNotThrow(() -> LockInvariantViolationException.rethrowIfRefusal(first));

        // And the same loop with a refusal inside it, which still has to be found.
        LockInvariantViolationException buried = new LockInvariantViolationException("refused");
        RuntimeException loopHead = new RuntimeException("head", buried);
        buried.initCause(loopHead);
        Assertions.assertSame(buried,
                Assertions.assertThrows(LockInvariantViolationException.class,
                        () -> LockInvariantViolationException.rethrowIfRefusal(loopHead)));
    }

    /** Fluss sends asynchronously and waits on the future, so the door is the call site. */
    @Test
    public void testFlussAdminRequestIsGuarded() {
        FlussMetadata metadata = new FlussMetadata("fluss_cat", null, null, null);
        assertRefusedUnderLock("fluss", () -> metadata.listDbNames(null));
    }

    /**
     * The iceberg catalogs that do not go through a guarded FE client of their own: glue and jdbc talk to
     * their systems directly, hadoop is a directory tree on storage, and iceberg's own HiveCatalog keeps
     * its own metastore client pool rather than using the FE's.
     *
     * <p>Every guarded method of each is called, not one of them, because what has to hold for all of them
     * is the same property: the guard is the first thing the method does. A door added below a request, or
     * below a {@code try} that rewrites the refusal, would pass a test that only called its neighbour.
     *
     * <p>The instance is a mock whose methods are the real ones -- their actual constructors contact the
     * system they wrap, which is exactly what must not happen here -- so every field is null and the
     * refusal is raised before anything could dereference one.
     */
    @Test
    public void testEveryDoorOfTheIcebergCatalogsRefuses() {
        assertEveryDoorRefuses(realMethodsOn(IcebergGlueCatalog.class), "iceberg-glue");
        assertEveryDoorRefuses(realMethodsOn(IcebergJdbcCatalog.class), "iceberg-jdbc");
        // A hadoop catalog is a directory tree, so its requests are storage, not a catalog service.
        assertEveryDoorRefuses(realMethodsOn(IcebergHadoopCatalog.class), "remote-storage");
        IcebergHiveCatalog hive = realMethodsOn(IcebergHiveCatalog.class);
        assertEveryDoorRefuses(hive, "hive-metastore");
        // And the four this catalog has of its own, on top of the common set: views and namespace metadata
        // are HMS requests too.
        assertRefusedUnderLock("hive-metastore", () -> hive.getViewBuilder(null, TableIdentifier.of("db", "v")));
        assertRefusedUnderLock("hive-metastore", () -> hive.dropView(null, "db", "v"));
        assertRefusedUnderLock("hive-metastore", () -> hive.getView(null, "db", "v"));
        assertRefusedUnderLock("hive-metastore", () -> hive.loadNamespaceMetadata(null, Namespace.of("db")));
    }

    private static <T extends IcebergCatalog> T realMethodsOn(Class<T> catalogClass) {
        return Mockito.mock(catalogClass, Mockito.withSettings().defaultAnswer(Mockito.CALLS_REAL_METHODS));
    }

    /** Calls every guarded method of one catalog and asserts each was refused. */
    private void assertEveryDoorRefuses(IcebergCatalog catalog, String transport) {
        Map<String, String> withLocation = new HashMap<>();
        withLocation.put("location", "s3://bucket/db");

        assertRefusedUnderLock(transport, () -> catalog.getTable(null, "db", "tbl"));
        assertRefusedUnderLock(transport, () -> catalog.tableExists(null, "db", "tbl"));
        assertRefusedUnderLock(transport, () -> catalog.listAllDatabases(null));
        assertRefusedUnderLock(transport, () -> catalog.getDB(null, "db"));
        assertRefusedUnderLock(transport, () -> catalog.listTables(null, "db"));
        assertRefusedUnderLock(transport, () -> catalog.dropTable(null, "db", "tbl", true));
        assertRefusedUnderLock(transport, () -> catalog.renameTable(null, "db", "tbl", "tbl2"));
        assertRefusedUnderLock(transport, () -> catalog.registerTable(null, "db", "tbl", "s3://bucket/m.json"));
        assertRefusedUnderLock(transport,
                () -> catalog.createTable(null, "db", "tbl", null, null, "s3://bucket/db/tbl", null, withLocation));
        // dropDB resolves the database first, and that resolve is what refuses; the refusal has to come
        // back out of the catch that otherwise turns everything into "Failed to access database".
        assertRefusedUnderLock(transport, () -> catalog.dropDB(null, "db"));
        // These two are storage whichever catalog it is: a location check and the cleanup of a failed commit.
        assertRefusedUnderLock("remote-storage", () -> catalog.createDB(null, "db", withLocation));
        assertRefusedUnderLock("remote-storage",
                () -> catalog.deleteUncommittedDataFiles(ImmutableList.of("s3://bucket/db/tbl/f.parquet")));
    }

    /** The REST catalog's two storage doors; its own requests are guarded in withAuthRecovery. */
    @Test
    public void testTheRestCatalogStorageDoorsRefuse() {
        Map<String, String> withLocation = new HashMap<>();
        withLocation.put("location", "s3://bucket/db");
        IcebergRESTCatalog catalog = realMethodsOn(IcebergRESTCatalog.class);
        assertRefusedUnderLock("remote-storage", () -> catalog.createDB(null, "db", withLocation));
        assertRefusedUnderLock("remote-storage",
                () -> catalog.deleteUncommittedDataFiles(ImmutableList.of("s3://bucket/db/tbl/f.parquet")));
    }

    /**
     * The FileIO every iceberg catalog installs: manifest lists, manifests and metadata.json are read
     * through it, which is the path a scan spends its time in and the one that never passed through
     * MetadataMgr.
     */
    @Test
    public void testIcebergFileIoReadIsGuarded() {
        IcebergCachingFileIO fileIO = new IcebergCachingFileIO();
        fileIO.setConf(new Configuration());
        fileIO.initialize(new HashMap<>());
        assertRefusedUnderLock("remote-storage",
                () -> fileIO.newInputFile("file:///tmp/starrocks-no-such-file.avro").getLength());
        // The other read door of the same handle: asked whether the file is there at all.
        assertRefusedUnderLock("remote-storage",
                () -> fileIO.newInputFile("file:///tmp/starrocks-no-such-file.avro").exists());
    }

    /**
     * The delete side. There is deliberately no write-side case: {@code newOutputFile} hands back the
     * object iceberg dispatches on -- {@code ParquetIO} looks for {@code HadoopOutputFile}, and for
     * {@code NativelyEncryptedFile} -- so it cannot be wrapped to add a guard without moving the writer
     * onto another path, which an observability change must not do.
     */
    @Test
    public void testIcebergFileIoDeleteIsGuarded() {
        IcebergCachingFileIO fileIO = new IcebergCachingFileIO();
        fileIO.setConf(new Configuration());
        fileIO.initialize(new HashMap<>());
        assertRefusedUnderLock("remote-storage",
                () -> fileIO.deleteFile("file:///tmp/starrocks-no-such-file.avro"));
    }

    /**
     * Hive's own storage helper, the one the sink's commit and the DDL paths reach storage through. One
     * method stands for the seven: they are the same shape, and what the test pins is that the class has
     * doors at all -- it had none, so a rename or a cleanup under a database lock was invisible.
     */
    @Test
    public void testHiveStorageHelperIsGuarded() {
        assertRefusedUnderLock("remote-storage",
                () -> HiveUtils.pathExists(new Path("s3://bucket/db/tbl"), new Configuration()));
        assertRefusedUnderLock("remote-storage",
                () -> HiveUtils.deleteIfExists(new Path("s3://bucket/db/tbl/f"), false, new Configuration()));
        assertRefusedUnderLock("remote-storage",
                () -> HiveUtils.createDirectoryIfNotExists(new Path("s3://bucket/db/tbl/d"), new Configuration()));
    }

    /** All seven of hive's storage helpers, for the reason the catalog sweep above gives. */
    @Test
    public void testEveryHiveStorageHelperDoorRefuses() {
        Configuration conf = new Configuration();
        Path path = new Path("s3://bucket/db/tbl");
        assertRefusedUnderLock("remote-storage", () -> HiveUtils.pathExists(path, conf));
        assertRefusedUnderLock("remote-storage", () -> HiveUtils.isDirectory(path, conf));
        assertRefusedUnderLock("remote-storage", () -> HiveUtils.isEmpty(path, conf));
        assertRefusedUnderLock("remote-storage", () -> HiveUtils.createDirectory(path, conf));
        assertRefusedUnderLock("remote-storage", () -> HiveUtils.deleteIfExists(path, false, conf));
        assertRefusedUnderLock("remote-storage", () -> HiveUtils.createDirectoryIfNotExists(path, conf));
        // The one that takes a file system rather than resolving one; null is never dereferenced because
        // the guard comes first, which is the property under test.
        assertRefusedUnderLock("remote-storage", () -> HiveUtils.checkedDelete(null, path, false));
    }

    /** The listing side of the hive sink's commit path. */
    @Test
    public void testHiveRemoteFileOperationsListingIsGuarded() {
        RemoteFileOperations operations =
                new RemoteFileOperations(null, null, null, false, false, new Configuration());
        assertRefusedUnderLock("remote-storage", () -> operations.listStatus(new Path("s3://bucket/db/tbl")));
        assertRefusedUnderLock("remote-storage",
                () -> operations.removeNotCurrentQueryFiles(new Path("s3://bucket/db/tbl"), "query-id-1234567890"));
        assertRefusedUnderLock("remote-storage",
                () -> operations.renameDirectory(new Path("s3://bucket/a"), new Path("s3://bucket/b"), () -> {
                }));
    }

    /**
     * Kudu's doors, on the branches that actually call the client: an HMS-backed catalog answers from the
     * metastore, whose own client carries the hive-metastore guard, and the schema-emulation-off branches
     * answer from a constant or a map -- neither is a wait, and neither may be reported as one.
     */
    @Test
    public void testEveryKuduDoorRefuses() {
        // No metastore and emulation on, so every call below goes to the client. Building the client is
        // local; the guard is what stops it before the first request.
        KuduMetadata metadata = new KuduMetadata("kudu_cat", new HdfsEnvironment(), "localhost:7051",
                true, "", Optional.empty());
        assertRefusedUnderLock("kudu", () -> metadata.listDbNames(null));
        assertRefusedUnderLock("kudu", () -> metadata.listTableNames(null, "db"));
        assertRefusedUnderLock("kudu", () -> metadata.getDb(null, "db"));
        assertRefusedUnderLock("kudu", () -> metadata.getTable(null, "db", "tbl"));
    }

    /**
     * ODPS split planning. The door is inside the two policy-specific methods, which only a policy the
     * switch recognized reaches -- and the refusal has to come back out of the catch that otherwise turns
     * everything into "Encounter error when try to split".
     */
    @Test
    public void testOdpsSplitPlanningRefusalSurvivesTheCatch() {
        OdpsMetadata metadata = odpsMetadata();
        OdpsTable table = Mockito.mock(OdpsTable.class);
        Mockito.when(table.getFullSchema()).thenReturn(ImmutableList.of());
        Mockito.when(table.getCatalogDBName()).thenReturn("project");
        Mockito.when(table.getCatalogTableName()).thenReturn("tbl");
        GetRemoteFilesParams params = GetRemoteFilesParams.newBuilder().setFieldNames(ImmutableList.of()).build();
        assertRefusedUnderLock("odps",
                () -> metadata.getRemoteFiles(table, params, new TableReadSessionBuilder()));
    }

    /** Fluss's remaining admin doors, same shape as the listDbNames one above. */
    @Test
    public void testEveryFlussAdminDoorRefuses() {
        FlussMetadata metadata = new FlussMetadata("fluss_cat", null, null, null);
        assertRefusedUnderLock("fluss", () -> metadata.listDbNames(null));
        assertRefusedUnderLock("fluss", () -> metadata.listTableNames(null, "db"));
        assertRefusedUnderLock("fluss", () -> metadata.getDb(null, "db"));
        assertRefusedUnderLock("fluss", () -> metadata.getTable(null, "db", "tbl"));
        assertRefusedUnderLock("fluss", () -> metadata.listPartitionNames("db", "tbl", null));
    }

    /**
     * The transport a catalog's requests are reported under is a property of its type, and it has to be
     * the same name the rest of the FE uses for that system -- a hive-backed iceberg catalog reports as
     * the metastore, a hadoop one as storage -- or "lock-held time by transport" stops adding up.
     */
    @Test
    public void testEveryIcebergCatalogTypeNamesItsTransport() {
        Assertions.assertEquals("iceberg-glue", IcebergCatalogType.GLUE_CATALOG.transportTag());
        Assertions.assertEquals("iceberg-rest", IcebergCatalogType.REST_CATALOG.transportTag());
        Assertions.assertEquals("iceberg-jdbc", IcebergCatalogType.JDBC_CATALOG.transportTag());
        Assertions.assertEquals("remote-storage", IcebergCatalogType.HADOOP_CATALOG.transportTag());
        Assertions.assertEquals("hive-metastore", IcebergCatalogType.HIVE_CATALOG.transportTag());
        Assertions.assertEquals("iceberg-catalog", IcebergCatalogType.CUSTOM_CATALOG.transportTag());
        Assertions.assertEquals("iceberg-catalog", IcebergCatalogType.UNKNOWN.transportTag());
    }

    /** A one-element delta-kernel iterator; the handler asks it for a file and opens that file. */
    private static CloseableIterator<FileStatus> oneFile(FileStatus file) {
        return new CloseableIterator<FileStatus>() {
            private boolean consumed;

            @Override
            public boolean hasNext() {
                return !consumed;
            }

            @Override
            public FileStatus next() {
                if (consumed) {
                    throw new NoSuchElementException();
                }
                consumed = true;
                return file;
            }

            @Override
            public void close() {
            }
        };
    }
}
