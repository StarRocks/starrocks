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

package com.starrocks.connector.delta;

import com.starrocks.common.Config;
import com.starrocks.common.profile.Tracers;
import io.delta.kernel.Table;
import io.delta.kernel.internal.util.FileNames;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;
import io.delta.storage.AzureLogStore;
import mockit.Expectations;
import mockit.Injectable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathIOException;
import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystemStore;
import org.apache.hadoop.fs.azurebfs.constants.AbfsServiceType;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsRestOperationException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.InvalidAbfsRestOperationException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.InvalidUriException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.SASTokenProviderException;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;
import org.apache.hadoop.fs.azurebfs.utils.TracingHeaderFormat;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.URI;
import java.nio.file.AccessDeniedException;
import java.nio.file.Files;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DeltaLakeAzureLogStoreTest {
    private static final long VERSION = 712884;
    private static final String VERSION_PREFIX = "00000000000000712884";
    // Kernel lists from "<20-digit version>." (FileNames.listingPrefix), which need not exist itself.
    private static final String START_FILE = startFrom(new Path("/_delta_log"), VERSION).getName();

    @BeforeEach
    public void setUpTracers() {
        Tracers.register();
        Tracers.init(Tracers.Mode.TIMER, Tracers.Module.EXTERNAL, true, false);
    }

    @AfterEach
    public void closeTracers() {
        Tracers.close();
    }

    @ParameterizedTest
    @ValueSource(strings = {"abfs", "abfss"})
    public void testAzureListingStartsAtCheckpoint(String scheme, @Injectable AzureBlobFileSystemStore store,
                                                  @Injectable AbfsConfiguration abfsConf) throws IOException {
        Configuration conf = new Configuration(false);
        Path logDir = new Path(scheme + "://container@account.dfs.core.windows.net/table/_delta_log");
        bindAzure(conf, store, abfsConf);
        assertEquals(VERSION_PREFIX + ".", START_FILE);
        // Keep every file at the requested version (including checkpoint parts), checksums, and the
        // later commit, in lexical order.
        String checkpoint = VERSION_PREFIX + ".checkpoint.0000000001.0000000002.parquet";
        String checkpointPart2 = VERSION_PREFIX + ".checkpoint.0000000002.0000000002.parquet";
        String checksum = VERSION_PREFIX + ".crc";
        String laterCommit = "00000000000000712902.json";
        new Expectations() {
            {
                store.listStatus(logDir, START_FILE, (TracingContext) any);
                result = new org.apache.hadoop.fs.FileStatus[] {
                        status(logDir, laterCommit), status(logDir, checksum), status(logDir, checkpointPart2),
                        status(logDir, checkpoint)};
                times = 1;
            }
        };

        DeltaLakeEngine engine = newEngine(conf);
        try (CloseableIterator<FileStatus> files = engine.getFileSystemClient()
                .listFrom(startFrom(logDir, VERSION).toString())) {
            List<FileStatus> listed = files.toInMemoryList();
            assertEquals(List.of(checkpoint, checkpointPart2, checksum, laterCommit),
                    listed.stream().map(file -> new Path(file.getPath()).getName()).toList());
            assertEquals(new Path(logDir, checkpoint).toString(), listed.get(0).getPath());
            assertEquals(123, listed.get(0).getSize());
            assertEquals(456, listed.get(0).getModificationTime());
        }
        assertEquals(Map.of("DeltaLake.listFrom.azureFiles", 4L), listingCounters());
        assertTrue(Tracers.printScopeTimer().contains("DeltaLake.listFrom"));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testEmptyAzureListingFallsBackToFullListing(boolean logDirExists, @Injectable AzureBlobFileSystemStore store,
                                                           @Injectable AbfsConfiguration abfsConf) throws IOException {
        Configuration conf = new Configuration(false);
        Path logDir = new Path("abfss://container@account.dfs.core.windows.net/table/_delta_log");
        bindAzure(conf, store, abfsConf);
        new Expectations() {
            {
                store.listStatus(logDir, START_FILE, (TracingContext) any);
                result = new org.apache.hadoop.fs.FileStatus[0];
            }
        };
        // An empty result means a misread start token or a missing directory (flat-namespace DFS lists
        // it as empty); the full listing recovers the former and reports the latter.
        TestAzureFileSystem.fullListing = new org.apache.hadoop.fs.FileStatus[] {status(logDir, VERSION_PREFIX + ".json")};
        TestAzureFileSystem.logDirExists = logDirExists;

        DeltaLakeAzureLogStore logStore = new DeltaLakeAzureLogStore(conf);
        if (logDirExists) {
            assertEquals(List.of(VERSION_PREFIX + ".json"), names(logStore.listFrom(startFrom(logDir, VERSION), conf)));
        } else {
            assertThrows(FileNotFoundException.class, () -> logStore.listFrom(startFrom(logDir, VERSION), conf));
        }
        assertEquals(Map.of("DeltaLake.listFrom.azureFiles", 0L, "DeltaLake.listFrom.azureEmptyFallback", 1L),
                listingCounters());
        assertTrue(Tracers.printScopeTimer().contains("DeltaLake.listFrom"));
    }

    @Test
    public void testFullListingFailureSurfacesOnce(@Injectable AzureBlobFileSystemStore store,
                                                  @Injectable AbfsConfiguration abfsConf) throws IOException {
        Configuration conf = new Configuration(false);
        Path logDir = new Path("abfss://container@account.dfs.core.windows.net/table/_delta_log");
        bindAzure(conf, store, abfsConf);
        new Expectations() {
            {
                store.listStatus(logDir, START_FILE, (TracingContext) any);
                result = new org.apache.hadoop.fs.FileStatus[0];
            }
        };
        IOException failure = new PathIOException(logDir.toString(), "full listing failed");
        TestAzureFileSystem.fullListing = new org.apache.hadoop.fs.FileStatus[0];
        TestAzureFileSystem.fullListingFailure = failure;

        // The fallback's own failure is not mistaken for a startFrom failure and retried.
        assertSame(failure, assertThrows(IOException.class,
                () -> new DeltaLakeAzureLogStore(conf).listFrom(startFrom(logDir, VERSION), conf)));
        assertEquals(1, TestAzureFileSystem.fullListingCalls);
    }

    @ParameterizedTest
    @ValueSource(strings = {"cause", "flag"})
    public void testInterruptedListingIsNotRetried(String interruption, @Injectable AzureBlobFileSystemStore store,
                                                  @Injectable AbfsConfiguration abfsConf) throws IOException {
        Configuration conf = new Configuration(false);
        Path logDir = new Path("abfss://container@account.dfs.core.windows.net/table/_delta_log");
        bindAzure(conf, store, abfsConf);
        // Both failures would fall back to the full listing on their own (see
        // testStartFromFailureFallsBackToFullListing); only the interruption must stop that. A sleep interrupted
        // between ABFS retries only shows up as the restored interrupt flag.
        boolean byFlag = interruption.equals("flag");
        IOException failure = byFlag
                ? new AbfsRestOperationException(400, "InvalidQueryParameterValue", "bad continuation", null)
                : new PathIOException(logDir.toString(), new InterruptedIOException("query cancelled"));
        new Expectations() {
            {
                store.listStatus(logDir, START_FILE, (TracingContext) any);
                result = failure;
            }
        };

        // The test filesystem rejects the full listing a fallback would issue.
        try {
            if (byFlag) {
                Thread.currentThread().interrupt();
            }
            assertSame(failure, assertThrows(IOException.class,
                    () -> new DeltaLakeAzureLogStore(conf).listFrom(startFrom(logDir, VERSION), conf)));
        } finally {
            assertEquals(byFlag, Thread.interrupted());
        }
    }

    @Test
    public void testListingThatSkipsExistingVersionFallsBackToFullListing(
            @Injectable AzureBlobFileSystemStore store, @Injectable AbfsConfiguration abfsConf) throws IOException {
        Configuration conf = new Configuration(false);
        Path logDir = new Path("abfss://container@account.dfs.core.windows.net/table/_delta_log");
        bindAzure(conf, store, abfsConf);
        String commit = VERSION_PREFIX + ".json";
        String laterCommit = "00000000000000712902.json";
        new Expectations() {
            {
                // Azure resolved the client-built continuation token past the requested commit.
                store.listStatus(logDir, START_FILE, (TracingContext) any);
                result = new org.apache.hadoop.fs.FileStatus[] {status(logDir, laterCommit)};
                times = 1;
            }
        };
        TestAzureFileSystem.existingNames = Set.of("_delta_log", commit);
        TestAzureFileSystem.fullListing = new org.apache.hadoop.fs.FileStatus[] {
                status(logDir, "00000000000000712883.json"), status(logDir, commit), status(logDir, laterCommit)};

        assertEquals(List.of(commit, laterCommit), names(new DeltaLakeAzureLogStore(conf)
                .listFrom(startFrom(logDir, VERSION), conf)));
    }

    @Test
    public void testListingFromCleanedUpVersionIsAccepted(@Injectable AzureBlobFileSystemStore store,
                                                         @Injectable AbfsConfiguration abfsConf) throws IOException {
        Configuration conf = new Configuration(false);
        Path logDir = new Path("abfss://container@account.dfs.core.windows.net/table/_delta_log");
        bindAzure(conf, store, abfsConf);
        String checkpoint = "00000000000000000010.checkpoint.parquet";
        new Expectations() {
            {
                // Log cleanup removed versions 0-9; Kernel still lists from 0 for time travel and cleanup.
                store.listStatus(logDir, "00000000000000000000.", (TracingContext) any);
                result = new org.apache.hadoop.fs.FileStatus[] {status(logDir, checkpoint)};
            }
        };
        TestAzureFileSystem.existingNames = Set.of("_delta_log", checkpoint);

        // The test filesystem rejects the full listing a fallback would issue.
        assertEquals(List.of(checkpoint), names(new DeltaLakeAzureLogStore(conf).listFrom(startFrom(logDir, 0), conf)));
    }

    @Test
    public void testMissingLogDirectoryIsNotRetried(@Injectable AzureBlobFileSystemStore store,
                                                   @Injectable AbfsConfiguration abfsConf) throws IOException {
        Configuration conf = new Configuration(false);
        Path logDir = new Path("abfss://container@account.dfs.core.windows.net/table/_delta_log");
        bindAzure(conf, store, abfsConf);
        AbfsRestOperationException failure = new AbfsRestOperationException(404,
                "PathNotFound", "listing failed", null);
        new Expectations() {
            {
                store.listStatus(logDir, START_FILE, (TracingContext) any);
                result = failure;
            }
        };

        // A full listing would only report the same missing directory; the test filesystem rejects it.
        FileNotFoundException thrown = assertThrows(FileNotFoundException.class, () -> new DeltaLakeAzureLogStore(conf)
                .listFrom(startFrom(logDir, VERSION), conf));
        assertSame(failure, thrown.getCause());
    }

    @ParameterizedTest
    @ValueSource(strings = {"400", "501", "505", "driver", "runtime"})
    public void testStartFromFailureFallsBackToFullListing(String failureKind, @Injectable AzureBlobFileSystemStore store,
                                                          @Injectable AbfsConfiguration abfsConf) throws IOException {
        Configuration conf = new Configuration(false);
        Path logDir = new Path("abfss://container@account.dfs.core.windows.net/table/_delta_log");
        bindAzure(conf, store, abfsConf);
        Exception failure = switch (failureKind) {
            case "driver" -> new InvalidUriException("unexpected path");
            case "runtime" -> new IllegalStateException("unexpected continuation token");
            default -> new AbfsRestOperationException(Integer.parseInt(failureKind), "test-error", "listing failed", null);
        };
        new Expectations() {
            {
                store.listStatus(logDir, START_FILE, (TracingContext) any);
                result = failure;
                times = 1;
            }
        };
        TestAzureFileSystem.fullListing = new org.apache.hadoop.fs.FileStatus[] {
                status(logDir, "00000000000000712883.json"), status(logDir, VERSION_PREFIX + ".json")};

        assertEquals(List.of(VERSION_PREFIX + ".json"), names(new DeltaLakeAzureLogStore(conf)
                .listFrom(startFrom(logDir, VERSION), conf)));
        assertEquals(Map.of("DeltaLake.listFrom.azureFallback", 1L), listingCounters());
    }

    @ParameterizedTest
    @ValueSource(strings = {"401", "403", "sas", "408", "410", "429", "500", "503", "network", "timeout"})
    public void testFailureTheFullListingWouldRepeatIsRethrown(String failureKind,
                                                              @Injectable AzureBlobFileSystemStore store,
                                                              @Injectable AbfsConfiguration abfsConf) throws IOException {
        Configuration conf = new Configuration(false);
        Path logDir = new Path("abfss://container@account.dfs.core.windows.net/table/_delta_log");
        bindAzure(conf, store, abfsConf);
        // Network failures reach the caller the way ABFS reports them after its retries: status -1.
        AzureBlobFileSystemException failure = switch (failureKind) {
            case "sas" -> new SASTokenProviderException("SAS token expired");
            case "network" -> new InvalidAbfsRestOperationException(new java.net.UnknownHostException("account"));
            case "timeout" -> new InvalidAbfsRestOperationException(new java.net.SocketTimeoutException("read timed out"));
            default -> new AbfsRestOperationException(Integer.parseInt(failureKind), "test-error", "listing failed", null);
        };
        new Expectations() {
            {
                store.listStatus(logDir, START_FILE, (TracingContext) any);
                result = failure;
            }
        };

        // The test filesystem rejects the full listing a fallback would issue.
        IOException thrown = assertThrows(IOException.class,
                () -> new DeltaLakeAzureLogStore(conf).listFrom(startFrom(logDir, VERSION), conf));
        if (failureKind.equals("401") || failureKind.equals("403")) {
            assertInstanceOf(AccessDeniedException.class, thrown);
            assertSame(failure, thrown.getCause());
        } else {
            assertSame(failure, thrown);
        }
        assertTrue(listingCounters().isEmpty());
        assertTrue(Tracers.printScopeTimer().contains("DeltaLake.listFrom"));
    }

    @Test
    public void testDisabledByConfig(@Injectable AzureBlobFileSystemStore store,
                                     @Injectable AbfsConfiguration abfsConf) throws IOException {
        Configuration conf = new Configuration(false);
        Path logDir = new Path("abfss://container@account.dfs.core.windows.net/table/_delta_log");
        Config.enable_delta_lake_azure_start_from_listing = false;
        try {
            // Registration does not depend on the config, so it can be switched either way at runtime.
            Configuration catalogConf = new Configuration(false);
            DeltaLakeAzureLogStore.registerAsDefault(catalogConf);
            assertEquals(DeltaLakeAzureLogStore.class.getName(), catalogConf.get("io.delta.kernel.logStore.abfss.impl"));

            // The registered LogStore uses the full listing while the config is off.
            TestAzureFileSystem.store = store;
            TestAzureFileSystem.expectedConfiguration = conf;
            TestAzureFileSystem.existingNames = null;
            TestAzureFileSystem.logDirExists = true;
            TestAzureFileSystem.fullListingFailure = null;
            TestAzureFileSystem.fullListing = new org.apache.hadoop.fs.FileStatus[] {status(logDir, VERSION_PREFIX + ".json")};
            conf.setClass("fs.abfss.impl", TestAzureFileSystem.class, FileSystem.class);
            conf.setBoolean("fs.abfss.impl.disable.cache", true);
            new Expectations() {
                {
                    store.listStatus((Path) any, anyString, (TracingContext) any);
                    times = 0;
                }
            };
            assertEquals(List.of(VERSION_PREFIX + ".json"), names(new DeltaLakeAzureLogStore(conf)
                    .listFrom(startFrom(logDir, VERSION), conf)));
        } finally {
            Config.enable_delta_lake_azure_start_from_listing = true;
        }
    }

    @Test
    public void testBlobServiceTypeUsesFullListing(@Injectable AzureBlobFileSystemStore store,
                                                  @Injectable AbfsConfiguration abfsConf) throws IOException {
        Configuration conf = new Configuration(false);
        Path logDir = new Path("abfss://container@account.blob.core.windows.net/table/_delta_log");
        bindAzure(conf, store, abfsConf, AbfsServiceType.BLOB);
        new Expectations() {
            {
                store.listStatus((Path) any, anyString, (TracingContext) any);
                times = 0;
            }
        };
        TestAzureFileSystem.fullListing = new org.apache.hadoop.fs.FileStatus[] {status(logDir, VERSION_PREFIX + ".json")};

        assertEquals(List.of(VERSION_PREFIX + ".json"), names(new DeltaLakeAzureLogStore(conf)
                .listFrom(startFrom(logDir, VERSION), conf)));
    }

    @Test
    public void testMetastoreRegistersForAzureSchemesOnly() {
        Configuration conf = new Configuration(false);
        new HMSBackedDeltaMetastore("delta0", null, conf, new DeltaLakeCatalogProperties(Map.of()));
        // Per-table configurations are copies of the catalog configuration.
        Configuration perTable = new Configuration(conf);
        for (String scheme : List.of("abfs", "abfss")) {
            String key = "io.delta.kernel.logStore." + scheme + ".impl";
            assertEquals(DeltaLakeAzureLogStore.class.getName(), conf.get(key));
            assertEquals(DeltaLakeAzureLogStore.class.getName(), perTable.get(key));
        }
        assertNull(conf.get("io.delta.kernel.logStore.wasbs.impl"));
        assertNull(conf.get("io.delta.kernel.logStore.s3a.impl"));
    }

    @Test
    public void testEngineDoesNotModifyConfiguration() {
        Configuration conf = new Configuration(false);
        DeltaLakeEngine.create(conf, new DeltaLakeCatalogProperties(Map.of()), null, null);
        assertEquals(0, conf.size());
    }

    @Test
    public void testCustomAzureLogStoreIsPreserved() throws IOException {
        Configuration conf = new Configuration(false);
        conf.set("io.delta.kernel.logStore.abfss.impl", RecordingLogStore.class.getName());
        Path requestedPath = startFrom(new Path("abfss://container@account.dfs.core.windows.net/table/_delta_log"), VERSION);
        RecordingLogStore.requested = null;

        DeltaLakeEngine engine = newEngine(conf);
        assertEquals(RecordingLogStore.class.getName(), conf.get("io.delta.kernel.logStore.abfss.impl"));
        try (CloseableIterator<FileStatus> files = engine.getFileSystemClient().listFrom(requestedPath.toString())) {
            assertTrue(files.toInMemoryList().isEmpty());
        }
        assertEquals(requestedPath, RecordingLogStore.requested);
    }

    @Test
    public void testLocalListingAndSnapshot(@TempDir java.nio.file.Path tableDir) throws IOException {
        java.nio.file.Path logDir = Files.createDirectory(tableDir.resolve("_delta_log"));
        // A complete minimal Delta table exercises Kernel through the engine, including the
        // missing _last_checkpoint path and the unchanged non-Azure listing implementation.
        Files.writeString(logDir.resolve("00000000000000000000.json"), """
                {"protocol":{"minReaderVersion":1,"minWriterVersion":2}}
                {"metaData":{"id":"test-table","format":{"provider":"parquet","options":{}},\
                "schemaString":"{\\"type\\":\\"struct\\",\\"fields\\":[]}",\
                "partitionColumns":[],"configuration":{},"createdTime":0}}
                """);
        Files.writeString(logDir.resolve("00000000000000000001.json"), "{\"commitInfo\":{}}\n");
        DeltaLakeEngine engine = newEngine(new Configuration());
        try (CloseableIterator<FileStatus> files = engine.getFileSystemClient()
                .listFrom(startFrom(new Path(logDir.toUri()), 1).toString())) {
            assertEquals(List.of("00000000000000000001.json"), files.toInMemoryList().stream()
                    .map(file -> new Path(file.getPath()).getName()).toList());
        }
        assertEquals(1, Table.forPath(engine, tableDir.toUri().toString()).getLatestSnapshot(engine).getVersion());
    }

    private static DeltaLakeEngine newEngine(Configuration conf) {
        DeltaLakeAzureLogStore.registerAsDefault(conf);
        DeltaLakeCatalogProperties properties = new DeltaLakeCatalogProperties(Map.of(
                DeltaLakeCatalogProperties.ENABLE_DELTA_LAKE_JSON_META_CACHE, "false",
                DeltaLakeCatalogProperties.ENABLE_DELTA_LAKE_CHECKPOINT_META_CACHE, "false"));
        return DeltaLakeEngine.create(conf, properties, null, null);
    }

    private static Map<String, Object> listingCounters() {
        return Tracers.getAllVars().stream().collect(java.util.stream.Collectors.toMap(
                variable -> variable.getName(), variable -> variable.getValue()));
    }

    private static org.apache.hadoop.fs.FileStatus status(Path dir, String filename) {
        return new org.apache.hadoop.fs.FileStatus(123, false, 1, 4096, 456, new Path(dir, filename));
    }

    /** The listing start Kernel requests for {@code version}, e.g. {@code _delta_log/00000000000000712884.}. */
    private static Path startFrom(Path logDir, long version) {
        return new Path(FileNames.listingPrefix(new io.delta.kernel.internal.fs.Path(logDir.toString()), version));
    }

    private static List<String> names(Iterator<org.apache.hadoop.fs.FileStatus> files) {
        List<String> names = new java.util.ArrayList<>();
        files.forEachRemaining(file -> names.add(file.getPath().getName()));
        return names;
    }

    private static void bindAzure(Configuration conf, AzureBlobFileSystemStore store, AbfsConfiguration abfsConf) {
        bindAzure(conf, store, abfsConf, AbfsServiceType.DFS);
    }

    private static void bindAzure(Configuration conf, AzureBlobFileSystemStore store, AbfsConfiguration abfsConf,
                                  AbfsServiceType serviceType) {
        TestAzureFileSystem.store = store;
        TestAzureFileSystem.fullListing = null;
        TestAzureFileSystem.logDirExists = true;
        TestAzureFileSystem.existingNames = null;
        TestAzureFileSystem.fullListingFailure = null;
        TestAzureFileSystem.fullListingCalls = 0;
        TestAzureFileSystem.expectedConfiguration = conf;
        for (String scheme : List.of("abfs", "abfss")) {
            conf.setClass("fs." + scheme + ".impl", TestAzureFileSystem.class, FileSystem.class);
            conf.setBoolean("fs." + scheme + ".impl.disable.cache", true);
        }
        new Expectations() {
            {
                store.getAbfsConfiguration();
                result = abfsConf;
                abfsConf.getFsConfiguredServiceType();
                result = serviceType;
            }
        };
        if (serviceType == AbfsServiceType.BLOB) {
            return;
        }
        new Expectations() {
            {
                abfsConf.getClientCorrelationId();
                result = "delta-test";
                abfsConf.getTracingHeaderFormat();
                result = TracingHeaderFormat.ALL_ID_FORMAT;
            }
        };
    }

    /** User-configured LogStore standing in for e.g. a custom commit coordinator integration. */
    public static class RecordingLogStore extends AzureLogStore {
        private static Path requested;

        public RecordingLogStore(Configuration conf) {
            super(conf);
        }

        @Override
        public Iterator<org.apache.hadoop.fs.FileStatus> listFrom(Path path, Configuration conf) {
            requested = path;
            return Collections.emptyIterator();
        }
    }

    /**
     * Real filesystem dispatch with a mocked Azure store. A full directory listing fails the test
     * unless the test opts into the legacy listing by setting {@code fullListing}.
     */
    public static class TestAzureFileSystem extends AzureBlobFileSystem {
        private static AzureBlobFileSystemStore store;
        private static org.apache.hadoop.fs.FileStatus[] fullListing;
        private static boolean logDirExists;
        private static IOException fullListingFailure;
        private static int fullListingCalls;
        // When set, exactly these names exist; otherwise only the log directory, and only for a full listing.
        private static Set<String> existingNames;
        private static Configuration expectedConfiguration;
        private URI uri;

        @Override
        public void initialize(URI name, Configuration conf) {
            assertSame(expectedConfiguration, conf, "listing must retain the table's credential configuration");
            setConf(conf);
            uri = name;
        }

        @Override
        public URI getUri() {
            return uri;
        }

        @Override
        public Path getWorkingDirectory() {
            return new Path("/");
        }

        @Override
        public AzureBlobFileSystemStore getAbfsStore() {
            return store;
        }

        @Override
        public org.apache.hadoop.fs.FileStatus[] listStatus(Path path) throws IOException {
            if (fullListing == null) {
                throw new AssertionError("Must pass the start filename to the Azure store instead of listing all files");
            }
            fullListingCalls++;
            if (fullListingFailure != null) {
                throw fullListingFailure;
            }
            return fullListing;
        }

        @Override
        public boolean exists(Path path) {
            if (existingNames != null) {
                return existingNames.contains(path.getName());
            }
            if (fullListing == null) {
                throw new AssertionError("A separate directory existence request is unnecessary");
            }
            return logDirExists;
        }

        @Override
        public void close() {
            // This test filesystem owns no resources.
        }
    }
}
