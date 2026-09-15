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

package com.starrocks.connector.iceberg.procedure;

import com.starrocks.connector.HdfsEnvironment;
import com.starrocks.connector.iceberg.IcebergUtil;
import com.starrocks.connector.iceberg.hive.IcebergHiveCatalog;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.AlterTableOperationClause;
import com.starrocks.sql.ast.AlterTableStmt;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

/**
 * The two procedures that reach storage through a Hadoop {@link FileSystem} - remove_orphan_files and
 * add_files - used to build it from the catalog-level configuration only. On an Iceberg REST catalog
 * that vends credentials per table there are no static credentials in the catalog properties at all,
 * so those two failed with {@code NoAuthWithAWSException} while every other operation on the same
 * catalog worked.
 * <p>
 * These tests capture the {@link Configuration} the procedure actually hands to
 * {@code FileSystem.get}, which is observable both before and after the fix:
 * <ul>
 *   <li>trigger set: a catalog that only vends credentials, and one that vends them while also
 *       carrying static ones (the vended credentials must win, as on the scan/sink paths);</li>
 *   <li>control set: a catalog with static credentials only (they must survive), and a catalog with
 *       no credentials at all (none must be invented).</li>
 * </ul>
 */
public class IcebergProcedureVendedCredentialsTest {

    private static final String TABLE_LOCATION = "s3://bucket/db/tbl";
    private static final String INCOMING_LOCATION = "s3://bucket/staging/incoming";

    private static final String VENDED_AK = "vended-access-key-id";
    private static final String VENDED_SK = "vended-secret-access-key";
    private static final String VENDED_TOKEN = "vended-session-token";

    private static final String STATIC_AK = "static-access-key";
    private static final String STATIC_SK = "static-secret-key";

    private static final String GCS_ACCESS_TOKEN = "gcs.oauth2.token";
    private static final String GCS_IMPERSONATION_KEY = "fs.gs.auth.impersonation.service.account";

    private static final String TEMPORARY_CREDENTIAL_PROVIDER =
            "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider";

    // Hadoop s3a keys, spelled out the way the other credential tests in this repo do.
    private static final String S3A_ACCESS_KEY = "fs.s3a.access.key";
    private static final String S3A_SECRET_KEY = "fs.s3a.secret.key";
    private static final String S3A_SESSION_TOKEN = "fs.s3a.session.token";
    private static final String S3A_CREDENTIALS_PROVIDER = "fs.s3a.aws.credentials.provider";

    // ---------------------------------------------------------------- remove_orphan_files

    @Test
    void testRemoveOrphanFilesUsesVendedCredentialsWhenCatalogHasNoStaticOnes() {
        Configuration conf = runRemoveOrphanFiles(vendedProperties(), new HdfsEnvironment());

        assertEquals(VENDED_AK, conf.get(S3A_ACCESS_KEY));
        assertEquals(VENDED_SK, conf.get(S3A_SECRET_KEY));
        assertEquals(VENDED_TOKEN, conf.get(S3A_SESSION_TOKEN));
        assertEquals(TEMPORARY_CREDENTIAL_PROVIDER, conf.get(S3A_CREDENTIALS_PROVIDER));
    }

    @Test
    void testRemoveOrphanFilesPrefersVendedCredentialsOverStaticOnes() {
        Configuration conf = runRemoveOrphanFiles(vendedProperties(), staticCredentialEnvironment());

        assertEquals(VENDED_AK, conf.get(S3A_ACCESS_KEY));
        assertEquals(VENDED_TOKEN, conf.get(S3A_SESSION_TOKEN));
    }

    /** Control: nothing is vended, so the credentials the user configured must still be there. */
    @Test
    void testRemoveOrphanFilesKeepsStaticCredentialsWhenNothingIsVended() {
        Configuration conf = runRemoveOrphanFiles(Collections.emptyMap(), staticCredentialEnvironment());

        assertEquals(STATIC_AK, conf.get(S3A_ACCESS_KEY));
        assertEquals(STATIC_SK, conf.get(S3A_SECRET_KEY));
    }

    /** Control: no credentials anywhere means none are invented. */
    @Test
    void testRemoveOrphanFilesInventsNoCredentials() {
        Configuration conf = runRemoveOrphanFiles(Collections.emptyMap(), new HdfsEnvironment());

        assertNull(conf.get(S3A_ACCESS_KEY));
        assertNull(conf.get(S3A_SESSION_TOKEN));
    }

    /** Incomplete vended credentials (no session token) are not usable and must not be applied. */
    @Test
    void testRemoveOrphanFilesIgnoresIncompleteVendedCredentials() {
        Map<String, String> partial = new HashMap<>();
        partial.put(S3FileIOProperties.ACCESS_KEY_ID, VENDED_AK);
        partial.put(S3FileIOProperties.SECRET_ACCESS_KEY, VENDED_SK);

        Configuration conf = runRemoveOrphanFiles(partial, staticCredentialEnvironment());

        assertEquals(STATIC_AK, conf.get(S3A_ACCESS_KEY));
        assertNull(conf.get(S3A_SESSION_TOKEN));
    }

    // ------------------------------------------------------------------------- add_files

    @Test
    void testAddFilesUsesVendedCredentialsWhenCatalogHasNoStaticOnes() {
        Configuration conf = runAddFiles(vendedProperties(), new HdfsEnvironment());

        assertEquals(VENDED_AK, conf.get(S3A_ACCESS_KEY));
        assertEquals(VENDED_SK, conf.get(S3A_SECRET_KEY));
        assertEquals(VENDED_TOKEN, conf.get(S3A_SESSION_TOKEN));
        assertEquals(TEMPORARY_CREDENTIAL_PROVIDER, conf.get(S3A_CREDENTIALS_PROVIDER));
    }

    /** Control: nothing is vended, so the credentials the user configured must still be there. */
    @Test
    void testAddFilesKeepsStaticCredentialsWhenNothingIsVended() {
        Configuration conf = runAddFiles(Collections.emptyMap(), staticCredentialEnvironment());

        assertEquals(STATIC_AK, conf.get(S3A_ACCESS_KEY));
        assertEquals(STATIC_SK, conf.get(S3A_SECRET_KEY));
    }

    // ------------------------------ overlaying is not neutralizing ------------------------------
    //
    // Both cases below are the same mistake in two places: applying the vended credentials sets the
    // keys it needs, but leaves the base configuration's *conflicting* state in place. Overlaying is
    // not neutralizing.

    /**
     * A catalog-level GCS impersonation account must not survive onto a vended token.
     * {@code GCPCloudCredential} only omits *adding* impersonation when a token is present, so a value
     * copied from the base configuration stays, and gcs-connector then impersonates on top of a token
     * that typically lacks that IAM permission.
     */
    @Test
    void testGcsImpersonationIsClearedWhenTokenIsVended() {
        Map<String, String> baseWithImpersonation = new HashMap<>();
        baseWithImpersonation.put("gcp.gcs.use_compute_engine_service_account", "false");
        baseWithImpersonation.put("gcp.gcs.impersonation_service_account", "svc@project.iam.gserviceaccount.com");

        Map<String, String> vendedGcsToken = new HashMap<>();
        vendedGcsToken.put(GCS_ACCESS_TOKEN, "ya29.vended-token");

        Configuration conf = IcebergUtil.buildStorageConfiguration(
                tableWithVendedProperties(vendedGcsToken, "gs://bucket/db/tbl"),
                catalogWithNoProperties(), new HdfsEnvironment(baseWithImpersonation));

        assertNull(conf.get(GCS_IMPERSONATION_KEY),
                "a vended GCS token must not be used together with catalog impersonation");
    }

    /**
     * A file system built from vended credentials must not be served out of Hadoop's process-wide
     * cache, whose key is (scheme, authority, ugi) and excludes the Configuration - one instance would
     * otherwise serve every catalog on the account with whichever credential created it first, and
     * keep serving a token after it expired.
     */
    @Test
    void testSharedFileSystemCacheIsDisabledForVendedCredentials() {
        Map<String, String> vendedGcsToken = new HashMap<>();
        vendedGcsToken.put(GCS_ACCESS_TOKEN, "ya29.vended-token");

        Configuration conf = IcebergUtil.buildStorageConfiguration(
                tableWithVendedProperties(vendedGcsToken, "gs://bucket/db/tbl"),
                catalogWithNoProperties(), new HdfsEnvironment());

        assertTrue(conf.getBoolean("fs.gs.impl.disable.cache", false),
                "gs file systems carrying a vended token must be isolated from the shared cache");
    }

    /** Control: nothing vended, so nothing is isolated and nothing is unset. */
    @Test
    void testSharedFileSystemCacheIsKeptWhenNothingIsVended() {
        Configuration conf = IcebergUtil.buildStorageConfiguration(
                tableWithVendedProperties(Collections.emptyMap(), "gs://bucket/db/tbl"),
                catalogWithNoProperties(), staticCredentialEnvironment());

        assertFalse(conf.getBoolean("fs.gs.impl.disable.cache", false),
                "without vended credentials the shared cache must keep working");
    }

    // ------------------------------------------------------------------------- harness

    private static Map<String, String> vendedProperties() {
        Map<String, String> properties = new HashMap<>();
        properties.put(S3FileIOProperties.ACCESS_KEY_ID, VENDED_AK);
        properties.put(S3FileIOProperties.SECRET_ACCESS_KEY, VENDED_SK);
        properties.put(S3FileIOProperties.SESSION_TOKEN, VENDED_TOKEN);
        properties.put(S3FileIOProperties.ENDPOINT, "http://minio:9000");
        return properties;
    }

    private static HdfsEnvironment staticCredentialEnvironment() {
        Map<String, String> properties = new HashMap<>();
        properties.put("aws.s3.access_key", STATIC_AK);
        properties.put("aws.s3.secret_key", STATIC_SK);
        properties.put("aws.s3.endpoint", "http://minio:9000");
        properties.put("aws.s3.region", "us-east-1");
        return new HdfsEnvironment(properties);
    }

    private static Table tableWithVendedProperties(Map<String, String> ioProperties, String location) {
        Table table = tableWithVendedProperties(ioProperties);
        when(table.location()).thenReturn(location);
        return table;
    }

    private static IcebergHiveCatalog catalogWithNoProperties() {
        return mock(IcebergHiveCatalog.class);
    }

    /** A table whose FileIO reports {@code ioProperties} - i.e. what the catalog vended for it. */
    private static Table tableWithVendedProperties(Map<String, String> ioProperties) {
        FileIO fileIO = new StubFileIO(ioProperties);

        Snapshot snapshot = mock(Snapshot.class);
        when(snapshot.manifestListLocation()).thenReturn(null);
        when(snapshot.allManifests(any(FileIO.class))).thenReturn(Collections.emptyList());

        Table table = mock(Table.class);
        when(table.location()).thenReturn(TABLE_LOCATION);
        when(table.io()).thenReturn(fileIO);
        when(table.currentSnapshot()).thenReturn(snapshot);
        when(table.snapshots()).thenReturn(Collections.singletonList(snapshot));
        when(table.spec()).thenReturn(PartitionSpec.unpartitioned());
        return table;
    }

    private static IcebergTableProcedureContext createContext(Table table, HdfsEnvironment hdfsEnvironment) {
        // Not stubbing getCatalogProperties(): it is a default method on IcebergCatalog, and the mock
        // already answers it with an empty map. buildStorageConfiguration tolerates a null too.
        IcebergHiveCatalog catalog = mock(IcebergHiveCatalog.class);
        return new IcebergTableProcedureContext(catalog, table, mock(ConnectContext.class), null,
                hdfsEnvironment, mock(AlterTableStmt.class), mock(AlterTableOperationClause.class));
    }

    private static Configuration runRemoveOrphanFiles(Map<String, String> ioProperties,
                                                      HdfsEnvironment hdfsEnvironment) {
        Table table = tableWithVendedProperties(ioProperties);
        AtomicReference<Configuration> captured = new AtomicReference<>();

        try (MockedStatic<org.apache.iceberg.ReachableFileUtil> reachableUtil =
                     mockStatic(org.apache.iceberg.ReachableFileUtil.class);
                MockedStatic<FileSystem> fsStatic = mockStatic(FileSystem.class)) {

            reachableUtil.when(() -> org.apache.iceberg.ReachableFileUtil
                    .metadataFileLocations(any(Table.class), eq(false))).thenReturn(Collections.emptySet());
            reachableUtil.when(() -> org.apache.iceberg.ReachableFileUtil
                    .statisticsFilesLocations(any(Table.class))).thenReturn(Collections.emptyList());

            FileSystem fileSystem = mock(FileSystem.class);
            when(fileSystem.listFiles(any(Path.class), eq(true))).thenReturn(emptyLocatedFiles());
            stubFileSystemGet(fsStatic, fileSystem, captured);

            IcebergTableProcedureContext context = createContext(table, hdfsEnvironment);
            assertDoesNotThrow(() ->
                    RemoveOrphanFilesProcedure.getInstance().execute(context, Collections.emptyMap()));
        } catch (Exception e) {
            throw new AssertionError("unexpected failure while driving remove_orphan_files", e);
        }

        Configuration conf = captured.get();
        assertNotNull(conf, "remove_orphan_files never asked for a FileSystem");
        return conf;
    }

    private static Configuration runAddFiles(Map<String, String> ioProperties, HdfsEnvironment hdfsEnvironment) {
        Table table = tableWithVendedProperties(ioProperties);
        AtomicReference<Configuration> captured = new AtomicReference<>();

        try (MockedStatic<FileSystem> fsStatic = mockStatic(FileSystem.class)) {
            FileSystem fileSystem = mock(FileSystem.class);
            // An empty staging directory: the procedure resolves the FileSystem, finds nothing to add
            // and returns without touching the table.
            when(fileSystem.exists(any(Path.class))).thenReturn(true);
            when(fileSystem.getFileStatus(any(Path.class)))
                    .thenReturn(new FileStatus(0L, true, 0, 0L, 0L, new Path(INCOMING_LOCATION)));
            when(fileSystem.listStatus(any(Path.class))).thenReturn(new FileStatus[0]);
            stubFileSystemGet(fsStatic, fileSystem, captured);

            Map<String, ConstantOperator> args = new HashMap<>();
            args.put(AddFilesProcedure.LOCATION, ConstantOperator.createVarchar(INCOMING_LOCATION));
            args.put(AddFilesProcedure.FILE_FORMAT, ConstantOperator.createVarchar("parquet"));

            IcebergTableProcedureContext context = createContext(table, hdfsEnvironment);
            assertDoesNotThrow(() -> AddFilesProcedure.getInstance().execute(context, args));
        } catch (Exception e) {
            throw new AssertionError("unexpected failure while driving add_files", e);
        }

        Configuration conf = captured.get();
        assertNotNull(conf, "add_files never asked for a FileSystem");
        return conf;
    }

    private static void stubFileSystemGet(MockedStatic<FileSystem> fsStatic, FileSystem fileSystem,
                                          AtomicReference<Configuration> captured) {
        fsStatic.when(() -> FileSystem.get(any(), any())).thenAnswer(invocation -> {
            captured.compareAndSet(null, invocation.getArgument(1, Configuration.class));
            return fileSystem;
        });
    }

    private static RemoteIterator<LocatedFileStatus> emptyLocatedFiles() {
        return new RemoteIterator<>() {
            @Override
            public boolean hasNext() {
                return false;
            }

            @Override
            public LocatedFileStatus next() {
                throw new NoSuchElementException();
            }
        };
    }

    /**
     * A real FileIO rather than a mock: {@code properties()} is a default method on the interface, and
     * stubbing a default method with the inline mock maker needs to instrument {@code Closeable} /
     * {@code Serializable}, which fails with "Could not modify all classes". Nothing here touches
     * storage - only {@code properties()} is ever called on this path.
     */
    private static final class StubFileIO implements FileIO {
        private final Map<String, String> properties;

        private StubFileIO(Map<String, String> properties) {
            this.properties = properties;
        }

        @Override
        public Map<String, String> properties() {
            return properties;
        }

        @Override
        public InputFile newInputFile(String path) {
            throw new UnsupportedOperationException(path);
        }

        @Override
        public OutputFile newOutputFile(String path) {
            throw new UnsupportedOperationException(path);
        }

        @Override
        public void deleteFile(String path) {
            throw new UnsupportedOperationException(path);
        }
    }
}
