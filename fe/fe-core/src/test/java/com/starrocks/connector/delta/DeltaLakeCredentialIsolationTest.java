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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.connector.MetastoreType;
import com.starrocks.connector.hadoop.HadoopExt;
import com.starrocks.connector.hive.HiveMetaClient;
import com.starrocks.connector.hive.HiveMetastore;
import com.starrocks.connector.hive.HiveMetastoreTest;
import com.starrocks.connector.hive.IHiveMetastore;
import com.starrocks.connector.metastore.MetastoreTable;
import com.starrocks.connector.share.credential.CloudConfigurationConstants;
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.credential.CloudConfigurationFactory;
import io.delta.kernel.Operation;
import io.delta.kernel.Snapshot;
import io.delta.kernel.TransactionBuilder;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.exceptions.CheckpointAlreadyExistsException;
import io.delta.kernel.exceptions.TableNotFoundException;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.internal.TableImpl;
import io.delta.kernel.internal.replay.LogReplay;
import io.delta.kernel.internal.util.Utils;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.FileStatus;
import mockit.Mock;
import mockit.MockUp;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.fs.s3a.Constants;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

/**
 * Unity vends a credential scoped to one table's storage path, so a credential loaded for one table
 * must never end up being used to read another table's delta log.
 */
public class DeltaLakeCredentialIsolationTest {
    private static final String CHECKPOINT =
            "s3://bucket/nation/_delta_log/00000000000000000000.checkpoint.parquet";

    private DeltaLakeMetastore metastore;
    private String tablePathPrefix;
    private Function<String, CloudConfiguration> credentialForTable;

    @BeforeEach
    public void setUp() {
        tablePathPrefix = "s3://bucket/";
        credentialForTable = DeltaLakeCredentialIsolationTest::vendedCredentialFor;
        HiveMetaClient client = new HiveMetastoreTest.MockedHiveMetaClient();
        IHiveMetastore hiveMetastore = new HiveMetastore(client, "delta0", MetastoreType.HMS);
        DeltaLakeCatalogProperties properties = new DeltaLakeCatalogProperties(Maps.newHashMap());
        metastore = new HMSBackedDeltaMetastore("delta0", hiveMetastore, new Configuration(), properties);

        new MockUp<HMSBackedDeltaMetastore>() {
            @Mock
            public MetastoreTable getMetastoreTable(String dbName, String tableName) {
                return new MetastoreTable(dbName, tableName, tablePathPrefix + tableName, 123,
                        credentialForTable.apply(tableName));
            }
        };
        new MockUp<TableImpl>() {
            @Mock
            public io.delta.kernel.Table forPath(Engine engine, String path) {
                return new StubDeltaTable();
            }
        };
    }

    @Test
    public void testLoadingAnotherTableDoesNotOverwriteAnEarlierEngineCredential() throws Exception {
        DeltaLakeSnapshot nation = metastore.getLatestSnapshot("db1", "nation");
        metastore.getLatestSnapshot("db1", "supplier");

        Assertions.assertEquals("ak-nation", accessKeyOf(nation.getDeltaLakeEngine()));
    }

    @Test
    public void testCheckpointCacheLoaderUsesTheRequestingTableCredential() throws Exception {
        String[] observedAccessKey = new String[1];
        new MockUp<DeltaLakeParquetHandler>() {
            @Mock
            public List<ColumnarBatch> readParquetFile(String filePath, StructType physicalSchema,
                                                       Configuration hadoopConf) {
                observedAccessKey[0] = hadoopConf.get(Constants.ACCESS_KEY);
                return Lists.newArrayList();
            }
        };

        DeltaLakeSnapshot nation = metastore.getLatestSnapshot("db1", "nation");
        metastore.getLatestSnapshot("db1", "supplier");
        readCheckpointThrough(nation.getDeltaLakeEngine());

        Assertions.assertEquals("ak-nation", observedAccessKey[0]);
    }

    @Test
    public void testEnginesOfDifferentTablesGetDistinctFileSystemCacheKeys() throws Exception {
        DeltaLakeSnapshot nation = metastore.getLatestSnapshot("db1", "nation");
        DeltaLakeSnapshot supplier = metastore.getLatestSnapshot("db1", "supplier");

        // FileSystem instances are cached per credential fingerprint, so two tables sharing a scheme and
        // authority only get separate clients when their fingerprints differ.
        Assertions.assertNotEquals(credentialFingerprintOf(nation.getDeltaLakeEngine()),
                credentialFingerprintOf(supplier.getDeltaLakeEngine()));
        Assertions.assertTrue(HadoopExt.getInstance().getCredentialFingerprint(
                configurationOf(nation.getDeltaLakeEngine())).isEmpty());
    }

    @Test
    public void testAzureSnapshotClientsAreIsolatedFromCatalogClients() throws Exception {
        useAzureCredentials();
        Configuration nation = configurationOf(metastore.getLatestSnapshot("db1", "nation").getDeltaLakeEngine());
        Configuration sameNation = configurationOf(metastore.getLatestSnapshot("db1", "nation").getDeltaLakeEngine());
        Configuration supplier = configurationOf(metastore.getLatestSnapshot("db1", "supplier").getDeltaLakeEngine());
        Assertions.assertFalse(HadoopExt.getInstance().getCredentialFingerprint(nation).isEmpty());
        Assertions.assertTrue(HadoopExt.getInstance().getCredentialFingerprint(metastore.hdfsConfiguration).isEmpty());
        Assertions.assertNotEquals(HadoopExt.getInstance().getCredentialFingerprint(nation),
                HadoopExt.getInstance().getCredentialFingerprint(supplier));

        Configuration catalogConf = new Configuration(metastore.hdfsConfiguration);
        credentialForTable.apply("nation").applyToConfiguration(catalogConf);
        Assertions.assertEquals(HadoopExt.getInstance().getCloudConfString(catalogConf),
                HadoopExt.getInstance().getCloudConfString(nation));
        Assertions.assertTrue(HadoopExt.getInstance().getCredentialFingerprint(catalogConf).isEmpty());
        Assertions.assertEquals(HadoopExt.getInstance().getCredentialFingerprint(nation),
                HadoopExt.getInstance().getCredentialFingerprint(sameNation));
    }

    @Test
    public void testAzureSnapshotRespectsDisabledFileSystemCache() throws Exception {
        useAzureCredentials();
        metastore.hdfsConfiguration.setBoolean("fs.abfss.impl.disable.cache", true);
        Configuration snapshot = configurationOf(metastore.getLatestSnapshot("db1", "nation").getDeltaLakeEngine());
        Assertions.assertTrue(HadoopExt.getInstance().getCredentialFingerprint(snapshot).isEmpty());
        URI uri = URI.create(tablePathPrefix);
        try (FileSystem first = FileSystem.get(uri, snapshot); FileSystem second = FileSystem.get(uri, snapshot)) {
            Assertions.assertNotSame(first, second);
        }
    }

    @Test
    public void testAzureSnapshotUsesDefaultFileSystemAndCatalogCredentials() throws Exception {
        useAzureCredentials();
        credentialForTable.apply("catalog").applyToConfiguration(metastore.hdfsConfiguration);
        credentialForTable = table -> null;
        metastore.hdfsConfiguration.set("fs.defaultFS", tablePathPrefix);
        tablePathPrefix = "/warehouse/";
        Configuration snapshot = configurationOf(metastore.getLatestSnapshot("db1", "nation").getDeltaLakeEngine());
        Assertions.assertFalse(HadoopExt.getInstance().getCredentialFingerprint(snapshot).isEmpty());
        Assertions.assertTrue(HadoopExt.getInstance().getCredentialFingerprint(metastore.hdfsConfiguration).isEmpty());
    }

    private void useAzureCredentials() {
        tablePathPrefix = "abfss://snapshot@account.dfs.core.windows.net/";
        metastore.hdfsConfiguration.setClass("fs.abfss.impl", RawLocalFileSystem.class, FileSystem.class);
        credentialForTable = table -> CloudConfigurationFactory.buildCloudConfigurationForStorage(ImmutableMap.of(
                CloudConfigurationConstants.AZURE_ADLS2_STORAGE_ACCOUNT, "account",
                CloudConfigurationConstants.AZURE_ADLS2_SAS_TOKEN, "sas-" + table));
    }

    private static CloudConfiguration vendedCredentialFor(String tableName) {
        return CloudConfigurationFactory.buildCloudConfigurationForStorage(ImmutableMap.of(
                CloudConfigurationConstants.AWS_S3_ACCESS_KEY, "ak-" + tableName,
                CloudConfigurationConstants.AWS_S3_SECRET_KEY, "sk-" + tableName,
                CloudConfigurationConstants.AWS_S3_SESSION_TOKEN, "token-" + tableName,
                CloudConfigurationConstants.AWS_S3_REGION, "us-west-2"));
    }

    private static String accessKeyOf(DeltaLakeEngine engine) throws Exception {
        return configurationOf(engine).get(Constants.ACCESS_KEY);
    }

    private static String credentialFingerprintOf(DeltaLakeEngine engine) throws Exception {
        return configurationOf(engine).get(HadoopExt.HADOOP_CLOUD_CONFIGURATION_STRING);
    }

    private static Configuration configurationOf(DeltaLakeEngine engine) throws Exception {
        Field field = DeltaLakeEngine.class.getDeclaredField("hadoopConf");
        field.setAccessible(true);
        return (Configuration) field.get(engine);
    }

    private static void readCheckpointThrough(DeltaLakeEngine engine) throws IOException {
        engine.getParquetHandler()
                .readParquetFiles(Utils.singletonCloseableIterator(FileStatus.of(CHECKPOINT, 100, 123)),
                        LogReplay.getAddRemoveReadSchema(true), Optional.empty())
                .forEachRemaining(batch -> { });
    }

    private static class StubDeltaTable implements io.delta.kernel.Table {
        @Override
        public String getPath(Engine engine) {
            return null;
        }

        @Override
        public SnapshotImpl getLatestSnapshot(Engine engine) {
            return null;
        }

        @Override
        public Snapshot getSnapshotAsOfVersion(Engine engine, long versionId) throws TableNotFoundException {
            return null;
        }

        @Override
        public Snapshot getSnapshotAsOfTimestamp(Engine engine, long millisSinceEpochUTC) throws TableNotFoundException {
            return null;
        }

        @Override
        public TransactionBuilder createTransactionBuilder(Engine engine, String engineInfo, Operation operation) {
            return null;
        }

        @Override
        public void checkpoint(Engine engine, long version)
                throws TableNotFoundException, CheckpointAlreadyExistsException, IOException {
        }
    }
}
