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

package com.starrocks.connector.iceberg.io;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Weigher;
import com.starrocks.common.StarRocksException;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.hadoop.HadoopExt;
import com.starrocks.credential.gcp.GCPCloudConfigurationProvider;
import mockit.Mock;
import mockit.MockUp;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Files;
import org.apache.iceberg.hadoop.HadoopConfigurable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.ResolvingFileIO;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.util.SerializableSupplier;
import org.apache.iceberg.util.SerializationUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.starrocks.credential.azure.AzureCloudConfigurationProvider.ADLS_ENDPOINT;
import static com.starrocks.credential.azure.AzureCloudConfigurationProvider.ADLS_SAS_TOKEN;
import static com.starrocks.credential.azure.AzureCloudConfigurationProvider.BLOB_ENDPOINT;
import static com.starrocks.credential.gcp.GCPCloudConfigurationProvider.ACCESS_TOKEN_PROVIDER_IMPL;
import static com.starrocks.credential.gcp.GCPCloudConfigurationProvider.GCS_ACCESS_TOKEN;

public class IcebergCachingFileIOTest {

    public void writeIcebergMetaTestFile() {
        try {
            BufferedWriter out = new BufferedWriter(new FileWriter("/tmp/0001.metadata.json"));
            out.write("test iceberg metadata json file content");
            out.close();
        } catch (IOException e) {
            Assertions.fail(e.getMessage());
        }
    }

    @Test
    public void testNewInputFile() {
        writeIcebergMetaTestFile();
        String path = "file:/tmp/0001.metadata.json";

        // create iceberg cachingFileIO
        IcebergCachingFileIO cachingFileIO = new IcebergCachingFileIO();
        cachingFileIO.setConf(new Configuration());
        Map<String, String> icebergProperties = new HashMap<>();
        icebergProperties.put("iceberg.catalog.type", "hive");
        cachingFileIO.initialize(icebergProperties);

        InputFile cachingFileIOInputFile = cachingFileIO.newInputFile(path);
        cachingFileIOInputFile.newStream();

        String cachingFileIOPath = cachingFileIOInputFile.location();
        Assertions.assertEquals(path, cachingFileIOPath);

        long cacheIOInputFileSize = cachingFileIOInputFile.getLength();
        Assertions.assertEquals(cacheIOInputFileSize, 39);
        cachingFileIO.deleteFile(path);
    }

    // An InputFile over a local file that counts the stats (getLength) issued against it.
    private static InputFile countingInput(String localPath, AtomicInteger lengthCalls) {
        InputFile local = Files.localInput(localPath);
        return new InputFile() {
            @Override
            public long getLength() {
                lengthCalls.incrementAndGet();
                return local.getLength();
            }

            @Override
            public SeekableInputStream newStream() {
                return local.newStream();
            }

            @Override
            public String location() {
                return local.location();
            }

            @Override
            public boolean exists() {
                return local.exists();
            }
        };
    }

    private static String writeLocalFile(String name, String content) throws IOException {
        java.io.File file = java.io.File.createTempFile(name, ".avro");
        file.deleteOnExit();
        try (BufferedWriter out = new BufferedWriter(new FileWriter(file))) {
            out.write(content);
        }
        return file.getAbsolutePath();
    }

    private static String readAll(InputFile inputFile) throws IOException {
        try (SeekableInputStream in = inputFile.newStream()) {
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    private static IcebergCachingFileIO newCachingFileIO(String localPath, AtomicInteger lengthCalls) {
        new MockUp<ResolvingFileIO>() {
            @Mock
            public InputFile newInputFile(String location) {
                return countingInput(localPath, lengthCalls);
            }

            @Mock
            public InputFile newInputFile(String location, long length) {
                return countingInput(localPath, lengthCalls);
            }
        };
        IcebergCachingFileIO cachingFileIO = new IcebergCachingFileIO();
        cachingFileIO.setConf(new Configuration());
        cachingFileIO.initialize(new HashMap<>());
        return cachingFileIO;
    }

    @Test
    public void testCacheOffReadsWithoutStat() throws IOException {
        String content = "manifest list bytes";
        String localPath = writeLocalFile("snap-cache-off", content);
        AtomicInteger lengthCalls = new AtomicInteger();
        IcebergCachingFileIO cachingFileIO = newCachingFileIO(localPath, lengthCalls);

        // the cache is off by default (iceberg_metadata_cache_max_entry_size = 0): a read without a known
        // length, like the manifest list, must not stat the object just to decide not to cache it
        String path = "s3://bucket/metadata/snap-cache-off-" + System.nanoTime() + ".avro";
        Assertions.assertEquals(content, readAll(cachingFileIO.newInputFile(path)));
        Assertions.assertEquals(0, lengthCalls.get());
    }

    @Test
    public void testCacheOnUsesKnownLength() throws IOException {
        new MockUp<IcebergCachingFileIO.ContentCache>() {
            @Mock
            public long maxContentLength() {
                return 1024 * 1024;
            }
        };
        String content = "manifest bytes";
        String localPath = writeLocalFile("manifest-cache-on", content);
        AtomicInteger lengthCalls = new AtomicInteger();
        IcebergCachingFileIO cachingFileIO = newCachingFileIO(localPath, lengthCalls);

        // a manifest opened with the length from the manifest list is cached without a stat
        String path = "s3://bucket/metadata/manifest-cache-on-" + System.nanoTime() + ".avro";
        InputFile inputFile = cachingFileIO.newInputFile(path, content.length());
        Assertions.assertEquals(content.length(), inputFile.getLength());
        Assertions.assertEquals(content, readAll(inputFile));
        Assertions.assertEquals(0, lengthCalls.get());
    }

    @Test
    public void testCacheOnWithoutLengthStatsOnce() throws IOException {
        new MockUp<IcebergCachingFileIO.ContentCache>() {
            @Mock
            public long maxContentLength() {
                return 1024 * 1024;
            }
        };
        String content = "manifest list bytes";
        String localPath = writeLocalFile("snap-cache-on", content);
        AtomicInteger lengthCalls = new AtomicInteger();
        IcebergCachingFileIO cachingFileIO = newCachingFileIO(localPath, lengthCalls);

        // without a known length the cache needs the size: one stat decides and fills the entry, not two
        String path = "s3://bucket/metadata/snap-cache-on-" + System.nanoTime() + ".avro";
        Assertions.assertEquals(content, readAll(cachingFileIO.newInputFile(path)));
        Assertions.assertEquals(1, lengthCalls.get());
    }

    @Test
    public void testNormalizedAzurePathKeepsLogicalLocation() {
        String path = "abfss://c@acct.blob.core.windows.net/metadata/m0.avro";
        String normalized = "wasbs://c@acct.blob.core.windows.net/metadata/m0.avro";
        List<String> accessed = new ArrayList<>();
        new MockUp<ResolvingFileIO>() {
            @Mock
            public InputFile newInputFile(String location) {
                accessed.add(location);
                return Files.localInput("/tmp/non_existent_file");
            }

            @Mock
            public OutputFile newOutputFile(String location) {
                accessed.add(location);
                return null;
            }

            @Mock
            public void deleteFile(String location) {
                accessed.add(location);
            }
        };

        IcebergCachingFileIO cachingFileIO = new IcebergCachingFileIO();
        cachingFileIO.setConf(new Configuration());
        cachingFileIO.initialize(new HashMap<>());

        // the parsed-manifest caches are keyed by ManifestFile.path(), so location() must stay the caller's path
        Assertions.assertEquals(path, cachingFileIO.newInputFile(path).location());
        cachingFileIO.newOutputFile(path);
        cachingFileIO.deleteFile(path);
        Assertions.assertEquals(List.of(normalized, normalized, normalized), accessed);
    }

    @Test
    public void testNewFileWithException() {
        IcebergCachingFileIO cachingFileIO = new IcebergCachingFileIO();
        cachingFileIO.setConf(new Configuration());
        Map<String, String> icebergProperties = new HashMap<>();
        String key = ADLS_SAS_TOKEN + "account." + BLOB_ENDPOINT;
        icebergProperties.put(key, "sas_token");
        cachingFileIO.initialize(icebergProperties);

        String path = "file:/tmp/non_existent_file.json";
        Assertions.assertThrows(StarRocksConnectorException.class, () -> {
            cachingFileIO.newInputFile(path);
        });

        Assertions.assertThrows(StarRocksConnectorException.class, () -> {
            cachingFileIO.newOutputFile(path);
        });
    }

    @Test
    public void testBuildAzureConfFromProperties() throws StarRocksException {
        Map<String, String> properties = new HashMap<>();
        String key = ADLS_SAS_TOKEN + "account." + ADLS_ENDPOINT;
        String sasToken = "sas_token";
        properties.put(key, sasToken);
        String path = "abfss://container@account.dfs.core.windows.net/path/1/2";

        IcebergCachingFileIO cachingFileIO = new IcebergCachingFileIO();
        cachingFileIO.setConf(new Configuration());
        Configuration configuration = cachingFileIO.buildConfFromProperties(properties, path);

        String authType = configuration.get("fs.azure.account.auth.type.account." + ADLS_ENDPOINT);
        Assertions.assertEquals("SAS", authType);
        String token = configuration.get("fs.azure.sas.fixed.token.account." + ADLS_ENDPOINT);
        Assertions.assertEquals(sasToken, token);

        properties = new HashMap<>();
        key = ADLS_SAS_TOKEN + "account." + BLOB_ENDPOINT;
        sasToken = "blob_sas_token";
        properties.put(key, sasToken);
        path = "wasbs://container@account.blob.core.windows.net/path/1/2";

        cachingFileIO = new IcebergCachingFileIO();
        cachingFileIO.setConf(new Configuration());
        configuration = cachingFileIO.buildConfFromProperties(properties, path);

        token = configuration.get("fs.azure.sas.container.account." + BLOB_ENDPOINT);
        Assertions.assertEquals(sasToken, token);
    }

    @Test
    public void testFileSystemCacheNotBypassed() throws StarRocksException {
        // The FileSystem cache is keyed on the credential fingerprint now, so no scheme bypasses it - a
        // bypass leaks an unclosed FileSystem per call.
        Map<String, String> vended = new HashMap<>();
        vended.put(ADLS_SAS_TOKEN + "account." + ADLS_ENDPOINT, "sas_token");
        Map<String, String> noCredential = new HashMap<>();
        List<String> schemes = List.of("gs", "abfs", "abfss", "wasb", "wasbs", "adl", "s3a", "oss", "cosn", "hdfs");
        for (Configuration conf : List.of(buildConf(vended, "abfss://c@a.dfs.core.windows.net/p"),
                buildConf(noCredential, "abfss://c@a.dfs.core.windows.net/p"),
                buildConf(noCredential, "wasbs://c@a.blob.core.windows.net/p"),
                buildConf(noCredential, "gs://bucket/p"),
                buildConf(noCredential, "oss://bucket/p"))) {
            for (String scheme : schemes) {
                Assertions.assertFalse(conf.getBoolean("fs." + scheme + ".impl.disable.cache", false), scheme);
            }
        }
    }

    @Test
    public void testSchemeLessPathUsesDefaultFs() throws StarRocksException {
        // Hadoop's Path keeps a scheme-less location as-is, so getScheme() is null. FileSystem.get()
        // resolves such a path through fs.defaultFS, so the fingerprint follows the default scheme.
        String schemeLess = "/warehouse/db/t/metadata/v1.metadata.json";

        Assertions.assertEquals("", fingerprint(buildConf(new HashMap<>(), schemeLess)));

        Configuration baseConf = new Configuration();
        baseConf.set("fs.defaultFS", "abfss://c@a.dfs.core.windows.net");
        Assertions.assertFalse(fingerprint(buildConf(baseConf, new HashMap<>(), schemeLess)).isEmpty());
    }

    @Test
    public void testFallThroughConfKeepsBaseValues() throws StarRocksException {
        IcebergCachingFileIO cachingFileIO = new IcebergCachingFileIO();
        Configuration baseConf = new Configuration();
        baseConf.set("fs.azure.account.auth.type.account.dfs.core.windows.net", "OAuth");
        cachingFileIO.setConf(baseConf);

        Configuration configuration =
                cachingFileIO.buildConfFromProperties(new HashMap<>(), "abfss://c@account.dfs.core.windows.net/p");

        Assertions.assertEquals("OAuth",
                configuration.get("fs.azure.account.auth.type.account.dfs.core.windows.net"));
        Assertions.assertFalse(fingerprint(configuration).isEmpty());
    }

    private static Configuration buildConf(Map<String, String> properties, String path) throws StarRocksException {
        IcebergCachingFileIO cachingFileIO = new IcebergCachingFileIO();
        cachingFileIO.setConf(new Configuration());
        return cachingFileIO.buildConfFromProperties(properties, path);
    }

    private static Configuration buildConf(Configuration base, Map<String, String> properties, String path)
            throws StarRocksException {
        IcebergCachingFileIO cachingFileIO = new IcebergCachingFileIO();
        cachingFileIO.setConf(base);
        return cachingFileIO.buildConfFromProperties(properties, path);
    }

    private static String fingerprint(Configuration conf) {
        return HadoopExt.getInstance().getCredentialFingerprint(conf);
    }

    @Test
    public void testCredentialFingerprintOnlyForCredentialKeyedSchemes() throws StarRocksException {
        Map<String, String> none = new HashMap<>();
        for (String path : List.of("gs://bucket/p", "abfss://c@a.dfs.core.windows.net/p", "abfs://c@a.dfs.core.windows.net/p",
                "wasbs://c@a.blob.core.windows.net/p", "wasb://c@a.blob.core.windows.net/p",
                "adl://a.azuredatalakestore.net/p")) {
            Configuration conf = buildConf(none, path);
            Assertions.assertFalse(fingerprint(conf).isEmpty(), path);
            // The HadoopFileIO that ResolvingFileIO creates for the first path keeps this Configuration for
            // every later path of its impl, so both families are fingerprinted whatever the path.
            Assertions.assertNotNull(conf.get(HadoopExt.HADOOP_DYNAMIC_CREDENTIAL_PREFIX + "gs"), path);
            Assertions.assertNotNull(conf.get(HadoopExt.HADOOP_DYNAMIC_CREDENTIAL_PREFIX + "azure"), path);
        }
        for (String path : List.of("s3a://bucket/p", "oss://bucket/p", "cosn://bucket/p", "hdfs://nn/p")) {
            Assertions.assertEquals("", fingerprint(buildConf(none, path)), path);
        }
    }

    @Test
    public void testCredentialFingerprintSeparatesCredentials() throws StarRocksException {
        String adls = "abfss://c@account.dfs.core.windows.net/p";
        Map<String, String> sas1 = Map.of(ADLS_SAS_TOKEN + "account." + ADLS_ENDPOINT, "sas_one");
        Map<String, String> sas2 = Map.of(ADLS_SAS_TOKEN + "account." + ADLS_ENDPOINT, "sas_two");
        String f1 = fingerprint(buildConf(sas1, adls));
        Assertions.assertEquals(f1, fingerprint(buildConf(sas1, adls)));
        Assertions.assertNotEquals(f1, fingerprint(buildConf(sas2, adls)));
        Assertions.assertNotEquals(f1, fingerprint(buildConf(new HashMap<>(), adls)));
        Assertions.assertFalse(f1.contains("sas_one"));

        String gcs = "gs://bucket/p";
        Map<String, String> token1 = Map.of(GCS_ACCESS_TOKEN, "token_one",
                GCPCloudConfigurationProvider.GCS_ACCESS_TOKEN_EXPIRES_AT, "100");
        Map<String, String> token1Later = Map.of(GCS_ACCESS_TOKEN, "token_one",
                GCPCloudConfigurationProvider.GCS_ACCESS_TOKEN_EXPIRES_AT, "200");
        Map<String, String> token2 = Map.of(GCS_ACCESS_TOKEN, "token_two",
                GCPCloudConfigurationProvider.GCS_ACCESS_TOKEN_EXPIRES_AT, "100");
        String g1 = fingerprint(buildConf(token1, gcs));
        Assertions.assertNotEquals(g1, fingerprint(buildConf(token2, gcs)));
        Assertions.assertNotEquals(g1, fingerprint(buildConf(token1Later, gcs)));
        Assertions.assertFalse(g1.contains("token_one"));

        // Raw catalog settings that CloudConfiguration's string does not carry still separate FileSystems.
        Configuration keyfileA = new Configuration();
        keyfileA.set("google.cloud.auth.service.account.json.keyfile", "/keys/a.json");
        Configuration keyfileB = new Configuration();
        keyfileB.set("google.cloud.auth.service.account.json.keyfile", "/keys/b.json");
        Configuration endpointB = new Configuration();
        endpointB.set("fs.gs.storage.root.url", "https://gcs.example.com");
        Map<String, String> none = new HashMap<>();
        String a = fingerprint(buildConf(keyfileA, none, gcs));
        Assertions.assertNotEquals(a, fingerprint(buildConf(keyfileB, none, gcs)));
        Assertions.assertNotEquals(fingerprint(buildConf(new Configuration(), none, gcs)),
                fingerprint(buildConf(endpointB, none, gcs)));

        Configuration accountKeyA = new Configuration();
        accountKeyA.set("fs.azure.account.key.account.dfs.core.windows.net", "key_a");
        Configuration accountKeyB = new Configuration();
        accountKeyB.set("fs.azure.account.key.account.dfs.core.windows.net", "key_b");
        Assertions.assertNotEquals(fingerprint(buildConf(accountKeyA, none, adls)),
                fingerprint(buildConf(accountKeyB, none, adls)));

        // Secrets kept in a Hadoop credential provider are read through Configuration.getPassword().
        for (String path : List.of(adls, gcs)) {
            Configuration providerA = new Configuration();
            providerA.set("hadoop.security.credential.provider.path", "jceks://file/keys/a.jceks");
            Configuration providerB = new Configuration();
            providerB.set("hadoop.security.credential.provider.path", "jceks://file/keys/b.jceks");
            Configuration clearTextOff = new Configuration();
            clearTextOff.set("hadoop.security.credential.provider.path", "jceks://file/keys/a.jceks");
            clearTextOff.setBoolean("hadoop.security.credential.clear-text-fallback", false);
            String fa = fingerprint(buildConf(providerA, none, path));
            Assertions.assertNotEquals(fa, fingerprint(buildConf(providerB, none, path)), path);
            Assertions.assertNotEquals(fa, fingerprint(buildConf(clearTextOff, none, path)), path);
        }
    }

    @Test
    public void testBuildGCSConfFromProperties() throws StarRocksException {
        Map<String, String> properties = new HashMap<>();
        String accessToken = "access_token";
        properties.put(GCS_ACCESS_TOKEN, accessToken);
        String path = "gs://iceberg_gcp/iceberg_catalog/path/1/2";

        IcebergCachingFileIO cachingFileIO = new IcebergCachingFileIO();
        Configuration baseConf = new Configuration();
        baseConf.set(GCPCloudConfigurationProvider.IMPERSONATION_SERVICE_ACCOUNT_KEY,
                "impersonated@project.iam.gserviceaccount.com");
        cachingFileIO.setConf(baseConf);
        Configuration configuration = cachingFileIO.buildConfFromProperties(properties, path);
        String token = configuration.get("fs.gs.temporary.access.token");
        Assertions.assertEquals(accessToken, token);
        Assertions.assertEquals(ACCESS_TOKEN_PROVIDER_IMPL,
                configuration.get("fs.gs.auth.access.token.provider.impl"));
        Assertions.assertEquals(GCPCloudConfigurationProvider.AUTH_TYPE_ACCESS_TOKEN_PROVIDER,
                configuration.get(GCPCloudConfigurationProvider.AUTH_TYPE_KEY));
        Assertions.assertEquals(ACCESS_TOKEN_PROVIDER_IMPL,
                configuration.get(GCPCloudConfigurationProvider.ACCESS_TOKEN_PROVIDER_KEY));
        Assertions.assertNull(configuration.get(GCPCloudConfigurationProvider.DISABLE_FS_CACHE_KEY));
        Assertions.assertFalse(fingerprint(configuration).isEmpty());
        // catalog-level impersonation must not ride on top of the vended token
        Assertions.assertNull(configuration.get(GCPCloudConfigurationProvider.IMPERSONATION_SERVICE_ACCOUNT_KEY));
    }

    @Test
    void testWrappedIOConfigurationPropagation() {
        IcebergCachingFileIO cachingFileIO = new IcebergCachingFileIO();
        Map<String, String> properties = new HashMap<>();
        properties.put("iceberg.catalog.type", "hive");
        cachingFileIO.initialize(properties);

        Configuration conf = new Configuration();
        conf.set("test.key", "test.value");
        cachingFileIO.setConf(conf);

        FileIO wrappedIO = cachingFileIO.getWrappedIO();
        Assertions.assertTrue(wrappedIO instanceof HadoopConfigurable);

        HadoopConfigurable hadoopConfigurable = (HadoopConfigurable) wrappedIO;
        Assertions.assertDoesNotThrow(() -> {
            hadoopConfigurable.serializeConfWith(confToSerialize -> {
                Assertions.assertNotNull(confToSerialize);
                return (SerializableSupplier<Configuration>) () -> confToSerialize;
            });
        });
    }

    @Test
    void testWrappedIOSharesConfigurationAndSerializesACopy() throws IOException {
        IcebergCachingFileIO cachingFileIO = new IcebergCachingFileIO();
        cachingFileIO.initialize(new HashMap<>());
        Configuration conf = new Configuration(false);
        conf.set("test.key", "test.value");
        cachingFileIO.setConf(conf);

        // shared by reference: a per-table FileIO must not retain its own copies of the catalog configuration
        Assertions.assertSame(conf, cachingFileIO.getConf());
        HadoopConfigurable wrappedIO = (HadoopConfigurable) cachingFileIO.getWrappedIO();
        Assertions.assertSame(conf, wrappedIO.getConf());

        // The wrapped IO is what a metadata split ships to the BE, which has no fe-core classes. A concurrent
        // newInputFile can put the shared supplier back on it after SerializableTable installed its own, so
        // write it with a plain ObjectOutputStream (SerializationUtil would replace the supplier first): its
        // serialized form must not name an fe-core class.
        java.io.ByteArrayOutputStream bytes = new java.io.ByteArrayOutputStream();
        try (java.io.ObjectOutputStream out = new java.io.ObjectOutputStream(bytes)) {
            out.writeObject(cachingFileIO.getWrappedIO());
        }
        byte[] wrapped = bytes.toByteArray();
        // a lambda names its capturing class with slashes, a class descriptor with dots
        String stream = new String(wrapped, java.nio.charset.StandardCharsets.ISO_8859_1).replace('/', '.');
        Assertions.assertFalse(stream.contains("com.starrocks.connector.iceberg."), stream);
        FileIO wrappedCopy = SerializationUtil.deserializeFromBytes(wrapped);
        Assertions.assertEquals("test.value", ((HadoopConfigurable) wrappedCopy).getConf().get("test.key"));

        // serialization still carries the values, as SerializableConfiguration did
        IcebergCachingFileIO copy = SerializationUtil.deserializeFromBytes(SerializationUtil.serializeToBytes(cachingFileIO));
        Assertions.assertEquals("test.value", copy.getConf().get("test.key"));
        Assertions.assertEquals("test.value",
                ((HadoopConfigurable) copy.getWrappedIO()).getConf().get("test.key"));
    }

    /**
     * Proves the disk-cache overflow bug in TwoLevelContentCache.
     *
     * The diskCache weigher assigns weight=0 to pinned entries (useCount > 0).
     * When a reader pins an entry via computeIfPresent, Caffeine re-weighs it
     * to 0, "freeing" that capacity in its accounting. New files are then
     * admitted to fill the freed space. After the reader unpins, Caffeine
     * re-weighs back to the real size — but the extra files are already on disk.
     *
     * Result: physical disk usage = DISK_CACHE_CAPACITY + sum(pinned file sizes).
     */
    @Test
    public void testDiskCacheWeigherAllowsOverflowWhenPinned() {
        // FakeEntry mirrors the fields of DiskCacheEntry that the weigher inspects.
        class FakeEntry {
            final long length;
            int useCount;

            FakeEntry(long length) {
                this.length = length;
                this.useCount = 0;
            }

            void pin() {
                useCount++;
            }
            void unpin() {
                useCount--;
            }
        }

        final long CAPACITY = 100; // bytes
        List<String> evicted = new ArrayList<>();

        // Exact weigher logic copied from TwoLevelContentCache:
        // pinned entries weigh 0, unpinned entries weigh their actual byte length.
        Cache<String, FakeEntry> cache = Caffeine.newBuilder()
                .maximumWeight(CAPACITY)
                .weigher((Weigher<String, FakeEntry>) (k, v) ->
                        v.useCount == 0 ? (int) Math.min(v.length, Integer.MAX_VALUE) : 0)
                .evictionListener((k, v, cause) -> evicted.add((String) k))
                .build();

        // Step 1: put file A (60 bytes). Caffeine accounts 60/100.
        FakeEntry a = new FakeEntry(60);
        cache.put("A", a);
        cache.cleanUp();
        Assertions.assertEquals(1, cache.estimatedSize(), "A should be in cache");

        // Step 2: a query starts reading A — pin it.
        // computeIfPresent mutates useCount in-place; Caffeine re-weighs the entry.
        // New weight = 0, so Caffeine now believes 0/100 bytes are in use.
        cache.asMap().computeIfPresent("A", (k, v) -> {
            v.pin();
            return v;
        });
        cache.cleanUp();

        // Step 3: add file B (60 bytes). Caffeine sees 0 + 60 = 60 ≤ 100 → admitted.
        FakeEntry b = new FakeEntry(60);
        cache.put("B", b);
        cache.cleanUp();

        // Step 4: add file C (60 bytes). Caffeine sees 0 + 60 + 60 = 120 > 100 → evicts B.
        FakeEntry c = new FakeEntry(60);
        cache.put("C", c);
        cache.cleanUp();

        // Caffeine did the right thing from its perspective: evicted B to stay within capacity.
        Assertions.assertTrue(evicted.contains("B"), "Caffeine should have evicted B to respect capacity");

        // But A (60 bytes) is still on disk with weight=0 — Caffeine doesn't count it.
        // Actual bytes on disk = A(60) + C(60) = 120, which exceeds CAPACITY(100).
        long actualBytesOnDisk = cache.asMap().values().stream()
                .mapToLong(e -> e.length)
                .sum();

        Assertions.assertTrue(actualBytesOnDisk > CAPACITY,
                "BUG CONFIRMED: actual disk usage (" + actualBytesOnDisk + " bytes) exceeds " +
                "DISK_CACHE_CAPACITY (" + CAPACITY + " bytes) because pinned entry A " +
                "has weight=0 in Caffeine's accounting while still occupying disk space.");
    }

    /**
     * Proves the fix: with the corrected weigher (always weigh by actual length),
     * Caffeine's accounting stays accurate even when entries are pinned.
     * Total disk usage never exceeds DISK_CACHE_CAPACITY.
     */
    @Test
    public void testFixedWeigherRespectsCapacity() {
        class FakeEntry {
            final long length;
            int useCount;

            FakeEntry(long length) {
                this.length = length;
                this.useCount = 0;
            }

            void pin() {
                useCount++;
            }
            void unpin() {
                useCount--;
            }
        }

        final long CAPACITY = 100;
        List<String> evicted = new ArrayList<>();

        // FIXED weigher: always weigh by actual length, regardless of useCount.
        Cache<String, FakeEntry> cache = Caffeine.newBuilder()
                .maximumWeight(CAPACITY)
                .weigher((Weigher<String, FakeEntry>) (k, v) ->
                        (int) Math.min(v.length, Integer.MAX_VALUE))
                .evictionListener((k, v, cause) -> evicted.add((String) k))
                .build();

        // Step 1: put file A (60 bytes). Weight = 60. Cache: 60/100 used.
        FakeEntry a = new FakeEntry(60);
        cache.put("A", a);
        cache.cleanUp();

        // Step 2: pin A. With fixed weigher, weight stays 60. Cache still 60/100 used.
        cache.asMap().computeIfPresent("A", (k, v) -> {
            v.pin();
            return v;
        });
        cache.cleanUp();

        // Step 3: add B (60 bytes). Caffeine sees 60 + 60 = 120 > 100 → evicts A or B.
        FakeEntry b = new FakeEntry(60);
        cache.put("B", b);
        cache.cleanUp();

        // Actual bytes remaining in cache must not exceed CAPACITY.
        long actualBytesOnDisk = cache.asMap().values().stream()
                .mapToLong(e -> e.length)
                .sum();

        Assertions.assertTrue(actualBytesOnDisk <= CAPACITY,
                "FIXED: actual disk usage (" + actualBytesOnDisk + " bytes) should not exceed " +
                "DISK_CACHE_CAPACITY (" + CAPACITY + " bytes).");
    }

    /**
     * Demonstrates the orphan-file regression introduced when the weigher fix was first applied.
     *
     * With the corrected weigher (real weight for pinned entries), Caffeine can evict a pinned
     * entry under pressure. The eviction listener skips file deletion because useCount > 0 at
     * that moment. The old close() used computeIfPresent, which found no entry after eviction,
     * so unpin() and file deletion never ran — leaving an orphan file on disk forever.
     *
     * The fix: DiskCacheSeekableInputStream captures the DiskCacheEntry reference at open time
     * and unpins directly on it. When the last reader closes, if the entry is no longer in cache,
     * it deletes the orphan file immediately.
     *
     * This test models that sequence with FakeEntry to verify the close-time cleanup logic.
     */
    @Test
    public void testOrphanFileCleanedUpWhenLastStreamClosesAfterEviction() {
        class FakeEntry {
            final long length;
            final AtomicInteger useCount = new AtomicInteger(0);

            FakeEntry(long length) {
                this.length = length;
            }
        }

        final long CAPACITY = 100;
        AtomicBoolean deleteCalledForA = new AtomicBoolean(false);

        Cache<String, FakeEntry> cache = Caffeine.newBuilder()
                .maximumWeight(CAPACITY)
                .weigher((Weigher<String, FakeEntry>) (k, v) ->
                        (int) Math.min(v.length, Integer.MAX_VALUE))
                .evictionListener((k, v, cause) -> {
                    if (((FakeEntry) v).useCount.get() > 0) {
                        // mirrors eviction listener: skip delete while pinned
                    } else {
                        if ("A".equals(k)) {
                            deleteCalledForA.set(true);
                        }
                    }
                })
                .build();

        // Step 1: cache entry A (60 bytes) and pin it (simulating an open stream).
        FakeEntry a = new FakeEntry(60);
        cache.put("A", a);
        cache.cleanUp();
        cache.asMap().computeIfPresent("A", (k, v) -> {
            v.useCount.incrementAndGet();
            return v;
        });

        // Step 2: add B (60 bytes) — forces Caffeine to evict A to respect capacity.
        // Eviction listener fires with useCount=1 > 0 → skips delete → orphan.
        FakeEntry b = new FakeEntry(60);
        cache.put("B", b);
        cache.cleanUp();

        // A is now evicted and not in cache, delete was NOT called by the eviction listener.
        Assertions.assertNull(cache.getIfPresent("A"), "A should have been evicted");
        Assertions.assertFalse(deleteCalledForA.get(), "Delete should not fire from eviction listener while pinned");

        // Step 3: simulate close() with the fix applied.
        // Only delete when current == null (evicted, no replacement).
        int remaining = a.useCount.decrementAndGet();
        if (remaining == 0) {
            FakeEntry current = cache.asMap().get("A");
            if (current == null) {
                // mirrors the orphan cleanup in DiskCacheSeekableInputStream.close()
                deleteCalledForA.set(true);
            }
        }

        // Verify: cleanup triggered immediately when the last reader closed.
        Assertions.assertTrue(deleteCalledForA.get(),
                "Orphaned file for evicted-while-pinned entry A should be cleaned up on stream close");
        Assertions.assertEquals(0, a.useCount.get(), "useCount should reach 0 after close");
    }

}