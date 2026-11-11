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

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.cache.Cache;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.common.Config;
import com.starrocks.common.Pair;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.connector.ConnectorMetadataCache;
import com.starrocks.connector.MetastoreType;
import com.starrocks.connector.hive.HiveMetaClient;
import com.starrocks.connector.hive.HiveMetastore;
import com.starrocks.connector.hive.HiveMetastoreTest;
import com.starrocks.connector.hive.IHiveMetastore;
import com.starrocks.memory.estimate.Estimator;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.FileStatus;
import mockit.MockUp;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class DeltaLakeMetastoreTest {
    private DeltaLakeMetastore metastore;

    @BeforeEach
    public void setUp() throws Exception {
        HiveMetaClient client = new HiveMetastoreTest.MockedHiveMetaClient();
        IHiveMetastore hiveMetastore = new HiveMetastore(client, "delta0", MetastoreType.HMS);
        DeltaLakeCatalogProperties properties = new DeltaLakeCatalogProperties(Maps.newHashMap());
        metastore = new HMSBackedDeltaMetastore("delta0", hiveMetastore, new Configuration(), properties);
    }

    @Test
    public void testCheckpointCacheWeigherUsesExpectedEstimatorSamplingArgs() throws Exception {
        AtomicInteger keyEstimateCalls = new AtomicInteger();
        AtomicInteger valueEstimateCalls = new AtomicInteger();
        AtomicInteger sampledSize = new AtomicInteger();
        AtomicInteger sampledDepth = new AtomicInteger();

        new MockUp<Estimator>() {
            @mockit.Mock
            public long estimate(Object obj) {
                keyEstimateCalls.incrementAndGet();
                return 100L;
            }

            @mockit.Mock
            public long estimate(Object obj, int sampleSize, int maxDepth) {
                valueEstimateCalls.incrementAndGet();
                sampledSize.set(sampleSize);
                sampledDepth.set(maxDepth);
                return 200L;
            }
        };

        Cache<Pair<DeltaLakeFileStatus, StructType>, List<ColumnarBatch>> checkpointCache =
                getCheckpointCache(createMetastoreForTest());
        checkpointCache.put(createCheckpointCacheKey(), Lists.newArrayList());

        Assertions.assertTrue(keyEstimateCalls.get() > 0);
        Assertions.assertTrue(valueEstimateCalls.get() > 0);
        Assertions.assertEquals(20, sampledSize.get());
        Assertions.assertEquals(20, sampledDepth.get());
    }

    @Test
    public void testJsonCacheWeigherUsesExpectedEstimatorSamplingArgs() throws Exception {
        AtomicInteger keyEstimateCalls = new AtomicInteger();
        AtomicInteger valueEstimateCalls = new AtomicInteger();
        AtomicInteger sampledSize = new AtomicInteger();
        AtomicInteger sampledDepth = new AtomicInteger();

        new MockUp<Estimator>() {
            @mockit.Mock
            public long estimate(Object obj) {
                keyEstimateCalls.incrementAndGet();
                return 100L;
            }

            @mockit.Mock
            public long estimate(Object obj, int sampleSize, int maxDepth) {
                valueEstimateCalls.incrementAndGet();
                sampledSize.set(sampleSize);
                sampledDepth.set(maxDepth);
                return 200L;
            }
        };

        Cache<DeltaLakeFileStatus, List<JsonNode>> jsonCache = getJsonCache(createMetastoreForTest());
        jsonCache.put(createJsonCacheKey(), Lists.newArrayList());

        Assertions.assertTrue(keyEstimateCalls.get() > 0);
        Assertions.assertTrue(valueEstimateCalls.get() > 0);
        Assertions.assertEquals(20, sampledSize.get());
        Assertions.assertEquals(20, sampledDepth.get());
    }

    @Test
    public void testCheckpointCacheWeigherFallsBackToStructureEstimatorWhenEstimatorFails() throws Exception {
        AtomicInteger fallbackCalls = new AtomicInteger();

        new MockUp<Estimator>() {
            @mockit.Mock
            public long estimate(Object obj) {
                throw new RuntimeException("mock estimator failure");
            }
        };

        new MockUp<DeltaLakeCacheSizeEstimator>() {
            @mockit.Mock
            public long estimateCheckpointByStructure(Pair<DeltaLakeFileStatus, StructType> key, List<ColumnarBatch> value) {
                fallbackCalls.incrementAndGet();
                return 123L;
            }
        };

        Cache<Pair<DeltaLakeFileStatus, StructType>, List<ColumnarBatch>> checkpointCache =
                getCheckpointCache(createMetastoreForTest());
        checkpointCache.put(createCheckpointCacheKey(), Lists.newArrayList());

        Assertions.assertTrue(fallbackCalls.get() > 0);
    }

    @Test
    public void testJsonCacheWeigherFallsBackToStructureEstimatorWhenEstimatorFails() throws Exception {
        AtomicInteger fallbackCalls = new AtomicInteger();

        new MockUp<Estimator>() {
            @mockit.Mock
            public long estimate(Object obj) {
                throw new RuntimeException("mock estimator failure");
            }
        };

        new MockUp<DeltaLakeCacheSizeEstimator>() {
            @mockit.Mock
            public long estimateJsonByStructure(DeltaLakeFileStatus key, List<JsonNode> value) {
                fallbackCalls.incrementAndGet();
                return 123L;
            }
        };

        Cache<DeltaLakeFileStatus, List<JsonNode>> jsonCache = getJsonCache(createMetastoreForTest());
        jsonCache.put(createJsonCacheKey(), Lists.newArrayList());

        Assertions.assertTrue(fallbackCalls.get() > 0);
    }

    // With the global ratios at 0 (the default) each catalog keeps private checkpoint and log caches sized by its
    // properties as before; a kind whose global ratio is above 0 goes to the store all Delta Lake catalogs share.
    // Dropping a catalog drops its entries from either.
    @Test
    public void testPrivateByDefaultSharedWhenGlobalRatioSet() throws Exception {
        DeltaLakeMetastore first = createMetastoreForTest();
        DeltaLakeMetastore second = createMetastoreForTest();
        ConnectorMetadataCache.Store checkpoints = Deencapsulation.getField(first, "checkpointStore");
        Assertions.assertNotSame(checkpoints, Deencapsulation.getField(second, "checkpointStore"));
        Assertions.assertEquals(Math.round(Runtime.getRuntime().maxMemory() * 0.1), checkpoints.maximum());

        double old = Config.deltalake_json_meta_cache_global_memory_ratio;
        Config.deltalake_json_meta_cache_global_memory_ratio = 0.05;
        DeltaLakeMetastore sharedA;
        DeltaLakeMetastore sharedB;
        try {
            sharedA = createMetastoreForTest();
            sharedB = createMetastoreForTest();
        } finally {
            Config.deltalake_json_meta_cache_global_memory_ratio = old;
        }
        ConnectorMetadataCache.Store logs = Deencapsulation.getField(sharedA, "jsonStore");
        Assertions.assertSame(logs, Deencapsulation.getField(sharedB, "jsonStore"));
        Assertions.assertEquals(Math.round(Runtime.getRuntime().maxMemory() * 0.05), logs.maximum());
        Assertions.assertNotSame(Deencapsulation.getField(sharedA, "checkpointStore"),
                Deencapsulation.getField(sharedB, "checkpointStore"));

        // the same file in two catalogs is two entries, and one catalog's release leaves the other's
        Cache<DeltaLakeFileStatus, List<JsonNode>> logsA = getJsonCache(sharedA);
        Cache<DeltaLakeFileStatus, List<JsonNode>> logsB = getJsonCache(sharedB);
        logsA.put(createJsonCacheKey(), Lists.newArrayList());
        Assertions.assertNull(logsB.getIfPresent(createJsonCacheKey()));
        logsB.put(createJsonCacheKey(), Lists.newArrayList());
        Assertions.assertEquals(1, ((ConnectorMetadataCache.ScopedGuavaCache<?, ?>) logsA).keys().size());
        sharedA.releaseCaches();
        Assertions.assertNull(logsA.getIfPresent(createJsonCacheKey()));
        Assertions.assertNotNull(logsB.getIfPresent(createJsonCacheKey()));
        sharedB.releaseCaches();
        first.releaseCaches();
        second.releaseCaches();
    }

    // The catalog-level wrapper reports its delegate's file caches by the delegate's own ledger, so a catalog's
    // report does not move when another catalog sharing the store caches more.
    @Test
    public void testWrapperReportsTheDelegateLedger() throws Exception {
        double old = Config.deltalake_json_meta_cache_global_memory_ratio;
        Config.deltalake_json_meta_cache_global_memory_ratio = 0.05;
        DeltaLakeMetastore a;
        DeltaLakeMetastore b;
        try {
            a = createMetastoreForTest();
            b = createMetastoreForTest();
        } finally {
            Config.deltalake_json_meta_cache_global_memory_ratio = old;
        }
        CachingDeltaLakeMetastore wrapperA =
                CachingDeltaLakeMetastore.createCatalogLevelInstance(a, Runnable::run, 3600, -1, 100);
        try {
            getJsonCache(a).put(createJsonCacheKey(), Lists.newArrayList());
            long before = wrapperA.estimateSize();
            Assertions.assertTrue(before >= a.estimateSize() && a.estimateSize() > 0);
            getJsonCache(b).put(DeltaLakeFileStatus.of(FileStatus.of("s3://bucket/other.json", 100, 123)),
                    Lists.newArrayList());
            Assertions.assertEquals(before, wrapperA.estimateSize());
        } finally {
            a.releaseCaches();
            b.releaseCaches();
        }
    }

    private DeltaLakeMetastore createMetastoreForTest() {
        HiveMetaClient client = new HiveMetastoreTest.MockedHiveMetaClient();
        IHiveMetastore hiveMetastore = new HiveMetastore(client, "delta0", MetastoreType.HMS);
        DeltaLakeCatalogProperties properties = new DeltaLakeCatalogProperties(Maps.newHashMap());
        return new HMSBackedDeltaMetastore("delta0", hiveMetastore, new Configuration(), properties);
    }

    @SuppressWarnings("unchecked")
    private Cache<Pair<DeltaLakeFileStatus, StructType>, List<ColumnarBatch>> getCheckpointCache(
            DeltaLakeMetastore testMetastore) throws Exception {
        Field checkpointCacheField = DeltaLakeMetastore.class.getDeclaredField("checkpointCache");
        checkpointCacheField.setAccessible(true);
        return (Cache<Pair<DeltaLakeFileStatus, StructType>, List<ColumnarBatch>>) checkpointCacheField.get(testMetastore);
    }

    @SuppressWarnings("unchecked")
    private Cache<DeltaLakeFileStatus, List<JsonNode>> getJsonCache(DeltaLakeMetastore testMetastore) throws Exception {
        Field jsonCacheField = DeltaLakeMetastore.class.getDeclaredField("jsonCache");
        jsonCacheField.setAccessible(true);
        return (Cache<DeltaLakeFileStatus, List<JsonNode>>) jsonCacheField.get(testMetastore);
    }

    private Pair<DeltaLakeFileStatus, StructType> createCheckpointCacheKey() {
        DeltaLakeFileStatus deltaLakeFileStatus = DeltaLakeFileStatus.of(
                FileStatus.of("s3://bucket/path/to/file.parquet", 100, 123));
        StructType structType = new StructType(Lists.newArrayList());
        return Pair.create(deltaLakeFileStatus, structType);
    }

    private DeltaLakeFileStatus createJsonCacheKey() {
        return DeltaLakeFileStatus.of(FileStatus.of("s3://bucket/path/to/file.json", 100, 123));
    }
}
