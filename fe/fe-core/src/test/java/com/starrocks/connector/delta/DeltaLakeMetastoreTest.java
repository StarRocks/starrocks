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
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.common.Pair;
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

        LoadingCache<Pair<DeltaLakeFileStatus, StructType>, List<ColumnarBatch>> checkpointCache =
                getCheckpointCache(createMetastoreForTest());
        checkpointCache.put(createCheckpointCacheKey(), Lists.newArrayList());

        Assertions.assertTrue(keyEstimateCalls.get() > 0);
        Assertions.assertTrue(valueEstimateCalls.get() > 0);
        Assertions.assertEquals(3, sampledSize.get());
        Assertions.assertEquals(8, sampledDepth.get());
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

        LoadingCache<DeltaLakeFileStatus, List<JsonNode>> jsonCache = getJsonCache(createMetastoreForTest());
        jsonCache.put(createJsonCacheKey(), Lists.newArrayList());

        Assertions.assertTrue(keyEstimateCalls.get() > 0);
        Assertions.assertTrue(valueEstimateCalls.get() > 0);
        Assertions.assertEquals(5, sampledSize.get());
        Assertions.assertEquals(10, sampledDepth.get());
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

        LoadingCache<Pair<DeltaLakeFileStatus, StructType>, List<ColumnarBatch>> checkpointCache =
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

        LoadingCache<DeltaLakeFileStatus, List<JsonNode>> jsonCache = getJsonCache(createMetastoreForTest());
        jsonCache.put(createJsonCacheKey(), Lists.newArrayList());

        Assertions.assertTrue(fallbackCalls.get() > 0);
    }

    private DeltaLakeMetastore createMetastoreForTest() {
        HiveMetaClient client = new HiveMetastoreTest.MockedHiveMetaClient();
        IHiveMetastore hiveMetastore = new HiveMetastore(client, "delta0", MetastoreType.HMS);
        DeltaLakeCatalogProperties properties = new DeltaLakeCatalogProperties(Maps.newHashMap());
        return new HMSBackedDeltaMetastore("delta0", hiveMetastore, new Configuration(), properties);
    }

    @SuppressWarnings("unchecked")
    private LoadingCache<Pair<DeltaLakeFileStatus, StructType>, List<ColumnarBatch>> getCheckpointCache(
            DeltaLakeMetastore testMetastore) throws Exception {
        Field checkpointCacheField = DeltaLakeMetastore.class.getDeclaredField("checkpointCache");
        checkpointCacheField.setAccessible(true);
        return (LoadingCache<Pair<DeltaLakeFileStatus, StructType>, List<ColumnarBatch>>) checkpointCacheField.get(testMetastore);
    }

    @SuppressWarnings("unchecked")
    private LoadingCache<DeltaLakeFileStatus, List<JsonNode>> getJsonCache(DeltaLakeMetastore testMetastore) throws Exception {
        Field jsonCacheField = DeltaLakeMetastore.class.getDeclaredField("jsonCache");
        jsonCacheField.setAccessible(true);
        return (LoadingCache<DeltaLakeFileStatus, List<JsonNode>>) jsonCacheField.get(testMetastore);
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
