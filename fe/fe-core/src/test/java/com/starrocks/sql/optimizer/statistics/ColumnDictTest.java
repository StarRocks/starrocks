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

package com.starrocks.sql.optimizer.statistics;

import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.google.common.collect.ImmutableMap;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ColumnId;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.SinglePartitionInfo;
import com.starrocks.common.Config;
import com.starrocks.common.Pair;
import com.starrocks.planner.DataPartition;
import com.starrocks.planner.FragmentNormalizer;
import com.starrocks.planner.PlanFragment;
import com.starrocks.planner.PlanFragmentId;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.sql.optimizer.base.ColumnIdentifier;
import com.starrocks.thrift.TPlanFragment;
import com.starrocks.type.IntegerType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public class ColumnDictTest {
    private int previousLowCardinalityThreshold = 0;

    @BeforeEach
    void setUP() {
        previousLowCardinalityThreshold = Config.low_cardinality_threshold;
        Config.low_cardinality_threshold = 512;
    }

    @AfterEach
    void tearDown() {
        Config.low_cardinality_threshold = previousLowCardinalityThreshold;
    }

    @Test
    void checkLowCardinalityConfigAvailable() {
        ImmutableMap.Builder<ByteBuffer, Integer> builder = ImmutableMap.builder();

        for (int i = 0; i < 300; i++) {
            String key = "string-" + i;
            byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
            ByteBuffer keyBuffer = ByteBuffer.allocate(keyBytes.length);
            keyBuffer.put(keyBytes);
            keyBuffer.flip();

            builder.put(keyBuffer, i);
        }
        ImmutableMap<ByteBuffer, Integer> dictMap = builder.build();

        ColumnDict dict = new ColumnDict(dictMap, 1);
        Assertions.assertEquals(300, dict.getDictSize());
    }

    @Test
    public void testMergeDictSuccess() {
        ImmutableMap.Builder<ByteBuffer, Integer> builder1 = ImmutableMap.builder();
        ImmutableMap.Builder<ByteBuffer, Integer> builder2 = ImmutableMap.builder();

        addWords(builder1, 1, 101, "common-");
        addWords(builder1, 101, 121, "unique1-");
        addWords(builder2, 1, 101, "common-");
        addWords(builder2, 101, 131, "unique2-");

        ImmutableMap<ByteBuffer, Integer> dictMap1 = builder1.build();
        ImmutableMap<ByteBuffer, Integer> dictMap2 = builder2.build();
        ColumnDict dict1 = new ColumnDict(dictMap1, 1);
        ColumnDict dict2 = new ColumnDict(dictMap2, 2);

        Pair<ColumnDict, ColumnDict> res = ColumnDict.merge(dict1, dict2);

        Assertions.assertNotNull(res);
        ColumnDict newDict1 = res.first;
        ColumnDict newDict2 = res.second;
        Assertions.assertEquals(newDict1.getVersion(), dict1.getVersion());
        Assertions.assertEquals(newDict2.getVersion(), dict2.getVersion());

        Assertions.assertEquals(150, newDict1.getDictSize());
        for (int i = 1; i < 101; i++) {
            String key = "common-" + i;
            ByteBuffer keyBuffer = toByteBuffer(key);
            Assertions.assertEquals(i, newDict1.getDict().get(keyBuffer));
        }
        for (int i = 101; i < 121; i++) {
            String key = "unique1-" + i;
            ByteBuffer keyBuffer = toByteBuffer(key);
            Assertions.assertEquals(i, newDict1.getDict().get(keyBuffer));
        }
        for (int i = 101; i < 131; i++) {
            String key = "unique2-" + i;
            ByteBuffer keyBuffer = toByteBuffer(key);
            Assertions.assertEquals(i + 20, newDict1.getDict().get(keyBuffer));
        }

        Assertions.assertEquals(newDict1.getDict(), newDict2.getDict());

        res = ColumnDict.merge(newDict1, newDict2);

        Assertions.assertNotNull(res);
        ColumnDict newDict11 = res.first;
        ColumnDict newDict21 = res.second;
        Assertions.assertEquals(newDict11.getDict(), newDict1.getDict());
        Assertions.assertEquals(newDict11.getDict(), newDict21.getDict());
    }

    @Test
    public void testMergeDictWithHighBitBytesUtf8() {
        // Regression test: BE sorts dict strings via memcmp (unsigned bytes) and assigns ids in
        // that order. ByteBuffer.compareTo on JDK 8 compares bytes as signed, which inverts the
        // order of any UTF-8 string with a high-bit byte. Walking the merge with a signed
        // comparator over a BE-sorted (unsigned) array put d2's high-byte key at newIdx=1, then
        // drained d1 and put the same key again at a later newIdx, failing ImmutableMap.build()
        // with "Multiple entries with same key".
        // Minimal repro: ASCII '7' (0x37) vs Cyrillic 'В' (UTF-8 first byte 0xD0). Signed: 0x37 -
        // (signed 0xD0) = +103. Unsigned: 0x37 - 0xD0 = -153.
        ImmutableMap.Builder<ByteBuffer, Integer> builder1 = ImmutableMap.builder();
        ImmutableMap.Builder<ByteBuffer, Integer> builder2 = ImmutableMap.builder();

        builder1.put(toByteBuffer("78"), 1);
        builder1.put(toByteBuffer("В"), 2);

        builder2.put(toByteBuffer("В"), 1);

        ColumnDict dict1 = new ColumnDict(builder1.build(), 1);
        ColumnDict dict2 = new ColumnDict(builder2.build(), 2);

        Pair<ColumnDict, ColumnDict> res = ColumnDict.merge(dict1, dict2);

        Assertions.assertNotNull(res);
        Assertions.assertEquals(2, res.first.getDictSize());
        Assertions.assertEquals(1, res.first.getDict().get(toByteBuffer("78")));
        Assertions.assertEquals(2, res.first.getDict().get(toByteBuffer("В")));
        Assertions.assertEquals(res.first.getDict(), res.second.getDict());
    }

    @Test
    public void testMergeDictFail() {
        ImmutableMap.Builder<ByteBuffer, Integer> builder1 = ImmutableMap.builder();
        ImmutableMap.Builder<ByteBuffer, Integer> builder2 = ImmutableMap.builder();

        addWords(builder1, 1, 101, "common-");
        addWords(builder1, 101, 121, "unique1-");
        addWords(builder2, 1, 101, "common-");
        addWords(builder2, 101, 511, "unique2-");

        ImmutableMap<ByteBuffer, Integer> dictMap1 = builder1.build();
        ImmutableMap<ByteBuffer, Integer> dictMap2 = builder2.build();
        ColumnDict dict1 = new ColumnDict(dictMap1, 1);
        ColumnDict dict2 = new ColumnDict(dictMap2, 2);

        Pair<ColumnDict, ColumnDict> res = ColumnDict.merge(dict1, dict2);

        Assertions.assertNull(res);
    }

    private void addWords(ImmutableMap.Builder<ByteBuffer, Integer> builder, int start, int end, String prefix) {
        for (int i = start; i < end; i++) {
            String key = prefix + i;
            ByteBuffer keyBuffer = toByteBuffer(key);
            builder.put(keyBuffer, i);
        }
    }

    private ByteBuffer toByteBuffer(String str) {
        byte[] keyBytes = str.getBytes(StandardCharsets.UTF_8);
        ByteBuffer keyBuffer = ByteBuffer.allocate(keyBytes.length);
        keyBuffer.put(keyBytes);
        keyBuffer.flip();
        return keyBuffer;
    }

    private ImmutableMap<ByteBuffer, Integer> dictOf(boolean reverseInsertion, String... words) {
        String[] sorted = words.clone();
        java.util.Arrays.sort(sorted);
        java.util.Map<String, Integer> ids = new java.util.HashMap<>();
        for (int i = 0; i < sorted.length; i++) {
            ids.put(sorted[i], i + 1);
        }
        ImmutableMap.Builder<ByteBuffer, Integer> builder = ImmutableMap.builder();
        for (int i = 0; i < sorted.length; i++) {
            String word = reverseInsertion ? sorted[sorted.length - 1 - i] : sorted[i];
            builder.put(toByteBuffer(word), ids.get(word));
        }
        return builder.build();
    }

    @Test
    public void testContentIdentityDistinguishesContentUnderSameCollectedVersion() {
        long collectedVersion = 1789369019856L;
        ColumnDict follower = new ColumnDict(dictOf(false, "A", "B", "C"), collectedVersion);
        ColumnDict leader = new ColumnDict(dictOf(false, "A", "B", "C", "D"), collectedVersion);

        Assertions.assertEquals(follower.getCollectedVersion(), leader.getCollectedVersion());
        Assertions.assertNotEquals(follower.getContentIdentity(), leader.getContentIdentity());
    }

    @Test
    public void testContentIdentityIgnoresInsertionOrderAndCollectedVersion() {
        ColumnDict dict = new ColumnDict(dictOf(false, "A", "B", "C"), 100);
        ColumnDict reordered = new ColumnDict(dictOf(true, "A", "B", "C"), 100);
        ColumnDict laterCollection = new ColumnDict(dictOf(false, "A", "B", "C"), 200);

        Assertions.assertEquals(dict.getContentIdentity(), reordered.getContentIdentity());
        Assertions.assertEquals(dict.getContentIdentity(), laterCollection.getContentIdentity());
    }

    @Test
    public void testContentIdentityDistinguishesCodes() {
        ImmutableMap<ByteBuffer, Integer> shifted = ImmutableMap.<ByteBuffer, Integer>builder()
                .put(toByteBuffer("A"), 2)
                .put(toByteBuffer("B"), 3)
                .build();
        ColumnDict original = new ColumnDict(dictOf(false, "A", "B"), 100);
        ColumnDict recoded = new ColumnDict(shifted, 100);

        Assertions.assertNotEquals(original.getContentIdentity(), recoded.getContentIdentity());
    }

    @Test
    public void testRenewalKeepsContentIdentity() {
        ColumnDict dict = new ColumnDict(dictOf(false, "A", "B", "C"), 100);
        long before = dict.getContentIdentity();
        dict.updateVersion(200);

        Assertions.assertEquals(before, dict.getContentIdentity());
        Assertions.assertEquals(200, dict.getVersion());
    }

    private static final long RENEWAL_TABLE_ID = 1789369019L;
    private static final ColumnId RENEWAL_COLUMN = ColumnId.create("c0");
    private static final long COLLECTED_VERSION = 1789369019856L;
    private static final long VISIBLE_TIME = COLLECTED_VERSION + 1000;

    @SuppressWarnings("unchecked")
    private static AsyncLoadingCache<ColumnIdentifier, Optional<ColumnDict>> dictCache() throws Exception {
        Field field = CacheDictManager.class.getDeclaredField("dictStatistics");
        field.setAccessible(true);
        return (AsyncLoadingCache<ColumnIdentifier, Optional<ColumnDict>>) field.get(CacheDictManager.getInstance());
    }

    private static ColumnDict renewAfterLoad(ColumnDict cached, long echoedVersion) throws Exception {
        ColumnIdentifier id = new ColumnIdentifier(RENEWAL_TABLE_ID, RENEWAL_COLUMN);
        OlapTable table = new OlapTable(RENEWAL_TABLE_ID, "t0", List.of(new Column("c0", IntegerType.BIGINT)),
                KeysType.DUP_KEYS, new SinglePartitionInfo(), null);
        dictCache().put(id, CompletableFuture.completedFuture(Optional.of(cached)));
        try {
            CacheDictManager.getInstance().updateGlobalDict(table, RENEWAL_COLUMN, echoedVersion, VISIBLE_TIME);
            CompletableFuture<Optional<ColumnDict>> after = dictCache().getIfPresent(id);
            return after == null ? null : after.get().orElse(null);
        } finally {
            dictCache().synchronous().invalidate(id);
        }
    }

    @Test
    public void testRenewalEvictsDivergentContentUnderSameCollectedVersion() throws Exception {
        ColumnDict follower = new ColumnDict(dictOf(false, "A", "B", "C"), COLLECTED_VERSION);
        ColumnDict validated = new ColumnDict(dictOf(false, "A", "B", "C", "D"), COLLECTED_VERSION);

        Assertions.assertNull(renewAfterLoad(follower, validated.getContentIdentity()));
    }

    @Test
    public void testRenewalByMatchingContentIdentityAcrossCollectedVersions() throws Exception {
        ColumnDict cached = new ColumnDict(dictOf(false, "A", "B", "C"), COLLECTED_VERSION);
        long echoed = new ColumnDict(dictOf(false, "A", "B", "C"), COLLECTED_VERSION - 500).getContentIdentity();

        ColumnDict after = renewAfterLoad(cached, echoed);
        Assertions.assertNotNull(after);
        Assertions.assertEquals(VISIBLE_TIME, after.getVersion());
    }

    @Test
    public void testRenewalByCollectedVersionFromOlderPlanner() throws Exception {
        ColumnDict cached = new ColumnDict(dictOf(false, "A", "B", "C"), COLLECTED_VERSION);

        ColumnDict after = renewAfterLoad(cached, COLLECTED_VERSION);
        Assertions.assertNotNull(after);
        Assertions.assertEquals(VISIBLE_TIME, after.getVersion());
    }

    @Test
    public void testRenewalEvictsStaleCollectedVersion() throws Exception {
        ColumnDict cached = new ColumnDict(dictOf(false, "A", "B", "C"), COLLECTED_VERSION);

        Assertions.assertNull(renewAfterLoad(cached, COLLECTED_VERSION - 1));
    }

    @Test
    public void testLoadDictSendsContentIdentityWhileQueryDictKeepsCollectedVersion() {
        ColumnDict first = new ColumnDict(dictOf(false, "A", "B"), COLLECTED_VERSION);
        ColumnDict second = new ColumnDict(dictOf(false, "X", "Y", "Z"), COLLECTED_VERSION + 1);
        List<Pair<Integer, ColumnDict>> dicts = List.of(new Pair<>(5, first), new Pair<>(7, second));
        PlanFragment fragment = new PlanFragment(new PlanFragmentId(0), null, DataPartition.UNPARTITIONED);
        fragment.setLoadGlobalDicts(dicts);
        fragment.setQueryGlobalDicts(dicts);

        TPlanFragment thrift = fragment.toThrift();

        Assertions.assertEquals(5, thrift.getLoad_global_dicts().get(0).getColumnId());
        Assertions.assertEquals(first.getContentIdentity(), thrift.getLoad_global_dicts().get(0).getVersion());
        Assertions.assertEquals(7, thrift.getLoad_global_dicts().get(1).getColumnId());
        Assertions.assertEquals(second.getContentIdentity(), thrift.getLoad_global_dicts().get(1).getVersion());
        Assertions.assertEquals(COLLECTED_VERSION, thrift.getQuery_global_dicts().get(0).getVersion());
        Assertions.assertEquals(COLLECTED_VERSION + 1, thrift.getQuery_global_dicts().get(1).getVersion());
    }

    @Test
    public void testQueryCacheDigestUsesContentIdentity() {
        PlanFragment fragment = new PlanFragment(new PlanFragmentId(0), null, DataPartition.UNPARTITIONED);
        ColumnDict follower = new ColumnDict(dictOf(false, "A", "B", "C"), COLLECTED_VERSION);
        ColumnDict leader = new ColumnDict(dictOf(false, "A", "B", "C", "D"), COLLECTED_VERSION);
        ColumnDict recollected = new ColumnDict(dictOf(false, "A", "B", "C"), COLLECTED_VERSION + 500);

        long followerKey = fragment.normalizeDicts(List.of(new Pair<>(5, follower)),
                new FragmentNormalizer(null, null)).get(0).getVersion();
        long leaderKey = fragment.normalizeDicts(List.of(new Pair<>(5, leader)),
                new FragmentNormalizer(null, null)).get(0).getVersion();
        long recollectedKey = fragment.normalizeDicts(List.of(new Pair<>(5, recollected)),
                new FragmentNormalizer(null, null)).get(0).getVersion();

        Assertions.assertNotEquals(followerKey, leaderKey);
        Assertions.assertEquals(followerKey, recollectedKey);
    }
}
