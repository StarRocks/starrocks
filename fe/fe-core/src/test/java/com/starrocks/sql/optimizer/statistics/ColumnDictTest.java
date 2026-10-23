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
import com.starrocks.catalog.ColumnId;
import com.starrocks.common.Config;
import com.starrocks.common.Pair;
import com.starrocks.planner.DataPartition;
import com.starrocks.planner.FragmentNormalizer;
import com.starrocks.planner.PlanFragment;
import com.starrocks.planner.PlanFragmentId;
import com.starrocks.sql.optimizer.base.ColumnIdentifier;
import com.starrocks.thrift.TPlanFragment;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

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
        return renewAfterLoad(cached, echoedVersion, null);
    }

    private static ColumnDict renewAfterLoad(ColumnDict cached, long collectedVersion, Long contentIdentity) throws Exception {
        ColumnIdentifier id = new ColumnIdentifier(RENEWAL_TABLE_ID, RENEWAL_COLUMN);
        dictCache().put(id, CompletableFuture.completedFuture(Optional.of(cached)));
        try {
            CacheDictManager.getInstance().updateGlobalDict(
                    RENEWAL_TABLE_ID, RENEWAL_COLUMN, collectedVersion, VISIBLE_TIME, contentIdentity);
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

        Assertions.assertNull(renewAfterLoad(follower, COLLECTED_VERSION, validated.getContentIdentity()));
    }

    @Test
    public void testMatchingIdentityDoesNotBypassCollectedVersionMismatch() throws Exception {
        ColumnDict cached = new ColumnDict(dictOf(false, "A", "B", "C"), COLLECTED_VERSION);
        long identity = cached.getContentIdentity();
        Assertions.assertNull(renewAfterLoad(cached, COLLECTED_VERSION - 500, identity));
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
    public void testLoadDictAddsIdentityWithoutChangingCollectedVersion() {
        ColumnDict first = new ColumnDict(dictOf(false, "A", "B"), COLLECTED_VERSION);
        ColumnDict second = new ColumnDict(dictOf(false, "X", "Y", "Z"), COLLECTED_VERSION + 1);
        List<Pair<Integer, ColumnDict>> dicts = List.of(new Pair<>(5, first), new Pair<>(7, second));
        PlanFragment fragment = new PlanFragment(new PlanFragmentId(0), null, DataPartition.UNPARTITIONED);
        fragment.setLoadGlobalDicts(dicts);
        fragment.setQueryGlobalDicts(dicts);

        TPlanFragment thrift = fragment.toThrift();

        Assertions.assertEquals(5, thrift.getLoad_global_dicts().get(0).getColumnId());
        Assertions.assertEquals(first.getCollectedVersion(), thrift.getLoad_global_dicts().get(0).getVersion());
        Assertions.assertEquals(7, thrift.getLoad_global_dicts().get(1).getColumnId());
        Assertions.assertEquals(second.getCollectedVersion(), thrift.getLoad_global_dicts().get(1).getVersion());
        Assertions.assertTrue(thrift.getLoad_global_dicts().get(0).isSetContent_identity());
        Assertions.assertEquals(first.getContentIdentity(), thrift.getLoad_global_dicts().get(0).getContent_identity());
        Assertions.assertEquals(second.getContentIdentity(), thrift.getLoad_global_dicts().get(1).getContent_identity());
        Assertions.assertFalse(thrift.getQuery_global_dicts().get(0).isSetContent_identity());
        Assertions.assertFalse(thrift.getQuery_global_dicts().get(1).isSetContent_identity());
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
    @Test
    public void testNewFormatNeverFallsBackToCollectedVersion() throws Exception {
        ColumnDict cached = new ColumnDict(dictOf(false, "A", "B"), COLLECTED_VERSION);
        Assertions.assertNull(renewAfterLoad(cached, COLLECTED_VERSION, COLLECTED_VERSION));
    }

    @Test
    public void testZeroContentIdentityRenewsWhenMatching() throws Exception {
        ColumnDict cached = Mockito.spy(new ColumnDict(dictOf(false, "A", "B"), COLLECTED_VERSION));
        Mockito.when(cached.getContentIdentity()).thenReturn(0L);
        Assertions.assertSame(cached, renewAfterLoad(cached, COLLECTED_VERSION, 0L));
        Assertions.assertEquals(VISIBLE_TIME, cached.getVersion());
    }

    @Test
    public void testAlternatingLegacyAndContentRenewalsKeepMatchingDictionary() throws Exception {
        ColumnDict cached = new ColumnDict(dictOf(false, "A", "B"), COLLECTED_VERSION);
        for (int i = 0; i < 8; i++) {
            Assertions.assertSame(cached, renewAfterLoad(cached, COLLECTED_VERSION, null));
            Assertions.assertSame(cached, renewAfterLoad(cached, COLLECTED_VERSION, cached.getContentIdentity()));
        }
    }

}
