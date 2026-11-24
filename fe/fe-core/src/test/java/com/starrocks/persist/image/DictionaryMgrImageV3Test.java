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

package com.starrocks.persist.image;

import com.starrocks.catalog.Dictionary;
import com.starrocks.catalog.Dictionary.DictionaryState;
import com.starrocks.catalog.DictionaryMgr;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.persist.metablock.SRMetaBlockReaderV2;
import com.starrocks.persist.proto.DictionaryMgrPB;
import com.starrocks.persist.proto.DictionaryNamePB;
import com.starrocks.persist.proto.DictionaryPB;
import com.starrocks.persist.proto.IndexEntryPB;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.starrocks.common.jmockit.Deencapsulation.getField;
import static com.starrocks.common.jmockit.Deencapsulation.setField;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DictionaryMgrImageV3Test {
    @TempDir
    Path tempDir;

    @BeforeEach
    public void setUp() {
        LoadImageTestSupport.global();
    }

    static Dictionary dictionary(long id, DictionaryState state) {
        Dictionary dictionary = new Dictionary(id, "dictionary-" + id, "source_table", "catalog", "db",
                new ArrayList<>(List.of("k1", "k2")), new ArrayList<>(List.of("v1", "v2")), new HashMap<>());
        dictionary.setNextSchedulableTime(123456);
        setField(dictionary, "lastSuccessRefreshTime", 1000L);
        setField(dictionary, "lastSuccessFinishedTime", 2000L);
        setField(dictionary, "state", state);
        setField(dictionary, "stateBeforeRefresh", DictionaryState.FINISHED);
        setField(dictionary, "runtimeErrMsg", "previous failure");
        setField(dictionary, "lastSuccessVersion", 456L);
        setField(dictionary, "memoryLimit", 4294967296L);
        setField(dictionary, "refreshInterval", 0);
        setField(dictionary, "readLatest", true);
        setField(dictionary, "ignoreFailedRefresh", true);
        return dictionary;
    }

    static void add(DictionaryMgr manager, Dictionary dictionary) {
        manager.getDictionariesMapById().put(dictionary.getDictionaryId(), dictionary);
        manager.getDictionariesIdMapByName().put(dictionary.getDictionaryName(), dictionary.getDictionaryId());
    }

    private DictionaryMgr v2(DictionaryMgr source) throws Exception {
        UtFrameUtils.PseudoImage image = new UtFrameUtils.PseudoImage();
        source.save(image.getImageWriter());
        DictionaryMgr result = new DictionaryMgr();
        SRMetaBlockReaderV2 reader = new SRMetaBlockReaderV2(image.getJsonReader());
        result.load(reader);
        reader.close();
        return result;
    }

    private DictionaryMgr v3(DictionaryMgr source) throws Exception {
        Path path = LoadImageTestSupport.save(tempDir, source::save);
        DictionaryMgr result = new DictionaryMgr();
        List<IndexEntryPB> sections = LoadImageTestSupport.load(path, result.getImageV3Loaders());
        assertEquals(List.of(280, 281), sections.stream().map(IndexEntryPB::getType).toList());
        assertTrue(sections.get(0).getManagerLength() > 0);
        assertEquals(0, sections.get(1).getManagerLength());
        return result;
    }

    @Test
    public void testAllFieldsAndStatesRoundTripBeforeManagerReset() throws Exception {
        for (DictionaryState state : DictionaryState.values()) {
            Dictionary source = dictionary(1, state);
            Dictionary restored = Dictionary.fromProto(DictionaryPB.parseFrom(source.toProto().toByteArray()));
            GsonEquivalence.assertEquivalent(source, restored);
            assertEquals(source.buildQuery(), restored.buildQuery());
            assertEquals(123456, restored.getNextSchedulableTime());
            assertEquals(4294967296L, restored.getMemoryLimit());
            assertTrue(restored.getReadLatest());
            assertTrue(restored.getIgnoreFailedRefresh());
            TaskImageTestSupport.assertAllPersistedFieldsPresent(source);
        }
    }

    @Test
    public void testDefaultAndAbsentFields() {
        Dictionary source = new Dictionary();
        GsonEquivalence.assertEquivalent(source, Dictionary.fromProto(source.toProto()));
        Dictionary restored = Dictionary.fromProto(DictionaryPB.getDefaultInstance());
        assertEquals(2147483648L, restored.getMemoryLimit());
        assertEquals(Long.MAX_VALUE, restored.getNextSchedulableTime());
        assertEquals(0, restored.getRefreshInterval());
        assertFalse(restored.getReadLatest());
        assertFalse(restored.getIgnoreFailedRefresh());
        assertEquals(DictionaryState.UNINITIALIZED, getField(restored, "state"));
    }

    @Test
    public void testListsKeepOrderAndCanBeChangedAfterLoad() {
        Dictionary source = dictionary(1, DictionaryState.FINISHED);
        Dictionary restored = Dictionary.fromProto(source.toProto());
        List<String> keys = getField(restored, "dictionaryKeys");
        List<String> values = getField(restored, "dictionaryValues");
        assertEquals(List.of("k1", "k2"), keys);
        assertEquals(List.of("v1", "v2"), values);
        keys.add("k3");
        values.remove("v1");
        assertEquals(List.of("k1", "k2"), getField(source, "dictionaryKeys"));
        assertEquals(List.of("v1", "v2"), getField(source, "dictionaryValues"));
    }

    @Test
    public void testCreationOnlyPropertiesAreNotPersisted() {
        Dictionary source = dictionary(1, DictionaryState.CANCELLED);
        setField(source, "warmUp", false);
        setField(source, "properties", Map.of("dictionary_warm_up", "false", "dictionary_memory_limit", "4G"));
        Dictionary expected = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(source), Dictionary.class);
        Dictionary restored = Dictionary.fromProto(source.toProto());
        GsonEquivalence.assertEquivalent(expected, restored);
        assertTrue((boolean) getField(restored, "warmUp"));
        Map<String, String> properties = getField(restored, "properties");
        assertTrue(properties.isEmpty());
    }

    @Test
    public void testManagerResetsEveryRuntimeStateAsInV2() throws Exception {
        DictionaryMgr source = new DictionaryMgr();
        int id = 1;
        for (DictionaryState state : DictionaryState.values()) {
            add(source, dictionary(id++, state));
        }
        DictionaryMgr expected = v2(source);
        DictionaryMgr restored = v3(source);
        GsonEquivalence.assertEquivalent(expected, restored);
        for (Dictionary dictionary : restored.getDictionariesMapById().values()) {
            assertEquals(DictionaryState.UNINITIALIZED, getField(dictionary, "state"));
            assertNull(getField(dictionary, "stateBeforeRefresh"));
            assertEquals("", getField(dictionary, "runtimeErrMsg"));
            assertEquals(0, (long) getField(dictionary, "lastSuccessRefreshTime"));
            assertEquals(0, (long) getField(dictionary, "lastSuccessFinishedTime"));
            assertEquals(0, (long) getField(dictionary, "lastSuccessVersion"));
            assertEquals(Long.MAX_VALUE, dictionary.getNextSchedulableTime());
        }
        assertTrue(restored.getUnfinishedRefreshTasks().isEmpty());
        Set<Long> running = getField(restored, "runningRefreshTasks");
        assertTrue(running.isEmpty());

    }

    @Test
    public void testRefreshScheduleRecomputedAtLoad() throws Exception {
        DictionaryMgr source = new DictionaryMgr();
        Dictionary dictionary = dictionary(1, DictionaryState.REFRESHING);
        setField(dictionary, "refreshInterval", 30000);
        add(source, dictionary);
        long start = System.currentTimeMillis();
        DictionaryMgr restored = v3(source);
        long end = System.currentTimeMillis();
        long schedule = restored.getDictionariesMapById().get(1L).getNextSchedulableTime();
        assertTrue(schedule >= start + 30000);
        assertTrue(schedule <= end + 30000);

    }

    @Test
    public void testCountersAndIndependentNameIndexSurvive() throws Exception {
        DictionaryMgr source = new DictionaryMgr();
        add(source, dictionary(1, DictionaryState.COMMITTING));
        setField(source, "nextTxnId", 100001L);
        setField(source, "nextDictionaryId", 50001L);
        source.getDictionariesIdMapByName().put("alias", 1L);
        source.getDictionariesIdMapByName().put("orphan", 999L);
        DictionaryMgr expected = v2(source);
        DictionaryMgr restored = v3(source);
        assertEquals(100001L, restored.getNextTxnId());
        assertEquals(50001L, restored.getNextDictionaryId());
        assertEquals(expected.getDictionariesIdMapByName(), restored.getDictionariesIdMapByName());
        assertEquals(1, restored.getDictionariesMapById().size());
        assertEquals(3, restored.getDictionariesIdMapByName().size());

    }

    @Test
    public void testUnknownRuntimeStatesStillResetAtManagerBoundary() throws Exception {
        Path path = LoadImageTestSupport.save(tempDir, writer -> {
            try (SectionWriter section = writer.beginSection(SectionId.DICTIONARY_MGR)) {
                section.writeManager(DictionaryMgrPB.newBuilder().setNextTxnId(2).setNextDictionaryId(2).build());
                section.writeEntry(dictionary(1, DictionaryState.REFRESHING).toProto().toBuilder()
                        .setStateValue(999).setStateBeforeRefreshValue(998).build());
            }
            try (SectionWriter section = writer.beginSection(SectionId.DICTIONARY_NAME)) {
                section.writeEntry(DictionaryNamePB.newBuilder().setName("dictionary-1").setDictionaryId(1).build());
            }
        });
        DictionaryMgr restored = new DictionaryMgr();
        LoadImageTestSupport.load(path, restored.getImageV3Loaders());
        assertEquals(DictionaryState.UNINITIALIZED, getField(restored.getDictionariesMapById().get(1L), "state"));
        assertNull(getField(restored.getDictionariesMapById().get(1L), "stateBeforeRefresh"));

    }

    @Test
    public void testObjectsAndNamesHaveSeparateEntries() throws Exception {
        DictionaryMgr source = new DictionaryMgr();
        for (int i = 1; i <= 1024; i++) {
            add(source, dictionary(i, DictionaryState.FINISHED));
        }
        Path path = LoadImageTestSupport.save(tempDir, source::save);
        DictionaryMgr restored = new DictionaryMgr();
        List<IndexEntryPB> sections = LoadImageTestSupport.load(path, restored.getImageV3Loaders());
        assertEquals(List.of(1024L, 1024L), sections.stream().map(IndexEntryPB::getNumEntries).toList());
        assertEquals(1024, restored.getDictionariesMapById().size());
        assertEquals(source.getDictionariesIdMapByName(), restored.getDictionariesIdMapByName());

    }

    @Test
    public void testEmptyManagerReloadReplacesOldMapsAndCounters() throws Exception {
        Path path = LoadImageTestSupport.save(tempDir, new DictionaryMgr()::save);
        DictionaryMgr restored = new DictionaryMgr();
        add(restored, dictionary(1, DictionaryState.FINISHED));
        setField(restored, "nextDictionaryId", 900L);
        LoadImageTestSupport.load(path, restored.getImageV3Loaders());
        assertTrue(restored.getDictionariesMapById().isEmpty());
        assertTrue(restored.getDictionariesIdMapByName().isEmpty());
        assertEquals(1L, restored.getNextDictionaryId());
        assertEquals(1L, restored.getNextTxnId());

    }

    @Test
    public void testManualRefreshAfterRestoreKeepsAutomaticSchedulingDisabled() throws Exception {
        DictionaryMgr source = new DictionaryMgr();
        add(source, dictionary(1, DictionaryState.REFRESHING));
        Dictionary restored = v3(source).getDictionariesMapById().get(1L);
        assertEquals(Long.MAX_VALUE, restored.getNextSchedulableTime());
        restored.setRefreshing(3000L);
        assertEquals(Long.MAX_VALUE, restored.getNextSchedulableTime());
        assertEquals(DictionaryState.REFRESHING, getField(restored, "state"));
        restored.setCommitting();
        restored.setFinished(4000L, 100L);
        assertEquals(DictionaryState.FINISHED, getField(restored, "state"));
        assertEquals(100L, (long) getField(restored, "lastSuccessVersion"));
        assertEquals(4000L, (long) getField(restored, "lastSuccessFinishedTime"));
        assertEquals(Long.MAX_VALUE, restored.getNextSchedulableTime());

    }

    @Test
    public void testPeriodicRefreshAfterRestoreUsesPersistedInterval() throws Exception {
        DictionaryMgr source = new DictionaryMgr();
        Dictionary dictionary = dictionary(1, DictionaryState.CANCELLED);
        setField(dictionary, "refreshInterval", 15000);
        add(source, dictionary);
        Dictionary restored = v3(source).getDictionariesMapById().get(1L);
        restored.setRefreshing(50000L);
        assertEquals(65000L, restored.getNextSchedulableTime());
        assertEquals(DictionaryState.UNINITIALIZED, getField(restored, "stateBeforeRefresh"));
        restored.resetStateBeforeRefresh();
        assertEquals(DictionaryState.UNINITIALIZED, restored.getState());
        restored.setRefreshing(60000L);
        restored.setCancelled();
        assertEquals(DictionaryState.CANCELLED, restored.getState());
        assertNull(getField(restored, "stateBeforeRefresh"));
        assertEquals(75000L, restored.getNextSchedulableTime());
        assertEquals(15000, restored.getRefreshInterval());

    }
}
