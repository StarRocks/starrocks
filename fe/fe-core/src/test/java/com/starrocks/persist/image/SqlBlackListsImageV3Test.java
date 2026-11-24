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

import com.starrocks.common.Config;
import com.starrocks.meta.SqlBlackList;
import com.starrocks.meta.SqlDigestBlackList;
import com.starrocks.metric.LongCounterMetric;
import com.starrocks.metric.Metric;
import com.starrocks.metric.MetricRepo;
import com.starrocks.persist.proto.IndexEntryPB;
import com.starrocks.persist.proto.SqlBlackListPB;
import com.starrocks.persist.proto.SqlDigestBlackListPB;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

import static com.starrocks.common.jmockit.Deencapsulation.getField;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class SqlBlackListsImageV3Test {
    @TempDir
    Path tempDir;

    @BeforeEach
    public void setUp() {
        LoadImageTestSupport.global();
    }

    @Test
    public void testPatternRecoveryAndMatchingAgreeWithV2() throws Exception {
        SqlBlackList source = new SqlBlackList();
        source.put(13, Pattern.compile("select .* secret"));
        source.put(2, Pattern.compile("drop\\s+table"));
        source.put(29, Pattern.compile("敏感表"));
        SqlBlackList expected = new SqlBlackList();
        FinalManagersImageTestSupport.v2(source::save, expected::load);
        SqlBlackList restored = restore(source);
        assertRules(expected, restored);
        assertEquals(List.of(2L, 13L, 29L), restored.getBlackLists().stream().map(rule -> rule.id).toList());
        assertEquals(30L, ((AtomicLong) getField(restored, "ids")).get());
        restored.verifying("select public from ordinary_table");
        assertBlocked(restored, "select   a\nfrom secret");
        assertBlocked(restored, "drop table t");
        assertBlocked(restored, "select * from 敏感表");
        restored.delete(13L);
        restored.verifying("select a from secret");
        assertEquals(2, restored.getBlackLists().size());

    }

    private void assertBlocked(SqlBlackList restored, String sql) {
        LongCounterMetric previous = MetricRepo.COUNTER_SQL_BLOCK_HIT_COUNT;
        LongCounterMetric metric = new LongCounterMetric("test_sql_block", Metric.MetricUnit.REQUESTS, "test");
        MetricRepo.COUNTER_SQL_BLOCK_HIT_COUNT = metric;
        try {
            assertThrows(Exception.class, () -> restored.verifying(sql));
            assertEquals(1L, metric.getValue());
        } finally {
            MetricRepo.COUNTER_SQL_BLOCK_HIT_COUNT = previous;
        }
    }

    private SqlBlackList restore(SqlBlackList source) throws Exception {
        Path path = LoadImageTestSupport.save(tempDir, source::save);
        SqlBlackList result = new SqlBlackList();
        List<IndexEntryPB> sections = LoadImageTestSupport.load(path, result.getImageV3Loaders());
        assertEquals(330, sections.get(0).getType());
        assertEquals(source.getBlackLists().size(), sections.get(0).getNumEntries());
        assertEquals(0, sections.get(0).getManagerLength());
        return result;
    }

    private void assertRules(SqlBlackList expected, SqlBlackList actual) {
        assertEquals(expected.getBlackLists().stream().map(rule -> rule.id + ":" + rule.pattern.pattern()).toList(),
                actual.getBlackLists().stream().map(rule -> rule.id + ":" + rule.pattern.pattern()).toList());
    }

    @Test
    public void testDuplicatePatternDoesNotConsumeAnotherId() throws Exception {
        Path path = FinalManagersImageTestSupport.entries(tempDir, SectionId.SQL_BLACKLIST, List.of(
                SqlBlackListPB.newBuilder().setId(2).setPattern("duplicate").build(),
                SqlBlackListPB.newBuilder().setId(100).setPattern("duplicate").build(),
                SqlBlackListPB.newBuilder().setId(4).setPattern("distinct").build()));
        SqlBlackList restored = new SqlBlackList();
        LoadImageTestSupport.load(path, restored.getImageV3Loaders());
        assertEquals(List.of(2L, 4L), restored.getBlackLists().stream().map(rule -> rule.id).toList());
        assertEquals(5L, ((AtomicLong) getField(restored, "ids")).get());

    }

    @Test
    public void testInvalidRegexRefreshesAlreadyLoadedSnapshotAndReleasesLock() throws Exception {
        boolean previous = Config.metadata_ignore_unknown_subtype;
        try {
            for (boolean ignore : List.of(false, true)) {
                Config.metadata_ignore_unknown_subtype = ignore;
                Path path = FinalManagersImageTestSupport.entries(tempDir,
                        SectionId.SQL_BLACKLIST, List.of(
                                SqlBlackListPB.newBuilder().setId(9).setPattern("valid").build(),
                                SqlBlackListPB.newBuilder().setId(10).setPattern("[").build(),
                                SqlBlackListPB.newBuilder().setId(11).setPattern("unread").build()));
                SqlBlackList restored = new SqlBlackList();
                assertThrows(Exception.class, () -> LoadImageTestSupport.load(path, restored.getImageV3Loaders()));
                assertEquals(List.of(9L), restored.getBlackLists().stream().map(rule -> rule.id).toList());
                assertBlocked(restored, "valid");
                restored.put(12, Pattern.compile("after failure"));
                assertEquals(2, restored.getBlackLists().size());
            }
        } finally {
            Config.metadata_ignore_unknown_subtype = previous;
        }
    }

    @Test
    public void testPatternFlagsFollowV2PatternStringContract() throws Exception {
        SqlBlackList source = new SqlBlackList();
        source.put(1, Pattern.compile("abc", Pattern.CASE_INSENSITIVE));
        source.put(2, Pattern.compile("(?i)inline"));
        SqlBlackList expected = new SqlBlackList();
        FinalManagersImageTestSupport.v2(source::save, expected::load);
        SqlBlackList restored = restore(source);
        assertRules(expected, restored);
        restored.verifying("ABC");
        assertBlocked(restored, "INLINE");

    }

    @Test
    public void testLargePatternListAndEmptyImageKeepV2MergeBehavior() throws Exception {
        SqlBlackList source = new SqlBlackList();
        for (int i = 0; i < 1024; i++) {
            source.put(i, Pattern.compile("^query_" + i + "$"));
        }
        SqlBlackList restored = restore(source);
        assertRules(source, restored);
        Path empty = LoadImageTestSupport.save(tempDir, new SqlBlackList()::save);
        LoadImageTestSupport.load(empty, restored.getImageV3Loaders());
        assertRules(source, restored);
        restored.delete(List.of(0L, 1023L));
        assertEquals(1022, restored.getBlackLists().size());

    }

    @Test
    public void testDigestRecoveryMatchingAndDeletion() throws Exception {
        SqlDigestBlackList source = new SqlDigestBlackList();
        source.put("9c9f13");
        source.put("digest-中文");
        source.put("");
        SqlDigestBlackList expected = new SqlDigestBlackList();
        FinalManagersImageTestSupport.v2(source::save, expected::load);
        LongCounterMetric previous = MetricRepo.COUNTER_SQL_BLOCK_HIT_COUNT;
        MetricRepo.COUNTER_SQL_BLOCK_HIT_COUNT = new LongCounterMetric("test", Metric.MetricUnit.REQUESTS, "test");
        try {
            Path path = LoadImageTestSupport.save(tempDir, source::save);
            SqlDigestBlackList restored = new SqlDigestBlackList();
            List<IndexEntryPB> sections = LoadImageTestSupport.load(path, restored.getImageV3Loaders());
            assertEquals(370, sections.get(0).getType());
            assertEquals(3, sections.get(0).getNumEntries());
            assertEquals(0, sections.get(0).getManagerLength());
            assertEquals(expected.getDigests(), restored.getDigests());
            restored.verifying("unblocked");
            long before = MetricRepo.COUNTER_SQL_BLOCK_HIT_COUNT.getValue();
            assertThrows(Exception.class, () -> restored.verifying("9c9f13"));
            assertEquals(before + 1, MetricRepo.COUNTER_SQL_BLOCK_HIT_COUNT.getValue());
            restored.deleteAll(List.of("9c9f13", "missing"));
            restored.verifying("9c9f13");
            assertEquals(Set.of("digest-中文", ""), restored.getDigests());

        } finally {
            MetricRepo.COUNTER_SQL_BLOCK_HIT_COUNT = previous;
        }
    }

    @Test
    public void testDuplicateDigestAndReloadIntoNonEmptySet() throws Exception {
        Path path = FinalManagersImageTestSupport.entries(tempDir, SectionId.SQL_DIGEST_BLACKLIST,
                List.of(SqlDigestBlackListPB.newBuilder().setDigest("same").build(),
                        SqlDigestBlackListPB.newBuilder().setDigest("same").build(),
                        SqlDigestBlackListPB.newBuilder().setDigest("next").build()));
        SqlDigestBlackList restored = new SqlDigestBlackList();
        restored.put("existing");
        LoadImageTestSupport.load(path, restored.getImageV3Loaders());
        assertEquals(Set.of("existing", "same", "next"), restored.getDigests());

    }

    @Test
    public void testLargeDigestListAndEmptyImage() throws Exception {
        SqlDigestBlackList source = new SqlDigestBlackList();
        for (int i = 0; i < 1024; i++) {
            source.put("digest-" + i);
        }
        Path path = LoadImageTestSupport.save(tempDir, source::save);
        SqlDigestBlackList restored = new SqlDigestBlackList();
        List<IndexEntryPB> sections = LoadImageTestSupport.load(path, restored.getImageV3Loaders());
        assertEquals(1024, sections.get(0).getNumEntries());
        assertEquals(source.getDigests(), restored.getDigests());
        Path empty = LoadImageTestSupport.save(tempDir, new SqlDigestBlackList()::save);
        LoadImageTestSupport.load(empty, restored.getImageV3Loaders());
        assertEquals(source.getDigests(), restored.getDigests());

    }
}
