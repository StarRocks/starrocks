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

package com.starrocks.alter.reshard.presplit;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.MaterializedIndexMeta;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.OlapTable;
import com.starrocks.common.Config;
import com.starrocks.metric.MetricRepo;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LocalMetastore;
import com.starrocks.sql.analyzer.mv.RowIdStrategy;
import com.starrocks.sql.common.MetaUtils;
import com.starrocks.sql.optimizer.rule.ivm.common.IvmOpUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;

import static com.starrocks.alter.reshard.presplit.PresplitTestSupport.bigintColumn;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Eligibility coverage for {@link MaterializedViewRowIdBoundaries}: every shape it declines must be
 * attributable to {@link SkipReason#MATERIALIZED_VIEW_TARGET} rather than skipped silently, and the one
 * shape it serves must produce a source that reads the auto-increment counter at plan time.
 */
public class MaterializedViewRowIdBoundariesTest {

    private static final long BASE_INDEX_META_ID = 10L;
    private static final long ROLLUP_INDEX_META_ID = 11L;

    /** Estimate large enough that the derived planner's gap headroom never becomes the reason for a skip. */
    private static final Estimates BIG_ENOUGH = new Estimates(1L << 30, 1_000_000L);

    private int savedIdCacheSize;

    @BeforeEach
    public void setUp() {
        savedIdCacheSize = Config.auto_increment_cache_size;
        Config.auto_increment_cache_size = 25;
    }

    @AfterEach
    public void tearDown() {
        Config.auto_increment_cache_size = savedIdCacheSize;
    }

    @Test
    public void testPlainOlapTableIsDeclined() {
        // Sort key deliberately named like the hidden row-id column: the rejection must come from the
        // target not being a materialized view, not from the key's name.
        assertNotDerivable(mock(OlapTable.class), List.of(rowIdColumn()));
    }

    @Test
    public void testNonIncrementalMaterializedViewIsDeclined() {
        // A null strategy means the view has no __ROW_ID__ column at all, so there is no id domain to
        // derive anything from.
        assertNotDerivable(mockMaterializedView(null, /*visibleIndexCount=*/ 1), List.of(rowIdColumn()));
    }

    @Test
    public void testQueryComputedRowIdIsDeclined() {
        // QUERY_COMPUTED row ids encode the view's group-by keys; that domain is not derivable and no
        // boundary source serves it, so it must not fall through to the auto-increment planner.
        assertNotDerivable(mockMaterializedView(RowIdStrategy.QUERY_COMPUTED, /*visibleIndexCount=*/ 1),
                List.of(rowIdColumn()));
    }

    @Test
    public void testSecondVisibleIndexIsDeclined() {
        // The pipeline plans boundaries for every visible index, so a second one would be cut on row-id
        // boundaries that are not its key, or left unsplit.
        assertNotDerivable(mockMaterializedView(RowIdStrategy.AUTO_INCREMENT, /*visibleIndexCount=*/ 2),
                List.of(rowIdColumn()));
    }

    @Test
    public void testSortKeyOtherThanTheRowIdColumnIsDeclined() {
        // The derived cuts are row ids. An index keyed by anything else -- a user column, or the row id
        // plus a trailing column -- would be carved on values that do not belong to its key.
        assertNotDerivable(mockMaterializedView(RowIdStrategy.AUTO_INCREMENT, /*visibleIndexCount=*/ 1),
                List.of(bigintColumn("k")));
        assertNotDerivable(mockMaterializedView(RowIdStrategy.AUTO_INCREMENT, /*visibleIndexCount=*/ 1),
                List.of(rowIdColumn(), bigintColumn("k")));
    }

    @Test
    public void testIncrementalAutoIncrementMaterializedViewIsDerivable() {
        // The one shape the derived tier serves. The check must stay free of side effects so a caller
        // can ask it before resolving a target or consulting the feature flag -- that ordering is what
        // keeps an ordinary view's refresh out of both this feature's skip buckets and its config gate.
        MaterializedView materializedView =
                mockMaterializedView(RowIdStrategy.AUTO_INCREMENT, /*visibleIndexCount=*/ 1);
        boolean savedHasInit = MetricRepo.hasInit;
        MetricRepo.hasInit = true;
        try (MockedStatic<MetaUtils> metaUtils = Mockito.mockStatic(MetaUtils.class)) {
            stubSortKey(metaUtils, materializedView, List.of(rowIdColumn()));
            long baseline = skipCount();

            Assertions.assertTrue(MaterializedViewRowIdBoundaries.isDerivable(materializedView));

            Assertions.assertEquals(baseline, skipCount(), "the check must not record anything");
        } finally {
            MetricRepo.hasInit = savedHasInit;
        }
    }

    @Test
    public void testCounterIsReadWhenBoundariesArePlanned() {
        // The pristine check has to run against the counter as it stands when the split is planned, not
        // as it stood when the source was resolved: a load that starts allocating ids in between
        // invalidates the derivation, and a resolve-time read would carve the span anyway.
        MaterializedView materializedView =
                mockMaterializedView(RowIdStrategy.AUTO_INCREMENT, /*visibleIndexCount=*/ 1);
        LocalMetastore localMetastore = mock(LocalMetastore.class);
        when(localMetastore.getCurrentAutoIncrementIdByTableId(any())).thenReturn(null);

        try (MockedStatic<MetaUtils> metaUtils = Mockito.mockStatic(MetaUtils.class);
                MockedStatic<GlobalStateMgr> globalStateMgr = Mockito.mockStatic(GlobalStateMgr.class)) {
            stubSortKey(metaUtils, materializedView, List.of(rowIdColumn()));
            GlobalStateMgr globalState = mock(GlobalStateMgr.class);
            when(globalState.getLocalMetastore()).thenReturn(localMetastore);
            globalStateMgr.when(GlobalStateMgr::getCurrentState).thenReturn(globalState);

            DerivedBoundarySource source = MaterializedViewRowIdBoundaries.sourceFor(
                    materializedView, BIG_ENOUGH, /*activeComputeNodeCount=*/ 1);

            DerivedBoundarySource.Result pristine = source.plan(rowIdIndexTarget(), /*requestedTabletCount=*/ 4);
            Assertions.assertNull(pristine.skipReason());
            Assertions.assertEquals(3, pristine.boundaries().getBoundaries().size());

            when(localMetastore.getCurrentAutoIncrementIdByTableId(any())).thenReturn(500L);
            Assertions.assertEquals(SkipReason.ROW_ID_SPACE_NOT_PRISTINE,
                    source.plan(rowIdIndexTarget(), /*requestedTabletCount=*/ 4).skipReason(),
                    "an id already handed out between resolve and plan must stop the derivation");
        }
    }

    /**
     * Asserts {@code table} — with {@code sortKey} stubbed as its visible index's range-distribution
     * columns — is not something the derived tier can carve, and that answering so records nothing.
     * That the decline is REPORTED as materialized_view_target is asserted at the hook, which is where
     * the recording now lives, so that an ordinary view's refresh cannot be attributed to a target
     * resolver or to this feature's config gate.
     */
    private static void assertNotDerivable(OlapTable table, List<Column> sortKey) {
        boolean savedHasInit = MetricRepo.hasInit;
        MetricRepo.hasInit = true;
        try (MockedStatic<MetaUtils> metaUtils = Mockito.mockStatic(MetaUtils.class)) {
            stubSortKey(metaUtils, table, sortKey);
            long baseline = skipCount();

            Assertions.assertFalse(MaterializedViewRowIdBoundaries.isDerivable(table));

            Assertions.assertEquals(baseline, skipCount(), "the check must not record anything");
        } finally {
            MetricRepo.hasInit = savedHasInit;
        }
    }

    private static void stubSortKey(MockedStatic<MetaUtils> metaUtils, OlapTable table, List<Column> sortKey) {
        metaUtils.when(() -> MetaUtils.getRangeDistributionColumns(eq(table), anyLong())).thenReturn(sortKey);
    }

    private static long skipCount() {
        String label = SkipReason.MATERIALIZED_VIEW_TARGET.name().toLowerCase();
        return MetricRepo.COUNTER_TABLET_PRE_SPLIT_ELIGIBILITY_SKIPPED.getMetric(label).getValue();
    }

    private static MaterializedView mockMaterializedView(RowIdStrategy rowIdStrategy, int visibleIndexCount) {
        MaterializedView materializedView = mock(MaterializedView.class);
        when(materializedView.getRowIdStrategy()).thenReturn(rowIdStrategy);
        List<MaterializedIndexMeta> visibleIndexMetas = new ArrayList<>(visibleIndexCount);
        long[] metaIds = {BASE_INDEX_META_ID, ROLLUP_INDEX_META_ID};
        for (int i = 0; i < visibleIndexCount; i++) {
            MaterializedIndexMeta indexMeta = mock(MaterializedIndexMeta.class);
            when(indexMeta.getIndexMetaId()).thenReturn(metaIds[i]);
            visibleIndexMetas.add(indexMeta);
        }
        when(materializedView.getVisibleIndexMetas()).thenReturn(visibleIndexMetas);
        return materializedView;
    }

    private static Column rowIdColumn() {
        return bigintColumn(IvmOpUtils.COLUMN_ROW_ID);
    }

    private static IndexPreSplitTarget rowIdIndexTarget() {
        return new IndexPreSplitTarget(BASE_INDEX_META_ID, /*oldTabletId=*/ 100L, List.of(rowIdColumn()));
    }
}
