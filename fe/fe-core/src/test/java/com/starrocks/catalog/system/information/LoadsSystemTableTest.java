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

package com.starrocks.catalog.system.information;

import com.google.common.collect.Lists;
import com.starrocks.catalog.Database;
import com.starrocks.common.Config;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.load.loadv2.LoadJob;
import com.starrocks.load.loadv2.LoadMgr;
import com.starrocks.load.streamload.AbstractStreamLoadTask;
import com.starrocks.load.streamload.StreamLoadMgr;
import com.starrocks.load.streamload.StreamLoadMultiStmtTask;
import com.starrocks.load.streamload.StreamLoadTask;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LocalMetastore;
import com.starrocks.thrift.TGetLoadsParams;
import com.starrocks.thrift.TGetLoadsResult;
import com.starrocks.thrift.TLoadInfo;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for the predicate-pushdown filter parsing in
 * {@link LoadsSystemTable.LoadRequestFilter}.
 *
 * <p>Historical bug: BE pushed a session-zone wall-clock string (e.g.
 * "2026-05-15 02:45:08") into TGetLoadsParams; FE parsed it via the
 * hardcoded UTC+8 zone in TimeUtils, silently shifting the filter bound on
 * any non-Asia/Shanghai session.
 *
 * <p>Current contract: BE attaches the UTC epoch ms alongside the legacy
 * string field. FE reads the ms field first; only falls back to the string
 * when the ms field is absent (old BE during a rolling upgrade). The legacy
 * string path preserves the pre-fix behavior so we don't introduce a third
 * incompatible interpretation mid-flight.
 */
public class LoadsSystemTableTest {

    private static long epochMillis(String wallClock, ZoneId zone) {
        return LocalDateTime.parse(wallClock.replace(' ', 'T'))
                .atZone(zone)
                .toInstant()
                .toEpochMilli();
    }

    // ---------------------------------------------------------------------
    // pickTime: ms-field-preferred / legacy-string fallback
    // ---------------------------------------------------------------------

    @Test
    public void testPickTime() {
        long edtMs = epochMillis("2026-05-15 02:45:08", ZoneId.of("America/New_York"));

        // ms field set: wins over string. The string field's UTC+8 interpretation
        // is precisely the bug we're avoiding.
        Long picked = LoadsSystemTable.LoadRequestFilter.pickTime(edtMs, "2026-05-15 02:45:08");
        assertNotNull(picked);
        assertEquals(edtMs, picked.longValue());

        // No ms: fall back to legacy UTC+8 parse. Pin that behavior so a future
        // TimeUtils refactor doesn't silently change the rolling-upgrade fallback.
        picked = LoadsSystemTable.LoadRequestFilter.pickTime(null, "2026-05-15 02:45:08");
        assertNotNull(picked);
        assertEquals(TimeUtils.timeStringToLong("2026-05-15 02:45:08"), picked.longValue());

        // Neither set: null (no bound).
        assertNull(LoadsSystemTable.LoadRequestFilter.pickTime(null, null));

        // Unparseable legacy string must yield "no bound" rather than -1 leaking
        // into matchTimeRange and being mis-treated as an upper bound.
        assertNull(LoadsSystemTable.LoadRequestFilter.pickTime(null, "not-a-datetime"));
    }

    // ---------------------------------------------------------------------
    // LoadRequestFilter.from(TGetLoadsParams): primary epoch-ms path
    // ---------------------------------------------------------------------

    /**
     * All six bounds flow through the same pickTime path - pin them all so a
     * future refactor can't quietly drop one and reintroduce the bug for that
     * specific field.
     */
    @Test
    public void testFrom_allSixBoundsFromMs() {
        TGetLoadsParams req = new TGetLoadsParams();
        req.setLoad_start_time_from_ms(1_000L);
        req.setLoad_start_time_to_ms(2_000L);
        req.setLoad_finish_time_from_ms(3_000L);
        req.setLoad_finish_time_to_ms(4_000L);
        req.setCreate_time_from_ms(5_000L);
        req.setCreate_time_to_ms(6_000L);

        LoadsSystemTable.LoadRequestFilter filter = LoadsSystemTable.LoadRequestFilter.from(req);

        assertEquals(1_000L, filter.loadStartTimeFrom.longValue());
        assertEquals(2_000L, filter.loadStartTimeTo.longValue());
        assertEquals(3_000L, filter.loadFinishTimeFrom.longValue());
        assertEquals(4_000L, filter.loadFinishTimeTo.longValue());
        assertEquals(5_000L, filter.createTimeFrom.longValue());
        assertEquals(6_000L, filter.createTimeTo.longValue());
    }

    /**
     * Old BE (no ms field set, only legacy string). FE must fall back to the
     * legacy UTC+8 parse - matching the historical behavior exactly, so a
     * mixed-version cluster doesn't see a third interpretation of the same
     * wall-clock literal.
     */
    @Test
    public void testFrom_legacyStringFallback() {
        TGetLoadsParams req = new TGetLoadsParams();
        req.setLoad_finish_time_to("2026-05-15 02:45:08");
        // intentionally no setLoad_finish_time_to_ms(...)

        LoadsSystemTable.LoadRequestFilter filter = LoadsSystemTable.LoadRequestFilter.from(req);

        long expectedLegacy = TimeUtils.timeStringToLong("2026-05-15 02:45:08");
        long sameStringAsEdt = epochMillis("2026-05-15 02:45:08", ZoneId.of("America/New_York"));

        assertNotNull(filter.loadFinishTimeTo);
        assertEquals(expectedLegacy, filter.loadFinishTimeTo.longValue());
        // And it differs from the (correct) EDT interpretation by exactly 12 h -
        // proof that the legacy path remains UTC+8 and is precisely what an old
        // BE would have meant. New BEs that care about correctness must send ms.
        assertEquals(12L * 3600 * 1000, sameStringAsEdt - filter.loadFinishTimeTo);
    }

    /**
     * Mixed signals: ms field present takes priority even if the legacy string
     * would have produced a different number. Otherwise a partially-upgraded
     * BE could pin a stale, wrong value through the string field.
     */
    @Test
    public void testFrom_msTakesPrecedenceOverString() {
        long edtMs = epochMillis("2026-05-15 02:45:08", ZoneId.of("America/New_York"));

        TGetLoadsParams req = new TGetLoadsParams();
        req.setLoad_finish_time_to_ms(edtMs);
        req.setLoad_finish_time_to("1999-01-01 00:00:00"); // deliberately stale

        LoadsSystemTable.LoadRequestFilter filter = LoadsSystemTable.LoadRequestFilter.from(req);

        assertEquals(edtMs, filter.loadFinishTimeTo.longValue());
    }

    @Test
    public void testFrom_emptyRequestLeavesAllBoundsNull() {
        LoadsSystemTable.LoadRequestFilter filter = LoadsSystemTable.LoadRequestFilter.from(new TGetLoadsParams());
        assertNull(filter.loadStartTimeFrom);
        assertNull(filter.loadStartTimeTo);
        assertNull(filter.loadFinishTimeFrom);
        assertNull(filter.loadFinishTimeTo);
        assertNull(filter.createTimeFrom);
        assertNull(filter.createTimeTo);
    }

    // =====================================================================
    // Black-box tests on LoadsSystemTable.query(...).
    // ---------------------------------------------------------------------
    // Mock LoadMgr / StreamLoadMgr so that we can pin job finish times
    // deterministically, then check that the request's _ms field decides
    // which jobs survive the filter and that toThrift() output flows through
    // to the result.
    // =====================================================================

    private static TLoadInfo loadInfoWithLabel(String label) {
        TLoadInfo info = new TLoadInfo();
        info.setLabel(label);
        return info;
    }

    private static List<String> labels(TGetLoadsResult result) {
        return result.getLoads().stream().map(TLoadInfo::getLabel).collect(Collectors.toList());
    }

    /**
     * Three load jobs + two stream load tasks. Finish-times are anchored to
     * an arbitrary {@code anchorMs} so the tests below can pick a bound
     * relative to it and produce a deterministic answer:
     *   loadJobBefore:      anchorMs - 15 min
     *   loadJobAfter:       anchorMs +  5 min
     *   loadJobUnfinished:  -1     (matchTimeRange short-circuits true)
     *   streamTaskBefore:   anchorMs - 30 min
     *   streamTaskAfter:    anchorMs + 10 min
     */
    private static long wireMocks(GlobalStateMgr globalStateMgr,
                                  LoadMgr loadMgr,
                                  StreamLoadMgr streamLoadMgr,
                                  LoadJob loadJobBefore,
                                  LoadJob loadJobAfter,
                                  LoadJob loadJobUnfinished,
                                  AbstractStreamLoadTask streamTaskBefore,
                                  AbstractStreamLoadTask streamTaskAfter) {
        // Anchor is "now-ish" so finish times are real ms values that exercise
        // matchTimeRange's positive-value branch (not the value<0 shortcut).
        long anchorMs = epochMillis("2026-05-15 02:45:08", ZoneId.of("America/New_York"));
        long jobBeforeMs = anchorMs - 15L * 60 * 1000;
        long jobAfterMs = anchorMs + 5L * 60 * 1000;
        long streamBeforeMs = anchorMs - 30L * 60 * 1000;
        long streamAfterMs = anchorMs + 10L * 60 * 1000;

        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                minTimes = 0;
                result = globalStateMgr;

                globalStateMgr.getLoadMgr();
                minTimes = 0;
                result = loadMgr;

                globalStateMgr.getStreamLoadMgr();
                minTimes = 0;
                result = streamLoadMgr;

                loadMgr.getLoadJobs(null);
                minTimes = 0;
                result = Lists.newArrayList(loadJobBefore, loadJobAfter, loadJobUnfinished);

                streamLoadMgr.getTaskByName(null);
                minTimes = 0;
                result = Lists.newArrayList(streamTaskBefore, streamTaskAfter);

                loadJobBefore.getDbId();
                minTimes = 0;
                result = -1L;
                loadJobBefore.getLoadFinishTimeMs();
                minTimes = 0;
                result = jobBeforeMs;
                loadJobBefore.getLoadStartTimeMs();
                minTimes = 0;
                result = null;
                loadJobBefore.getCreateTimeMs();
                minTimes = 0;
                result = null;
                loadJobBefore.toThrift();
                minTimes = 0;
                result = loadInfoWithLabel("loadJobBefore");

                loadJobAfter.getDbId();
                minTimes = 0;
                result = -1L;
                loadJobAfter.getLoadFinishTimeMs();
                minTimes = 0;
                result = jobAfterMs;
                loadJobAfter.getLoadStartTimeMs();
                minTimes = 0;
                result = null;
                loadJobAfter.getCreateTimeMs();
                minTimes = 0;
                result = null;
                loadJobAfter.toThrift();
                minTimes = 0;
                result = loadInfoWithLabel("loadJobAfter");

                loadJobUnfinished.getDbId();
                minTimes = 0;
                result = -1L;
                loadJobUnfinished.getLoadFinishTimeMs();
                minTimes = 0;
                result = -1L;
                loadJobUnfinished.getLoadStartTimeMs();
                minTimes = 0;
                result = null;
                loadJobUnfinished.getCreateTimeMs();
                minTimes = 0;
                result = null;
                loadJobUnfinished.toThrift();
                minTimes = 0;
                result = loadInfoWithLabel("loadJobUnfinished");

                streamTaskBefore.getDbId();
                minTimes = 0;
                result = -1L;
                streamTaskBefore.getLoadFinishTimeMs();
                minTimes = 0;
                result = streamBeforeMs;
                streamTaskBefore.getLoadStartTimeMs();
                minTimes = 0;
                result = null;
                streamTaskBefore.getCreateTimeMs();
                minTimes = 0;
                result = null;
                streamTaskBefore.toThrift();
                minTimes = 0;
                result = Lists.newArrayList(loadInfoWithLabel("streamTaskBefore"));

                streamTaskAfter.getDbId();
                minTimes = 0;
                result = -1L;
                streamTaskAfter.getLoadFinishTimeMs();
                minTimes = 0;
                result = streamAfterMs;
                streamTaskAfter.getLoadStartTimeMs();
                minTimes = 0;
                result = null;
                streamTaskAfter.getCreateTimeMs();
                minTimes = 0;
                result = null;
                streamTaskAfter.toThrift();
                minTimes = 0;
                result = Lists.newArrayList(loadInfoWithLabel("streamTaskAfter"));
            }
        };
        return anchorMs;
    }

    /** With no time predicate, every mocked job/task flows through. */
    @Test
    public void testQuery_noFilterReturnsEverything(
            @Mocked GlobalStateMgr globalStateMgr,
            @Mocked LoadMgr loadMgr,
            @Mocked StreamLoadMgr streamLoadMgr,
            @Mocked LoadJob loadJobBefore,
            @Mocked LoadJob loadJobAfter,
            @Mocked LoadJob loadJobUnfinished,
            @Mocked AbstractStreamLoadTask streamTaskBefore,
            @Mocked AbstractStreamLoadTask streamTaskAfter) {
        wireMocks(globalStateMgr, loadMgr, streamLoadMgr,
                loadJobBefore, loadJobAfter, loadJobUnfinished,
                streamTaskBefore, streamTaskAfter);

        TGetLoadsResult result = LoadsSystemTable.query(new TGetLoadsParams());

        assertEquals(5, result.getLoads().size());
        List<String> got = labels(result);
        assertTrue(got.contains("loadJobBefore"));
        assertTrue(got.contains("loadJobAfter"));
        assertTrue(got.contains("loadJobUnfinished"));
        assertTrue(got.contains("streamTaskBefore"));
        assertTrue(got.contains("streamTaskAfter"));
    }

    /**
     * Request carries the bound as UTC epoch ms (the new contract).
     * Only Before-jobs and the unfinished load job survive.
     */
    @Test
    public void testQuery_filtersByFinishTimeToMs(
            @Mocked GlobalStateMgr globalStateMgr,
            @Mocked LoadMgr loadMgr,
            @Mocked StreamLoadMgr streamLoadMgr,
            @Mocked LoadJob loadJobBefore,
            @Mocked LoadJob loadJobAfter,
            @Mocked LoadJob loadJobUnfinished,
            @Mocked AbstractStreamLoadTask streamTaskBefore,
            @Mocked AbstractStreamLoadTask streamTaskAfter) {
        long anchorMs = wireMocks(globalStateMgr, loadMgr, streamLoadMgr,
                loadJobBefore, loadJobAfter, loadJobUnfinished,
                streamTaskBefore, streamTaskAfter);

        TGetLoadsParams req = new TGetLoadsParams();
        req.setLoad_finish_time_to_ms(anchorMs);

        TGetLoadsResult result = LoadsSystemTable.query(req);

        List<String> got = labels(result);
        assertEquals(3, got.size(),
                "Before-jobs and the unfinished load job must clear an at-anchor bound");
        assertTrue(got.contains("loadJobBefore"));
        assertTrue(got.contains("loadJobUnfinished"));
        assertTrue(got.contains("streamTaskBefore"));
        assertTrue(!got.contains("loadJobAfter"));
        assertTrue(!got.contains("streamTaskAfter"));
    }

    /**
     * Pre-fix scenario, expressed end-to-end: an old BE (no _ms field) sends
     * the wall-clock literal "2026-05-15 02:45:08" with the session in EDT.
     * FE falls back to UTC+8 parse for the bound (~12 h earlier in absolute
     * time than EDT) and as a result drops every finished job - only the
     * unfinished load job survives. The new BE path (testQuery_filtersByFinishTimeToMs)
     * does not have this property; this test pins the legacy fallback as the
     * deterministic-but-buggy behavior we deliberately preserve during
     * rolling upgrade.
     */
    @Test
    public void testQuery_legacyStringPathPreservesOldSemantics(
            @Mocked GlobalStateMgr globalStateMgr,
            @Mocked LoadMgr loadMgr,
            @Mocked StreamLoadMgr streamLoadMgr,
            @Mocked LoadJob loadJobBefore,
            @Mocked LoadJob loadJobAfter,
            @Mocked LoadJob loadJobUnfinished,
            @Mocked AbstractStreamLoadTask streamTaskBefore,
            @Mocked AbstractStreamLoadTask streamTaskAfter) {
        wireMocks(globalStateMgr, loadMgr, streamLoadMgr,
                loadJobBefore, loadJobAfter, loadJobUnfinished,
                streamTaskBefore, streamTaskAfter);

        TGetLoadsParams req = new TGetLoadsParams();
        // Only legacy string set; the EDT-session user typed this wall-clock.
        req.setLoad_finish_time_to("2026-05-15 02:45:08");

        TGetLoadsResult result = LoadsSystemTable.query(req);

        List<String> got = labels(result);
        // FE parses the literal as UTC+8 = absolute time ~12 h before the EDT
        // jobs finished -> every finished job is excluded; only the unfinished
        // load job has a value<0 finish ms and short-circuits to "match".
        assertEquals(1, got.size());
        assertTrue(got.contains("loadJobUnfinished"));
    }

    /**
     * Ms-precision contract: the BE pushdown literal carries sub-second from
     * the predicate (here zero - `2026-05-15 02:45:08` has usec=0), and FE
     * compares the full-ms job timestamp directly. A job at the same rendered
     * second but with a non-zero ms remainder is strictly greater than the
     * second-aligned `<=` upper bound and must be dropped by the FE prefilter,
     * matching how BE's post-filter on the ms-precision materialized column
     * would evaluate `08.789 <= '08'`.
     */
    @Test
    public void testQuery_msPrecisionDropsJobsAfterRenderedSecond(
            @Mocked GlobalStateMgr globalStateMgr,
            @Mocked LoadMgr loadMgr,
            @Mocked StreamLoadMgr streamLoadMgr,
            @Mocked LoadJob jobAtBoundary) {
        long anchorMs = epochMillis("2026-05-15 02:45:08", ZoneId.of("America/New_York"));
        long jobMsWithRemainder = anchorMs + 789; // strictly later than the second-aligned bound

        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                minTimes = 0;
                result = globalStateMgr;
                globalStateMgr.getLoadMgr();
                minTimes = 0;
                result = loadMgr;
                globalStateMgr.getStreamLoadMgr();
                minTimes = 0;
                result = streamLoadMgr;
                loadMgr.getLoadJobs(null);
                minTimes = 0;
                result = Lists.newArrayList(jobAtBoundary);
                streamLoadMgr.getTaskByName(null);
                minTimes = 0;
                result = Lists.newArrayList();
                jobAtBoundary.getDbId();
                minTimes = 0;
                result = -1L;
                jobAtBoundary.getLoadFinishTimeMs();
                minTimes = 0;
                result = jobMsWithRemainder;
                jobAtBoundary.getLoadStartTimeMs();
                minTimes = 0;
                result = null;
                jobAtBoundary.getCreateTimeMs();
                minTimes = 0;
                result = null;
                jobAtBoundary.toThrift();
                minTimes = 0;
                result = loadInfoWithLabel("jobAtBoundary");
            }
        };

        TGetLoadsParams req = new TGetLoadsParams();
        req.setLoad_finish_time_to_ms(anchorMs); // second-aligned upper bound

        TGetLoadsResult result = LoadsSystemTable.query(req);

        // anchorMs+789 > anchorMs, so the row fails `value <= upper` at the
        // FE prefilter; BE post-filter on the ms-precision column would also
        // reject it.
        assertEquals(0, labels(result).size());
    }

    // =====================================================================
    // Cursor paging on job id.
    // ---------------------------------------------------------------------
    // getLoads used to answer with every load record in FE memory, which
    // overran the 100MB thrift message limit of the BE-side client on busy
    // clusters. A caller opts into paging by setting start_job_id_offset;
    // FE then cuts the page at a job boundary and hands back the cursor for
    // the next one in next_job_id_offset (absent = end).
    // =====================================================================

    /**
     * Two load jobs and two stream load tasks with ids 10/30 and 20/40 - deliberately
     * interleaved across the two managers, and returned out of id order, so that any
     * test relying on ascending order is really exercising the merge sort rather than
     * the managers' own iteration order.
     *
     * <p>No time bounds are stubbed, so every job clears the filter and paging is the
     * only thing deciding what a response carries.
     */
    private static void wirePagingMocks(GlobalStateMgr globalStateMgr,
                                        LoadMgr loadMgr,
                                        StreamLoadMgr streamLoadMgr,
                                        LoadJob job10,
                                        LoadJob job30,
                                        AbstractStreamLoadTask task20,
                                        AbstractStreamLoadTask task40) {
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                minTimes = 0;
                result = globalStateMgr;

                globalStateMgr.getLoadMgr();
                minTimes = 0;
                result = loadMgr;

                globalStateMgr.getStreamLoadMgr();
                minTimes = 0;
                result = streamLoadMgr;

                loadMgr.getLoadJobs(null);
                minTimes = 0;
                result = Lists.newArrayList(job30, job10);

                streamLoadMgr.getTaskByName(null);
                minTimes = 0;
                result = Lists.newArrayList(task40, task20);

                job10.getId();
                minTimes = 0;
                result = 10L;
                job10.getDbId();
                minTimes = 0;
                result = -1L;
                job10.getLoadFinishTimeMs();
                minTimes = 0;
                result = null;
                job10.getLoadStartTimeMs();
                minTimes = 0;
                result = null;
                job10.getCreateTimeMs();
                minTimes = 0;
                result = null;
                job10.toThrift();
                minTimes = 0;
                result = loadInfoWithLabel("job10");

                job30.getId();
                minTimes = 0;
                result = 30L;
                job30.getDbId();
                minTimes = 0;
                result = -1L;
                job30.getLoadFinishTimeMs();
                minTimes = 0;
                result = null;
                job30.getLoadStartTimeMs();
                minTimes = 0;
                result = null;
                job30.getCreateTimeMs();
                minTimes = 0;
                result = null;
                job30.toThrift();
                minTimes = 0;
                result = loadInfoWithLabel("job30");

                task20.getId();
                minTimes = 0;
                result = 20L;
                task20.getDbId();
                minTimes = 0;
                result = -1L;
                task20.getLoadFinishTimeMs();
                minTimes = 0;
                result = null;
                task20.getLoadStartTimeMs();
                minTimes = 0;
                result = null;
                task20.getCreateTimeMs();
                minTimes = 0;
                result = null;
                task20.toThrift();
                minTimes = 0;
                result = Lists.newArrayList(loadInfoWithLabel("task20"));

                task40.getId();
                minTimes = 0;
                result = 40L;
                task40.getDbId();
                minTimes = 0;
                result = -1L;
                task40.getLoadFinishTimeMs();
                minTimes = 0;
                result = null;
                task40.getLoadStartTimeMs();
                minTimes = 0;
                result = null;
                task40.getCreateTimeMs();
                minTimes = 0;
                result = null;
                task40.toThrift();
                minTimes = 0;
                result = Lists.newArrayList(loadInfoWithLabel("task40"));
            }
        };
    }

    /**
     * A request without start_job_id_offset must be answered in full and without a
     * cursor, even when the row count is over the page limit. That is what an old BE
     * expects mid rolling-upgrade, and what the FE-internal
     * {@code information_schema.load_tracking_logs} path depends on - it calls
     * getLoads directly and would otherwise be silently truncated to one page.
     */
    @Test
    public void testQuery_unpagedRequestIgnoresPageLimit(
            @Mocked GlobalStateMgr globalStateMgr,
            @Mocked LoadMgr loadMgr,
            @Mocked StreamLoadMgr streamLoadMgr,
            @Mocked LoadJob job10,
            @Mocked LoadJob job30,
            @Mocked AbstractStreamLoadTask task20,
            @Mocked AbstractStreamLoadTask task40) {
        wirePagingMocks(globalStateMgr, loadMgr, streamLoadMgr, job10, job30, task20, task40);

        int saved = Config.max_get_loads_result_count;
        Config.max_get_loads_result_count = 1;
        try {
            TGetLoadsResult result = LoadsSystemTable.query(new TGetLoadsParams());

            assertEquals(4, labels(result).size());
            assertFalse(result.isSetNext_job_id_offset(),
                    "an unpaged caller must not be handed a cursor it would ignore");
        } finally {
            Config.max_get_loads_result_count = saved;
        }
    }

    /** Cursor 0 with a one-row limit: lowest id only, cursor points just past it. */
    @Test
    public void testQuery_pagedCutsAtLimitAndReturnsCursor(
            @Mocked GlobalStateMgr globalStateMgr,
            @Mocked LoadMgr loadMgr,
            @Mocked StreamLoadMgr streamLoadMgr,
            @Mocked LoadJob job10,
            @Mocked LoadJob job30,
            @Mocked AbstractStreamLoadTask task20,
            @Mocked AbstractStreamLoadTask task40) {
        wirePagingMocks(globalStateMgr, loadMgr, streamLoadMgr, job10, job30, task20, task40);

        int saved = Config.max_get_loads_result_count;
        Config.max_get_loads_result_count = 1;
        try {
            TGetLoadsParams req = new TGetLoadsParams();
            req.setStart_job_id_offset(0);

            TGetLoadsResult result = LoadsSystemTable.query(req);

            assertEquals(Lists.newArrayList("job10"), labels(result));
            assertTrue(result.isSetNext_job_id_offset());
            assertEquals(11L, result.getNext_job_id_offset());
        } finally {
            Config.max_get_loads_result_count = saved;
        }
    }

    /**
     * Ascending id order across both managers. The mocks hand back job30 before job10
     * and task40 before task20, so an implementation that appended in manager order
     * would fail here - and a cursor over an unsorted stream would skip rows.
     */
    @Test
    public void testQuery_pagedOrdersAcrossManagersByJobId(
            @Mocked GlobalStateMgr globalStateMgr,
            @Mocked LoadMgr loadMgr,
            @Mocked StreamLoadMgr streamLoadMgr,
            @Mocked LoadJob job10,
            @Mocked LoadJob job30,
            @Mocked AbstractStreamLoadTask task20,
            @Mocked AbstractStreamLoadTask task40) {
        wirePagingMocks(globalStateMgr, loadMgr, streamLoadMgr, job10, job30, task20, task40);

        int saved = Config.max_get_loads_result_count;
        Config.max_get_loads_result_count = 100;
        try {
            TGetLoadsParams req = new TGetLoadsParams();
            req.setStart_job_id_offset(0);

            TGetLoadsResult result = LoadsSystemTable.query(req);

            assertEquals(Lists.newArrayList("job10", "task20", "job30", "task40"), labels(result));
            assertFalse(result.isSetNext_job_id_offset(), "a page under the limit is the last one");
        } finally {
            Config.max_get_loads_result_count = saved;
        }
    }

    /**
     * Walk every page the way BE does and assert the union is the whole result set,
     * each row exactly once. This is the property a cursor has to hold: no gap at a
     * page seam, no row served twice.
     */
    @Test
    public void testQuery_pagedWalkCoversEveryRowExactlyOnce(
            @Mocked GlobalStateMgr globalStateMgr,
            @Mocked LoadMgr loadMgr,
            @Mocked StreamLoadMgr streamLoadMgr,
            @Mocked LoadJob job10,
            @Mocked LoadJob job30,
            @Mocked AbstractStreamLoadTask task20,
            @Mocked AbstractStreamLoadTask task40) {
        wirePagingMocks(globalStateMgr, loadMgr, streamLoadMgr, job10, job30, task20, task40);

        int saved = Config.max_get_loads_result_count;
        // 1 and 2 exercise both a seam that lands between two jobs and one that lands
        // on the last job; 3 leaves a short final page.
        for (int pageLimit : new int[] {1, 2, 3}) {
            Config.max_get_loads_result_count = pageLimit;
            try {
                List<String> walked = new ArrayList<>();
                long cursor = 0;
                int requests = 0;
                do {
                    TGetLoadsParams req = new TGetLoadsParams();
                    req.setStart_job_id_offset(cursor);
                    TGetLoadsResult page = LoadsSystemTable.query(req);
                    walked.addAll(labels(page));
                    cursor = page.isSetNext_job_id_offset() ? page.getNext_job_id_offset() : 0;
                    requests++;
                    assertTrue(requests <= 10, "cursor failed to advance, page limit " + pageLimit);
                } while (cursor != 0);

                assertEquals(Lists.newArrayList("job10", "task20", "job30", "task40"), walked,
                        "page limit " + pageLimit);
            } finally {
                Config.max_get_loads_result_count = saved;
            }
        }
    }

    /**
     * A stream load task can expand to several rows. The page is cut at a job
     * boundary, so all of one task's rows land in the same response even when that
     * takes the page over the limit - the alternative would be a cursor that cannot
     * express "resume in the middle of this task".
     */
    @Test
    public void testQuery_pageIsNeverSplitInsideOneJob(
            @Mocked GlobalStateMgr globalStateMgr,
            @Mocked LoadMgr loadMgr,
            @Mocked StreamLoadMgr streamLoadMgr,
            @Mocked AbstractStreamLoadTask multiRowTask) {
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                minTimes = 0;
                result = globalStateMgr;
                globalStateMgr.getLoadMgr();
                minTimes = 0;
                result = loadMgr;
                globalStateMgr.getStreamLoadMgr();
                minTimes = 0;
                result = streamLoadMgr;
                loadMgr.getLoadJobs(null);
                minTimes = 0;
                result = Lists.newArrayList();
                streamLoadMgr.getTaskByName(null);
                minTimes = 0;
                result = Lists.newArrayList(multiRowTask);

                multiRowTask.getId();
                minTimes = 0;
                result = 7L;
                multiRowTask.getDbId();
                minTimes = 0;
                result = -1L;
                multiRowTask.getLoadFinishTimeMs();
                minTimes = 0;
                result = null;
                multiRowTask.getLoadStartTimeMs();
                minTimes = 0;
                result = null;
                multiRowTask.getCreateTimeMs();
                minTimes = 0;
                result = null;
                multiRowTask.toThrift();
                minTimes = 0;
                result = Lists.newArrayList(
                        loadInfoWithLabel("row1"), loadInfoWithLabel("row2"), loadInfoWithLabel("row3"));
            }
        };

        int saved = Config.max_get_loads_result_count;
        Config.max_get_loads_result_count = 2;
        try {
            TGetLoadsParams req = new TGetLoadsParams();
            req.setStart_job_id_offset(0);

            TGetLoadsResult result = LoadsSystemTable.query(req);

            assertEquals(Lists.newArrayList("row1", "row2", "row3"), labels(result),
                    "a job's rows must not straddle two pages");
            assertEquals(8L, result.getNext_job_id_offset());
        } finally {
            Config.max_get_loads_result_count = saved;
        }
    }

    /**
     * The single-job lookup (BE sets job_id when the query has an id predicate) still
     * answers with that job, cursor or no cursor. BE now always attaches
     * start_job_id_offset=0, so this path must not be filtered out by it.
     */
    @Test
    public void testQuery_singleJobLookupUnaffectedByCursor(
            @Mocked GlobalStateMgr globalStateMgr,
            @Mocked LoadMgr loadMgr,
            @Mocked StreamLoadMgr streamLoadMgr,
            @Mocked LoadJob job10) {
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                minTimes = 0;
                result = globalStateMgr;
                globalStateMgr.getLoadMgr();
                minTimes = 0;
                result = loadMgr;
                globalStateMgr.getStreamLoadMgr();
                minTimes = 0;
                result = streamLoadMgr;
                loadMgr.getLoadJob(10L);
                minTimes = 0;
                result = job10;
                streamLoadMgr.getTaskById(10L);
                minTimes = 0;
                result = null;

                job10.getId();
                minTimes = 0;
                result = 10L;
                job10.getDbId();
                minTimes = 0;
                result = -1L;
                job10.getLoadFinishTimeMs();
                minTimes = 0;
                result = null;
                job10.getLoadStartTimeMs();
                minTimes = 0;
                result = null;
                job10.getCreateTimeMs();
                minTimes = 0;
                result = null;
                job10.toThrift();
                minTimes = 0;
                result = loadInfoWithLabel("job10");
            }
        };

        TGetLoadsParams req = new TGetLoadsParams();
        req.setJob_id(10L);
        req.setStart_job_id_offset(0);

        TGetLoadsResult result = LoadsSystemTable.query(req);

        assertEquals(Lists.newArrayList("job10"), labels(result));
        assertFalse(result.isSetNext_job_id_offset());
    }

    /**
     * A db-scoped request resolves the db id and goes through the db-scoped lookup rather
     * than walking every load job. The cursor filter has to apply on this path too.
     */
    @Test
    public void testQuery_dbScopedLookupIsPagedToo(
            @Mocked GlobalStateMgr globalStateMgr,
            @Mocked LocalMetastore localMetastore,
            @Mocked Database database,
            @Mocked LoadMgr loadMgr,
            @Mocked StreamLoadMgr streamLoadMgr,
            @Mocked LoadJob job10,
            @Mocked LoadJob job30) {
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                minTimes = 0;
                result = globalStateMgr;
                globalStateMgr.getLocalMetastore();
                minTimes = 0;
                result = localMetastore;
                globalStateMgr.getLoadMgr();
                minTimes = 0;
                result = loadMgr;
                globalStateMgr.getStreamLoadMgr();
                minTimes = 0;
                result = streamLoadMgr;
                localMetastore.getDb("db1");
                minTimes = 0;
                result = database;
                database.getId();
                minTimes = 0;
                result = 77L;
                loadMgr.getLoadJobsByDb(77L, null, false);
                minTimes = 0;
                result = Lists.newArrayList(job30, job10);
                streamLoadMgr.getTaskByName(null);
                minTimes = 0;
                result = Lists.newArrayList();

                job10.getId();
                minTimes = 0;
                result = 10L;
                job10.getDbId();
                minTimes = 0;
                result = 77L;
                job10.getLoadFinishTimeMs();
                minTimes = 0;
                result = null;
                job10.getLoadStartTimeMs();
                minTimes = 0;
                result = null;
                job10.getCreateTimeMs();
                minTimes = 0;
                result = null;
                job10.toThrift();
                minTimes = 0;
                result = loadInfoWithLabel("job10");

                job30.getId();
                minTimes = 0;
                result = 30L;
                job30.getDbId();
                minTimes = 0;
                result = 77L;
                job30.getLoadFinishTimeMs();
                minTimes = 0;
                result = null;
                job30.getLoadStartTimeMs();
                minTimes = 0;
                result = null;
                job30.getCreateTimeMs();
                minTimes = 0;
                result = null;
                job30.toThrift();
                minTimes = 0;
                result = loadInfoWithLabel("job30");
            }
        };

        TGetLoadsParams req = new TGetLoadsParams();
        req.setDb("db1");
        req.setStart_job_id_offset(11);

        TGetLoadsResult result = LoadsSystemTable.query(req);

        assertEquals(Lists.newArrayList("job30"), labels(result),
                "job10 is behind the cursor and must be skipped on the db-scoped path as well");
    }

    /** The db-scoped lookup switches to the label-matching overload when a label is given. */
    @Test
    public void testQuery_dbScopedLookupWithLabel(
            @Mocked GlobalStateMgr globalStateMgr,
            @Mocked LocalMetastore localMetastore,
            @Mocked Database database,
            @Mocked LoadMgr loadMgr,
            @Mocked StreamLoadMgr streamLoadMgr,
            @Mocked LoadJob job10) {
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                minTimes = 0;
                result = globalStateMgr;
                globalStateMgr.getLocalMetastore();
                minTimes = 0;
                result = localMetastore;
                globalStateMgr.getLoadMgr();
                minTimes = 0;
                result = loadMgr;
                globalStateMgr.getStreamLoadMgr();
                minTimes = 0;
                result = streamLoadMgr;
                localMetastore.getDb("db1");
                minTimes = 0;
                result = database;
                database.getId();
                minTimes = 0;
                result = 77L;
                loadMgr.getLoadJobsByDb(77L, "lbl", true);
                minTimes = 0;
                result = Lists.newArrayList(job10);
                streamLoadMgr.getTaskByName("lbl");
                minTimes = 0;
                result = Lists.newArrayList();

                job10.getId();
                minTimes = 0;
                result = 10L;
                job10.getDbId();
                minTimes = 0;
                result = 77L;
                job10.getLoadFinishTimeMs();
                minTimes = 0;
                result = null;
                job10.getLoadStartTimeMs();
                minTimes = 0;
                result = null;
                job10.getCreateTimeMs();
                minTimes = 0;
                result = null;
                job10.toThrift();
                minTimes = 0;
                result = loadInfoWithLabel("job10");
            }
        };

        TGetLoadsParams req = new TGetLoadsParams();
        req.setDb("db1");
        req.setLabel("lbl");
        req.setStart_job_id_offset(0);

        TGetLoadsResult result = LoadsSystemTable.query(req);

        assertEquals(Lists.newArrayList("job10"), labels(result));
    }

    /**
     * A multi-statement stream load is one page unit keyed by the PARENT id: its matching
     * children are emitted together and the cursor resumes at parentId + 1. Children are
     * allocated after their parent, so that cursor clears the parent and every child it
     * owns - resuming at a child id instead would re-serve part of the task.
     */
    @Test
    public void testQuery_multiStmtTaskIsOnePageUnitKeyedByParentId(
            @Mocked GlobalStateMgr globalStateMgr,
            @Mocked LoadMgr loadMgr,
            @Mocked StreamLoadMgr streamLoadMgr,
            @Mocked StreamLoadMultiStmtTask multiTask,
            @Mocked StreamLoadTask child1,
            @Mocked StreamLoadTask child2) {
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                minTimes = 0;
                result = globalStateMgr;
                globalStateMgr.getLoadMgr();
                minTimes = 0;
                result = loadMgr;
                globalStateMgr.getStreamLoadMgr();
                minTimes = 0;
                result = streamLoadMgr;
                loadMgr.getLoadJobs(null);
                minTimes = 0;
                result = Lists.newArrayList();
                streamLoadMgr.getTaskByName(null);
                minTimes = 0;
                result = Lists.newArrayList(multiTask);

                multiTask.getId();
                minTimes = 0;
                result = 50L;
                multiTask.getTasks();
                minTimes = 0;
                result = Lists.newArrayList(child1, child2);

                child1.getDbId();
                minTimes = 0;
                result = -1L;
                child1.getLoadFinishTimeMs();
                minTimes = 0;
                result = null;
                child1.getLoadStartTimeMs();
                minTimes = 0;
                result = null;
                child1.getCreateTimeMs();
                minTimes = 0;
                result = null;
                child1.toThrift();
                minTimes = 0;
                result = Lists.newArrayList(loadInfoWithLabel("child1"));

                child2.getDbId();
                minTimes = 0;
                result = -1L;
                child2.getLoadFinishTimeMs();
                minTimes = 0;
                result = null;
                child2.getLoadStartTimeMs();
                minTimes = 0;
                result = null;
                child2.getCreateTimeMs();
                minTimes = 0;
                result = null;
                child2.toThrift();
                minTimes = 0;
                result = Lists.newArrayList(loadInfoWithLabel("child2"));
            }
        };

        int saved = Config.max_get_loads_result_count;
        Config.max_get_loads_result_count = 1;
        try {
            TGetLoadsParams req = new TGetLoadsParams();
            req.setStart_job_id_offset(0);

            TGetLoadsResult result = LoadsSystemTable.query(req);

            assertEquals(Lists.newArrayList("child1", "child2"), labels(result),
                    "both children belong to the same page unit");
            assertEquals(51L, result.getNext_job_id_offset(),
                    "cursor must be parentId + 1, not a child id");
        } finally {
            Config.max_get_loads_result_count = saved;
        }
    }

    /**
     * When the filter rejects every child, the multi-statement task contributes no page
     * unit at all - it must not occupy a cursor slot or emit an empty row group.
     */
    @Test
    public void testQuery_multiStmtTaskWithNoMatchingChildIsDropped(
            @Mocked GlobalStateMgr globalStateMgr,
            @Mocked LoadMgr loadMgr,
            @Mocked StreamLoadMgr streamLoadMgr,
            @Mocked StreamLoadMultiStmtTask multiTask,
            @Mocked StreamLoadTask child1) {
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                minTimes = 0;
                result = globalStateMgr;
                globalStateMgr.getLoadMgr();
                minTimes = 0;
                result = loadMgr;
                globalStateMgr.getStreamLoadMgr();
                minTimes = 0;
                result = streamLoadMgr;
                loadMgr.getLoadJobs(null);
                minTimes = 0;
                result = Lists.newArrayList();
                streamLoadMgr.getTaskByName(null);
                minTimes = 0;
                result = Lists.newArrayList(multiTask);

                multiTask.getId();
                minTimes = 0;
                result = 50L;
                multiTask.getTasks();
                minTimes = 0;
                result = Lists.newArrayList(child1);

                child1.getDbId();
                minTimes = 0;
                result = -1L;
                child1.getStateName();
                minTimes = 0;
                result = "CANCELLED";
            }
        };

        TGetLoadsParams req = new TGetLoadsParams();
        req.setState("FINISHED");
        req.setStart_job_id_offset(0);

        TGetLoadsResult result = LoadsSystemTable.query(req);

        assertEquals(0, labels(result).size());
        assertFalse(result.isSetNext_job_id_offset());
    }
}
