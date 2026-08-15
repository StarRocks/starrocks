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
import com.starrocks.common.util.TimeUtils;
import com.starrocks.load.loadv2.LoadJob;
import com.starrocks.load.loadv2.LoadMgr;
import com.starrocks.load.streamload.AbstractStreamLoadTask;
import com.starrocks.load.streamload.StreamLoadMgr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TGetLoadsParams;
import com.starrocks.thrift.TGetLoadsResult;
import com.starrocks.thrift.TLoadInfo;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
}
