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
import com.starrocks.load.loadv2.LoadJob;
import com.starrocks.load.loadv2.LoadMgr;
import com.starrocks.load.streamload.StreamLoadMgr;
import com.starrocks.load.streamload.StreamLoadTask;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LocalMetastore;
import com.starrocks.thrift.TGetLoadsParams;
import com.starrocks.thrift.TGetLoadsResult;
import com.starrocks.thrift.TLoadInfo;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Cursor paging on job id for getLoads.
 *
 * <p>getLoads used to answer with every load record in FE memory, which overran the
 * 100MB thrift message limit of the BE-side client on busy clusters. A caller opts into
 * paging by setting start_job_id_offset; FE then cuts the page at a job boundary and hands
 * back the cursor for the next one in next_job_id_offset (absent = end).
 */
public class LoadsSystemTableTest {

    private static TLoadInfo loadInfoWithLabel(String label) {
        TLoadInfo info = new TLoadInfo();
        info.setLabel(label);
        return info;
    }

    private static List<String> labels(TGetLoadsResult result) {
        return result.getLoads().stream().map(TLoadInfo::getLabel).collect(Collectors.toList());
    }

    /**
     * Two load jobs and two stream load tasks with ids 10/30 and 20/40 - deliberately
     * interleaved across the two managers, and returned out of id order, so that any test
     * relying on ascending order is really exercising the merge sort rather than the
     * managers' own iteration order.
     */
    private static void wirePagingMocks(GlobalStateMgr globalStateMgr,
                                        LoadMgr loadMgr,
                                        StreamLoadMgr streamLoadMgr,
                                        LoadJob job10,
                                        LoadJob job30,
                                        StreamLoadTask task20,
                                        StreamLoadTask task40) {
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
                job10.toThrift();
                minTimes = 0;
                result = loadInfoWithLabel("job10");

                job30.getId();
                minTimes = 0;
                result = 30L;
                job30.toThrift();
                minTimes = 0;
                result = loadInfoWithLabel("job30");

                task20.getId();
                minTimes = 0;
                result = 20L;
                task20.toThrift();
                minTimes = 0;
                result = loadInfoWithLabel("task20");

                task40.getId();
                minTimes = 0;
                result = 40L;
                task40.toThrift();
                minTimes = 0;
                result = loadInfoWithLabel("task40");
            }
        };
    }

    /**
     * A request without start_job_id_offset must be answered in full and without a
     * cursor, even when the row count is over the page limit. That is what an old BE
     * expects mid rolling-upgrade, and what the FE-internal
     * {@code information_schema.load_tracking_logs} path depends on - it calls getLoads
     * directly and would otherwise be silently truncated to one page.
     */
    @Test
    public void testQuery_unpagedRequestIgnoresPageLimit(
            @Mocked GlobalStateMgr globalStateMgr,
            @Mocked LoadMgr loadMgr,
            @Mocked StreamLoadMgr streamLoadMgr,
            @Mocked LoadJob job10,
            @Mocked LoadJob job30,
            @Mocked StreamLoadTask task20,
            @Mocked StreamLoadTask task40) {
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
            @Mocked StreamLoadTask task20,
            @Mocked StreamLoadTask task40) {
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
            @Mocked StreamLoadTask task20,
            @Mocked StreamLoadTask task40) {
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
            @Mocked StreamLoadTask task20,
            @Mocked StreamLoadTask task40) {
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
                job10.toThrift();
                minTimes = 0;
                result = loadInfoWithLabel("job10");

                job30.getId();
                minTimes = 0;
                result = 30L;
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
}
