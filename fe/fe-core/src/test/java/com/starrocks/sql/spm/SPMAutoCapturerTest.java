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

package com.starrocks.sql.spm;

import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.metric.Metric;
import com.starrocks.metric.MetricLabel;
import com.starrocks.metric.MetricRepo;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.GlobalVariable;
import com.starrocks.sql.plan.PlanTestBase;
import com.starrocks.summary.QueryHistory;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.commons.lang3.reflect.MethodUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.lang.reflect.Constructor;
import java.time.LocalDateTime;
import java.util.List;

public class SPMAutoCapturerTest extends PlanTestBase {
    private String originalPattern;

    @BeforeAll
    public static void beforeAll() throws Exception {
        PlanTestBase.beforeAll();
    }

    @AfterEach
    public void tearDown() {
        if (originalPattern != null) {
            GlobalVariable.spmCaptureIncludeTablePattern = originalPattern;
        }
    }

    @Test
    public void testOnStoppedDropsCapturedConnectContext() throws Exception {
        SPMAutoCapturer capturer = new SPMAutoCapturer();

        ConnectContext captured = Mockito.mock(ConnectContext.class);
        FieldUtils.writeField(capturer, "connect", captured, true);
        Assertions.assertSame(captured, FieldUtils.readField(capturer, "connect", true),
                "precondition: connect installed");

        // The captured ConnectContext is leader-session-only state (carries leader-side query
        // execution state). On demotion it must be released so the next leader rebuilds a
        // fresh context and the demoted FE does not retain references into leader-only state.
        MethodUtils.invokeMethod(capturer, true, "onStopped");

        Assertions.assertNull(FieldUtils.readField(capturer, "connect", true),
                "captured ConnectContext should be cleared on demotion");
    }

    private long getMetricValue(String name, String result) {
        for (Metric<?> metric : MetricRepo.getMetricsByName(name)) {
            for (MetricLabel label : metric.getLabels()) {
                if ("result".equals(label.getKey()) && result.equals(label.getValue())) {
                    return (Long) metric.getValue();
                }
            }
        }
        return 0L;
    }

    private QueryHistory createQueryHistory(String db, String sql, String sqlDigest) throws Exception {
        Constructor<QueryHistory> constructor = QueryHistory.class.getDeclaredConstructor();
        constructor.setAccessible(true);
        QueryHistory queryHistory = constructor.newInstance();
        queryHistory.setDb(db);
        queryHistory.setOriginSQL(sql);
        queryHistory.setSqlDigest(sqlDigest);
        queryHistory.setQueryMs(10);
        queryHistory.setDatetime(LocalDateTime.now());
        return queryHistory;
    }

    @Test
    public void testSPMCaptureCandidateMetrics() throws Exception {
        originalPattern = GlobalVariable.spmCaptureIncludeTablePattern;
        GlobalVariable.spmCaptureIncludeTablePattern = "test\\..*";

        SPMAutoCapturer capturer = new SPMAutoCapturer();
        Deencapsulation.setField(capturer, "connect", connectContext);

        String db = connectContext.getCurrentCatalog() + "." + connectContext.getDatabase();
        String dbName = connectContext.getDatabase();
        long capturedBefore = getMetricValue("spm_capture_candidate_total", SPMMetrics.CAPTURE_CANDIDATE_CAPTURED);
        long dbMissingBefore = getMetricValue("spm_capture_candidate_total",
                SPMMetrics.CAPTURE_CANDIDATE_SKIPPED_DB_MISSING);
        long tableCountBefore = getMetricValue("spm_capture_candidate_total",
                SPMMetrics.CAPTURE_CANDIDATE_SKIPPED_TABLE_COUNT);
        long tableMissingBefore = getMetricValue("spm_capture_candidate_total",
                SPMMetrics.CAPTURE_CANDIDATE_SKIPPED_TABLE_MISSING);
        long patternBefore = getMetricValue("spm_capture_candidate_total",
                SPMMetrics.CAPTURE_CANDIDATE_SKIPPED_PATTERN_MISMATCH);
        long failedBefore = getMetricValue("spm_capture_candidate_total", SPMMetrics.CAPTURE_CANDIDATE_FAILED);

        List<QueryHistory> histories = List.of(
                createQueryHistory("default_catalog.missing_db",
                        "select * from missing_db.t0 join missing_db.t1 on t0.v1 = t1.v4", "db-miss"),
                createQueryHistory(db, "select * from " + dbName + ".t0", "table-count"),
                createQueryHistory(db, "select * from " + dbName + ".t0 join " + dbName
                        + ".missing_table on t0.v1 = missing_table.v1", "table-miss"),
                createQueryHistory(db, "select", "failed"),
                createQueryHistory(db, "select t0.v1 from " + dbName + ".t0 join " + dbName
                        + ".t1 on t0.v1 = t1.v4", "captured"));

        Deencapsulation.invoke(capturer, "generateBaseline", histories);

        GlobalVariable.spmCaptureIncludeTablePattern = "other\\..*";
        Deencapsulation.invoke(capturer, "generateBaseline",
                List.of(createQueryHistory(db, "select * from " + dbName + ".t0 join " + dbName
                        + ".t1 on t0.v1 = t1.v4", "pattern-miss")));

        Assertions.assertEquals(capturedBefore + 1,
                getMetricValue("spm_capture_candidate_total", SPMMetrics.CAPTURE_CANDIDATE_CAPTURED));
        Assertions.assertEquals(dbMissingBefore + 1,
                getMetricValue("spm_capture_candidate_total", SPMMetrics.CAPTURE_CANDIDATE_SKIPPED_DB_MISSING));
        Assertions.assertEquals(tableCountBefore + 1,
                getMetricValue("spm_capture_candidate_total", SPMMetrics.CAPTURE_CANDIDATE_SKIPPED_TABLE_COUNT));
        Assertions.assertEquals(tableMissingBefore + 1,
                getMetricValue("spm_capture_candidate_total", SPMMetrics.CAPTURE_CANDIDATE_SKIPPED_TABLE_MISSING));
        Assertions.assertEquals(patternBefore + 1,
                getMetricValue("spm_capture_candidate_total", SPMMetrics.CAPTURE_CANDIDATE_SKIPPED_PATTERN_MISMATCH));
        Assertions.assertEquals(failedBefore + 1,
                getMetricValue("spm_capture_candidate_total", SPMMetrics.CAPTURE_CANDIDATE_FAILED));
    }
}
