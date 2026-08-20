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

package com.starrocks.analysis;

import com.starrocks.common.Config;
import com.starrocks.failpoint.TriggerMode;
import com.starrocks.proto.FailPointTriggerModeType;
import com.starrocks.proto.PUpdateFailPointStatusRequest;
import com.starrocks.sql.analyzer.AnalyzeTestUtil;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.UpdateFailPointStatusStatement;
import com.starrocks.thrift.TUpdateFailPointRequest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

public class FailPointStmtTest {
    @BeforeAll
    public static void beforeClass() throws Exception {
        AnalyzeTestUtil.init();
    }

    public void testNormalCase(String sql) {
        StatementBase stmt = AnalyzeTestUtil.analyzeSuccess(sql);
        Assertions.assertEquals(sql, stmt.toSql());
    }

    @Test
    public void testUpdateFailPointStatus() {
        List<String> sqls = Arrays.asList(
                "ADMIN ENABLE FAILPOINT 'test'",
                "ADMIN ENABLE FAILPOINT 'test' WITH 1 TIMES",
                "ADMIN ENABLE FAILPOINT 'test' WITH 0.5 PROBABILITY",
                "ADMIN ENABLE FAILPOINT 'test' WITH PAUSE",
                "ADMIN ENABLE FAILPOINT 'test' WITH PAUSE ON FRONTEND",
                "ADMIN ENABLE FAILPOINT 'test' WITH PAUSE ON BACKEND '127.0.0.1:9000,127.0.0.2:9002'",
                "ADMIN ENABLE FAILPOINT 'test' ON BACKEND '127.0.0.1:9000,127.0.0.2:9002'",
                "ADMIN DISABLE FAILPOINT 'test'"
        );
        for (String sql : sqls) {
            testNormalCase(sql);
        }
    }

    @Test
    public void testPauseIsExclusiveWithTimesAndProbability() {
        AnalyzeTestUtil.analyzeFail("ADMIN ENABLE FAILPOINT 'test' WITH 1 TIMES PAUSE");
        AnalyzeTestUtil.analyzeFail("ADMIN ENABLE FAILPOINT 'test' WITH PAUSE 1 TIMES");
    }

    @Test
    public void testPauseWireEncodingDegradesSafely() {
        UpdateFailPointStatusStatement stmt = (UpdateFailPointStatusStatement)
                AnalyzeTestUtil.analyzeSuccess("ADMIN ENABLE FAILPOINT 'test' WITH PAUSE");

        // proto: trigger_mode.mode = DISABLE so a BE predating the pause disables rather than
        // defaulting an unknown enum to ENABLE, and the discriminator rides on the REQUEST so such a
        // BE cannot echo it back and make SHOW FAILPOINTS lie.
        PUpdateFailPointStatusRequest request = stmt.toProto();
        Assertions.assertEquals(FailPointTriggerModeType.DISABLE, request.triggerMode.mode);
        Assertions.assertNull(request.triggerMode.pause);
        Assertions.assertEquals(Boolean.TRUE, request.pause);
        Assertions.assertEquals(Integer.valueOf(Config.failpoint_pause_timeout_second),
                request.pauseTimeoutSecond);

        // thrift: is_enable = false + pause = true, for the same reason on an old FE.
        TUpdateFailPointRequest thriftRequest = stmt.toThrift();
        Assertions.assertFalse(thriftRequest.isIs_enable());
        Assertions.assertTrue(thriftRequest.isPause());
        // Followers snapshot the same timeout rather than re-reading their own config.
        Assertions.assertEquals(Config.failpoint_pause_timeout_second,
                thriftRequest.getPause_timeout_second());

        // A pause is an ENABLE statement even though its wire form says is_enable = false; the
        // leader must arm off isArming(), never off the raw flag.
        Assertions.assertTrue(stmt.isArming());

        // local execution is unaffected by the wire encoding
        Assertions.assertEquals(TriggerMode.PAUSE, stmt.getTriggerPolicy().getMode());
    }

    @Test
    public void testPauseTimeoutIsNormalizedOnTheWire() {
        int original = Config.failpoint_pause_timeout_second;
        try {
            Config.failpoint_pause_timeout_second = 0;
            UpdateFailPointStatusStatement stmt = (UpdateFailPointStatusStatement)
                    AnalyzeTestUtil.analyzeSuccess("ADMIN ENABLE FAILPOINT 'test' WITH PAUSE");
            // Clamped to 1 before being sent, so FE and BE cannot disagree about a bad value.
            Assertions.assertEquals(Integer.valueOf(1), stmt.toProto().pauseTimeoutSecond);
        } finally {
            Config.failpoint_pause_timeout_second = original;
        }
    }

    @Test
    public void testNonPauseEncodingUnchanged() {
        UpdateFailPointStatusStatement stmt = (UpdateFailPointStatusStatement)
                AnalyzeTestUtil.analyzeSuccess("ADMIN ENABLE FAILPOINT 'test' WITH 3 TIMES");
        Assertions.assertEquals(FailPointTriggerModeType.ENABLE_N_TIMES, stmt.toProto().triggerMode.mode);
        Assertions.assertNull(stmt.toProto().pause);
        Assertions.assertTrue(stmt.toThrift().isIs_enable());
        Assertions.assertFalse(stmt.toThrift().isSetPause());
        Assertions.assertTrue(stmt.isArming());
    }

    @Test
    public void testShowFailPoints() {
        List<String> sqls = Arrays.asList(
                "SHOW FAILPOINTS",
                "SHOW FAILPOINTS LIKE '%a%'",
                "SHOW FAILPOINTS ON BACKEND '127.0.0.1:9000,127.0.0.1:9002'",
                "SHOW FAILPOINTS LIKE '%a%' ON BACKEND '127.0.0.1:9000,127.0.0.1:9002'"
        );
        for (String sql : sqls) {
            testNormalCase(sql);
        }
    }
}
