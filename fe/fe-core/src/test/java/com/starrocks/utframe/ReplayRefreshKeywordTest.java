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

package com.starrocks.utframe;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ReplayRefreshKeywordTest {

    @Test
    public void testBareAsyncClauseBecomesOnChange() {
        String ddl = "CREATE MATERIALIZED VIEW `mv` (`k1`)\n"
                + "DISTRIBUTED BY HASH(`k1`) BUCKETS 3\n"
                + "REFRESH ASYNC\n"
                + "AS SELECT k1 FROM t";

        Assertions.assertEquals("CREATE MATERIALIZED VIEW `mv` (`k1`)\n"
                + "DISTRIBUTED BY HASH(`k1`) BUCKETS 3\n"
                + "REFRESH ON_CHANGE\n"
                + "AS SELECT k1 FROM t", UtFrameUtils.normalizeReplayRefreshKeyword(ddl));
    }

    @Test
    public void testRefreshMomentIsKept() {
        String ddl = "CREATE MATERIALIZED VIEW `mv` (`k1`)\n"
                + "REFRESH DEFERRED ASYNC\n"
                + "AS SELECT k1 FROM t";

        Assertions.assertTrue(UtFrameUtils.normalizeReplayRefreshKeyword(ddl)
                .contains("REFRESH DEFERRED ON_CHANGE"));
    }

    @Test
    public void testQueryLiteralIsNotRewritten() {
        String ddl = "CREATE MATERIALIZED VIEW `mv` (`k1`, `note`)\n"
                + "REFRESH ASYNC\n"
                + "AS SELECT k1, 'REFRESH ASYNC' AS note FROM t";

        String normalized = UtFrameUtils.normalizeReplayRefreshKeyword(ddl);
        Assertions.assertTrue(normalized.contains("\nREFRESH ON_CHANGE\n"), normalized);
        Assertions.assertTrue(normalized.contains("'REFRESH ASYNC' AS note"), normalized);
    }

    @Test
    public void testTimedFormIsLeftAlone() {
        for (String clause : new String[] {
                "REFRESH ASYNC EVERY(INTERVAL 1 HOUR)",
                "REFRESH ASYNC START(\"2026-01-01 00:00:00\") EVERY(INTERVAL 1 DAY)",
                "REFRESH SCHEDULE EVERY(INTERVAL 1 HOUR)",
                "REFRESH ON_CHANGE",
                "REFRESH MANUAL"}) {
            String ddl = "CREATE MATERIALIZED VIEW `mv` (`k1`)\n" + clause + "\nAS SELECT k1 FROM t";
            Assertions.assertEquals(ddl, UtFrameUtils.normalizeReplayRefreshKeyword(ddl), clause);
        }
    }
}
