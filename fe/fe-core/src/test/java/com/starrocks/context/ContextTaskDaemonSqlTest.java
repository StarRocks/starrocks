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

package com.starrocks.context;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ContextTaskDaemonSqlTest {

    @Test
    public void testBuildFailedStateSqlEscapesBackslashAndQuote() {
        String sql = ContextTaskDaemon.buildFailedStateSql(7L, "2026-05-22 10:00:00", "boom\\'tail");
        Assertions.assertTrue(sql.contains("error_message = 'boom\\\\''tail'"), sql);
        Assertions.assertTrue(sql.contains("WHERE task_id = 7"), sql);
    }

    @Test
    public void testBuildFailedStateSqlEscapesNewline() {
        String sql = ContextTaskDaemon.buildFailedStateSql(8L, "2026-05-22 10:00:00", "boom\nstack");
        Assertions.assertTrue(sql.contains("'boom\\nstack'"), sql);
        Assertions.assertFalse(sql.contains("boom\nstack"), sql);
    }

    @Test
    public void testBuildFailedStateSqlTruncatesBeforeEscapingSequenceBoundary() {
        StringBuilder raw = new StringBuilder();
        for (int i = 0; i < 1899; i++) {
            raw.append('a');
        }
        raw.append('\\');
        String sql = ContextTaskDaemon.buildFailedStateSql(9L, "2026-05-22 10:00:00", raw.toString());
        String escaped = extractErrorMessage(sql);
        Assertions.assertEquals(1899, escaped.length(), sql);
        Assertions.assertFalse(escaped.endsWith("\\"), sql);
    }

    @Test
    public void testBuildFailedStateSqlKeepsEscapedPayloadWithinLimit() {
        StringBuilder raw = new StringBuilder();
        for (int i = 0; i < 1200; i++) {
            raw.append('\n');
        }
        String sql = ContextTaskDaemon.buildFailedStateSql(10L, "2026-05-22 10:00:00", raw.toString());
        String escaped = extractErrorMessage(sql);
        Assertions.assertTrue(escaped.length() <= 1900, sql);
        Assertions.assertEquals(0, escaped.length() % 2, sql);
    }

    private static String extractErrorMessage(String sql) {
        String marker = "error_message = '";
        int start = sql.indexOf(marker);
        Assertions.assertTrue(start >= 0, sql);
        start += marker.length();
        int end = sql.lastIndexOf("' WHERE task_id = ");
        Assertions.assertTrue(end > start, sql);
        return sql.substring(start, end);
    }
}
