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

/**
 * Pin the DML shape that {@link ContextTaskScheduler#buildUpdateStateSql} emits. The original
 * implementation re-issued an {@code INSERT} with sentinel values
 * ({@code contextbase_id=0, task_type='UNKNOWN', payload_json=NULL}) and depended on the
 * primary-key engine's replace semantics to overwrite the existing row — which silently wiped
 * every column except {@code state} and {@code error_message}. This test fails loudly if any
 * future refactor regresses that to {@code INSERT}.
 */
public class ContextTaskSchedulerSqlTest {

    @Test
    public void buildsUpdateNotInsert() {
        String sql = ContextTaskScheduler.buildUpdateStateSql(42L, "RUNNING", null);
        Assertions.assertTrue(sql.startsWith("UPDATE "), sql);
        Assertions.assertFalse(sql.contains("INSERT"), sql);
        Assertions.assertFalse(sql.contains("'UNKNOWN'"),
                "INSERT-replace path used 'UNKNOWN' as sentinel task_type; UPDATE path must not write any task_type");
        Assertions.assertFalse(sql.contains("contextbase_id"),
                "UPDATE must not touch contextbase_id — the original INSERT wiped it to 0");
        Assertions.assertFalse(sql.contains("payload_json"),
                "UPDATE must not touch payload_json — the original INSERT wiped it to NULL");
        Assertions.assertTrue(sql.contains("WHERE task_id = 42"));
        Assertions.assertTrue(sql.contains("state = 'RUNNING'"));
    }

    @Test
    public void buildsUpdateWithErrorMessage() {
        String sql = ContextTaskScheduler.buildUpdateStateSql(7L, "FAILED", "provider 500");
        Assertions.assertTrue(sql.startsWith("UPDATE "), sql);
        Assertions.assertTrue(sql.contains("state = 'FAILED'"));
        Assertions.assertTrue(sql.contains("error_message = 'provider 500'"));
        Assertions.assertTrue(sql.contains("WHERE task_id = 7"));
    }

    @Test
    public void errorMessageWithSingleQuoteIsEscaped() {
        // Direct verification that the unified escape helper is wired into the update path.
        // A pre-fix bug here would either leave the unescaped quote (breaking the SQL) or pick
        // an inconsistent escape that the BE parser rejects.
        String sql = ContextTaskScheduler.buildUpdateStateSql(8L, "FAILED", "it's bad");
        Assertions.assertTrue(sql.contains("'it''s bad'"), sql);
    }

    @Test
    public void errorMessageWithNewlineEscaped() {
        // Audit-log readers grep statement-per-line; raw newlines inside a literal would split
        // the logged DML across multiple lines. The unified escape replaces them with \n.
        String sql = ContextTaskScheduler.buildUpdateStateSql(9L, "FAILED", "boom\nstack");
        Assertions.assertFalse(sql.contains("boom\nstack"), sql);
        Assertions.assertTrue(sql.contains("'boom\\nstack'"), sql);
    }
}
