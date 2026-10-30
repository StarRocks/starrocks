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

package com.starrocks.sql.parser;

import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.ast.ExecuteScriptStmt;
import com.starrocks.sql.ast.StatementBase;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ExecuteScriptStmtParserTest {

    private static ExecuteScriptStmt parse(String sql) {
        List<StatementBase> stmts = SqlParser.parse(sql, new SessionVariable());
        return (ExecuteScriptStmt) stmts.get(0);
    }

    @Test
    void testFrontendTarget() {
        ExecuteScriptStmt stmt = parse("ADMIN EXECUTE ON FRONTEND 'print 1'");
        assertEquals(ExecuteScriptStmt.TargetType.FRONTEND, stmt.getTargetType());
        assertTrue(stmt.isFrontendScript());
        assertTrue(stmt.getNodeIds().isEmpty());
        assertEquals("print 1", stmt.getScript());
    }

    @Test
    void testSingleBackendTarget() {
        ExecuteScriptStmt stmt = parse("ADMIN EXECUTE ON 10001 'do_thing'");
        assertEquals(ExecuteScriptStmt.TargetType.NODES, stmt.getTargetType());
        assertEquals(List.of(10001L), stmt.getNodeIds());
        assertEquals(10001L, stmt.getBeId());
    }

    @Test
    void testMultipleBackendTargets() {
        ExecuteScriptStmt stmt = parse("ADMIN EXECUTE ON 10001, 10002, 10003 'do_thing'");
        assertEquals(ExecuteScriptStmt.TargetType.NODES, stmt.getTargetType());
        assertEquals(List.of(10001L, 10002L, 10003L), stmt.getNodeIds());
    }

    @Test
    void testAllBackendsTarget() {
        ExecuteScriptStmt stmt = parse("ADMIN EXECUTE ON ALL BACKENDS 'do_thing'");
        assertEquals(ExecuteScriptStmt.TargetType.ALL_BACKENDS, stmt.getTargetType());
        assertTrue(stmt.getNodeIds().isEmpty());
    }

    @Test
    void testAllComputeNodesTarget() {
        ExecuteScriptStmt stmt = parse("ADMIN EXECUTE ON ALL COMPUTE NODES 'do_thing'");
        assertEquals(ExecuteScriptStmt.TargetType.ALL_COMPUTE_NODES, stmt.getTargetType());
        assertTrue(stmt.getNodeIds().isEmpty());
    }

    @Test
    void testNodeIdsAreDefensivelyCopied() {
        List<Long> mutable = new ArrayList<>();
        mutable.add(10001L);
        mutable.add(10002L);
        ExecuteScriptStmt stmt = new ExecuteScriptStmt(
                ExecuteScriptStmt.TargetType.NODES, mutable, "s", NodePosition.ZERO);
        mutable.add(99999L);
        mutable.set(0, 77777L);
        assertEquals(List.of(10001L, 10002L), stmt.getNodeIds());
        assertThrows(UnsupportedOperationException.class, () -> stmt.getNodeIds().add(1L));
    }
}
