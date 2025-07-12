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

package com.starrocks.sql.ast;

import com.starrocks.analysis.BinaryPredicate;
import com.starrocks.analysis.BinaryType;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.sql.parser.NodePosition;
import org.junit.Assert;
import org.junit.Test;

public class DropSnapshotStmtTest {

    @Test
    public void testBasicConstruction() {
        // Test basic construction without WHERE clause
        DropSnapshotStmt stmt = new DropSnapshotStmt("test_repo", null);
        Assert.assertEquals("test_repo", stmt.getRepoName());
        Assert.assertNull(stmt.getWhere());
        Assert.assertNull(stmt.getSnapshotName());
        Assert.assertNull(stmt.getTimestamp());
        Assert.assertNull(stmt.getTimestampOperator());
        Assert.assertTrue(stmt.getSnapshotNames().isEmpty());
    }

    @Test
    public void testConstructionWithPosition() {
        NodePosition pos = new NodePosition(1, 1);
        DropSnapshotStmt stmt = new DropSnapshotStmt("test_repo", null, pos);
        Assert.assertEquals("test_repo", stmt.getRepoName());
        Assert.assertEquals(pos, stmt.getPos());
    }

    @Test
    public void testSettersAndGetters() {
        DropSnapshotStmt stmt = new DropSnapshotStmt("test_repo", null);
        
        // Test snapshot name
        stmt.setSnapshotName("test_snapshot");
        Assert.assertEquals("test_snapshot", stmt.getSnapshotName());
        
        // Test timestamp operator
        stmt.setTimestampOperator("<=");
        Assert.assertEquals("<=", stmt.getTimestampOperator());
        
        // Test timestamp
        stmt.setTimestamp("2024-01-01-12-00-00");
        Assert.assertEquals("2024-01-01-12-00-00", stmt.getTimestamp());
        
        // Test adding snapshot names
        stmt.addSnapshotName("snapshot1");
        stmt.addSnapshotName("snapshot2");
        Assert.assertEquals(2, stmt.getSnapshotNames().size());
        Assert.assertTrue(stmt.getSnapshotNames().contains("snapshot1"));
        Assert.assertTrue(stmt.getSnapshotNames().contains("snapshot2"));
    }

    @Test
    public void testToSqlWithoutWhere() {
        DropSnapshotStmt stmt = new DropSnapshotStmt("test_repo", null);
        String sql = stmt.toSql();
        Assert.assertEquals("DROP SNAPSHOT ON test_repo", sql);
    }

    @Test
    public void testToSqlWithWhere() {
        // Create a simple WHERE clause: SNAPSHOT = 'test_snapshot'
        SlotRef slotRef = new SlotRef(null, "SNAPSHOT");
        StringLiteral stringLiteral = new StringLiteral("test_snapshot");
        BinaryPredicate where = new BinaryPredicate(BinaryType.EQ, slotRef, stringLiteral);
        
        DropSnapshotStmt stmt = new DropSnapshotStmt("test_repo", where);
        String sql = stmt.toSql();
        Assert.assertTrue(sql.startsWith("DROP SNAPSHOT ON test_repo WHERE"));
        Assert.assertTrue(sql.contains("SNAPSHOT"));
        Assert.assertTrue(sql.contains("test_snapshot"));
    }

    @Test
    public void testVisitorPattern() {
        DropSnapshotStmt stmt = new DropSnapshotStmt("test_repo", null);

        // Create a simple visitor to test the pattern
        AstVisitor<String, Void> visitor = new AstVisitor<String, Void>() {
            @Override
            public String visitDropSnapshotStatement(DropSnapshotStmt statement, Void context) {
                return "visited_drop_snapshot";
            }
        };

        String result = stmt.accept(visitor, null);
        Assert.assertEquals("visited_drop_snapshot", result);
    }

    @Test
    public void testDefaultVisitorBehavior() {
        DropSnapshotStmt stmt = new DropSnapshotStmt("test_repo", null);

        // Create a visitor that uses the default visitDropSnapshotStatement implementation
        // This will test the line: return visitDDLStatement(statement, context);
        AstVisitor<String, Void> defaultVisitor = new AstVisitor<String, Void>() {
            @Override
            public String visitDDLStatement(DdlStmt statement, Void context) {
                return "visited_ddl_statement";
            }

            // Don't override visitDropSnapshotStatement to test the default behavior
        };

        String result = stmt.accept(defaultVisitor, null);
        Assert.assertEquals("visited_ddl_statement", result);
    }

    @Test
    public void testVisitorPatternWithContext() {
        DropSnapshotStmt stmt = new DropSnapshotStmt("test_repo", null);

        // Test visitor pattern with context parameter
        AstVisitor<String, String> contextVisitor = new AstVisitor<String, String>() {
            @Override
            public String visitDropSnapshotStatement(DropSnapshotStmt statement, String context) {
                return "visited_" + statement.getRepoName() + "_with_" + context;
            }
        };

        String result = stmt.accept(contextVisitor, "test_context");
        Assert.assertEquals("visited_test_repo_with_test_context", result);
    }

    @Test
    public void testVisitorPatternWithDifferentReturnTypes() {
        DropSnapshotStmt stmt = new DropSnapshotStmt("test_repo", null);
        stmt.setSnapshotName("test_snapshot");

        // Test visitor that returns Integer
        AstVisitor<Integer, Void> intVisitor = new AstVisitor<Integer, Void>() {
            @Override
            public Integer visitDropSnapshotStatement(DropSnapshotStmt statement, Void context) {
                return statement.getRepoName().length();
            }
        };

        Integer intResult = stmt.accept(intVisitor, null);
        Assert.assertEquals(Integer.valueOf(9), intResult); // "test_repo".length() = 9

        // Test visitor that returns Boolean
        AstVisitor<Boolean, Void> boolVisitor = new AstVisitor<Boolean, Void>() {
            @Override
            public Boolean visitDropSnapshotStatement(DropSnapshotStmt statement, Void context) {
                return statement.getSnapshotName() != null;
            }
        };

        Boolean boolResult = stmt.accept(boolVisitor, null);
        Assert.assertTrue(boolResult);
    }

    @Test
    public void testDefaultVisitorWithDifferentStatements() {
        DropSnapshotStmt stmt1 = new DropSnapshotStmt("repo1", null);
        DropSnapshotStmt stmt2 = new DropSnapshotStmt("repo2", null);
        stmt2.setSnapshotName("snapshot2");

        // Test that the default visitor behavior works consistently
        AstVisitor<String, Void> defaultVisitor = new AstVisitor<String, Void>() {
            @Override
            public String visitDDLStatement(DdlStmt statement, Void context) {
                if (statement instanceof DropSnapshotStmt) {
                    DropSnapshotStmt dropStmt = (DropSnapshotStmt) statement;
                    return "ddl_" + dropStmt.getRepoName();
                }
                return "ddl_unknown";
            }
        };

        String result1 = stmt1.accept(defaultVisitor, null);
        String result2 = stmt2.accept(defaultVisitor, null);

        Assert.assertEquals("ddl_repo1", result1);
        Assert.assertEquals("ddl_repo2", result2);
    }

    @Test
    public void testMultipleSnapshotNames() {
        DropSnapshotStmt stmt = new DropSnapshotStmt("test_repo", null);
        
        // Add multiple snapshot names
        stmt.addSnapshotName("snapshot1");
        stmt.addSnapshotName("snapshot2");
        stmt.addSnapshotName("snapshot3");
        
        Assert.assertEquals(3, stmt.getSnapshotNames().size());
        Assert.assertTrue(stmt.getSnapshotNames().contains("snapshot1"));
        Assert.assertTrue(stmt.getSnapshotNames().contains("snapshot2"));
        Assert.assertTrue(stmt.getSnapshotNames().contains("snapshot3"));
    }

    @Test
    public void testTimestampOperators() {
        DropSnapshotStmt stmt = new DropSnapshotStmt("test_repo", null);
        
        // Test <= operator
        stmt.setTimestampOperator("<=");
        stmt.setTimestamp("2024-01-01-12-00-00");
        Assert.assertEquals("<=", stmt.getTimestampOperator());
        Assert.assertEquals("2024-01-01-12-00-00", stmt.getTimestamp());
        
        // Test >= operator
        stmt.setTimestampOperator(">=");
        stmt.setTimestamp("2024-12-31-23-59-59");
        Assert.assertEquals(">=", stmt.getTimestampOperator());
        Assert.assertEquals("2024-12-31-23-59-59", stmt.getTimestamp());
    }

    @Test
    public void testRepoNameVariations() {
        // Test different repository name formats
        DropSnapshotStmt stmt1 = new DropSnapshotStmt("simple_repo", null);
        Assert.assertEquals("simple_repo", stmt1.getRepoName());
        
        DropSnapshotStmt stmt2 = new DropSnapshotStmt("repo-with-dashes", null);
        Assert.assertEquals("repo-with-dashes", stmt2.getRepoName());
        
        DropSnapshotStmt stmt3 = new DropSnapshotStmt("repo_with_underscores", null);
        Assert.assertEquals("repo_with_underscores", stmt3.getRepoName());
        
        DropSnapshotStmt stmt4 = new DropSnapshotStmt("repo123", null);
        Assert.assertEquals("repo123", stmt4.getRepoName());
    }

    @Test
    public void testSnapshotNameVariations() {
        DropSnapshotStmt stmt = new DropSnapshotStmt("test_repo", null);
        
        // Test different snapshot name formats
        stmt.setSnapshotName("simple_snapshot");
        Assert.assertEquals("simple_snapshot", stmt.getSnapshotName());
        
        stmt.setSnapshotName("snapshot-with-dashes");
        Assert.assertEquals("snapshot-with-dashes", stmt.getSnapshotName());
        
        stmt.setSnapshotName("snapshot.with.dots");
        Assert.assertEquals("snapshot.with.dots", stmt.getSnapshotName());
        
        stmt.setSnapshotName("snapshot_123");
        Assert.assertEquals("snapshot_123", stmt.getSnapshotName());
        
        stmt.setSnapshotName("123_snapshot");
        Assert.assertEquals("123_snapshot", stmt.getSnapshotName());
    }

    @Test
    public void testClearAndReset() {
        DropSnapshotStmt stmt = new DropSnapshotStmt("test_repo", null);
        
        // Set some values
        stmt.setSnapshotName("test_snapshot");
        stmt.setTimestampOperator("<=");
        stmt.setTimestamp("2024-01-01-12-00-00");
        stmt.addSnapshotName("snapshot1");
        stmt.addSnapshotName("snapshot2");
        
        // Verify they are set
        Assert.assertEquals("test_snapshot", stmt.getSnapshotName());
        Assert.assertEquals("<=", stmt.getTimestampOperator());
        Assert.assertEquals("2024-01-01-12-00-00", stmt.getTimestamp());
        Assert.assertEquals(2, stmt.getSnapshotNames().size());
        
        // Reset values
        stmt.setSnapshotName(null);
        stmt.setTimestampOperator(null);
        stmt.setTimestamp(null);
        stmt.getSnapshotNames().clear();
        
        // Verify they are cleared
        Assert.assertNull(stmt.getSnapshotName());
        Assert.assertNull(stmt.getTimestampOperator());
        Assert.assertNull(stmt.getTimestamp());
        Assert.assertTrue(stmt.getSnapshotNames().isEmpty());
    }
}
