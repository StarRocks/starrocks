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

package com.starrocks.sql;

import com.starrocks.sql.ast.DropSnapshotStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.parser.SqlParser;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

/**
 * Integration test for DROP SNAPSHOT functionality.
 * This test verifies that the complete pipeline from SQL parsing to AST creation works correctly.
 */
public class DropSnapshotIntegrationTest {

    @Test
    public void testDropSnapshotSqlParsing() {
        // Test basic DROP SNAPSHOT parsing
        String sql = "DROP SNAPSHOT ON repo WHERE SNAPSHOT = 'backup1'";
        
        try {
            List<StatementBase> statements = SqlParser.parse(sql, 0);
            Assert.assertEquals("Should parse exactly one statement", 1, statements.size());
            Assert.assertTrue("Should be DropSnapshotStmt", statements.get(0) instanceof DropSnapshotStmt);
            
            DropSnapshotStmt stmt = (DropSnapshotStmt) statements.get(0);
            Assert.assertEquals("Repository name should match", "repo", stmt.getRepoName());
            Assert.assertNotNull("WHERE clause should not be null", stmt.getWhere());
            
            System.out.println("✓ Basic DROP SNAPSHOT parsing test passed");
        } catch (Exception e) {
            Assert.fail("Failed to parse basic DROP SNAPSHOT statement: " + e.getMessage());
        }
    }

    @Test
    public void testDropSnapshotWithTimestamp() {
        // Test DROP SNAPSHOT with timestamp filter
        String sql = "DROP SNAPSHOT ON repo WHERE TIMESTAMP <= '2024-01-01-12-00-00'";
        
        try {
            List<StatementBase> statements = SqlParser.parse(sql, 0);
            Assert.assertEquals("Should parse exactly one statement", 1, statements.size());
            Assert.assertTrue("Should be DropSnapshotStmt", statements.get(0) instanceof DropSnapshotStmt);
            
            DropSnapshotStmt stmt = (DropSnapshotStmt) statements.get(0);
            Assert.assertEquals("Repository name should match", "repo", stmt.getRepoName());
            Assert.assertNotNull("WHERE clause should not be null", stmt.getWhere());
            
            System.out.println("✓ DROP SNAPSHOT with timestamp parsing test passed");
        } catch (Exception e) {
            Assert.fail("Failed to parse DROP SNAPSHOT with timestamp: " + e.getMessage());
        }
    }

    @Test
    public void testDropSnapshotWithCompoundCondition() {
        // Test DROP SNAPSHOT with compound WHERE condition
        String sql = "DROP SNAPSHOT ON repo WHERE SNAPSHOT = 'backup1' AND TIMESTAMP <= '2024-01-01-12-00-00'";
        
        try {
            List<StatementBase> statements = SqlParser.parse(sql, 0);
            Assert.assertEquals("Should parse exactly one statement", 1, statements.size());
            Assert.assertTrue("Should be DropSnapshotStmt", statements.get(0) instanceof DropSnapshotStmt);
            
            DropSnapshotStmt stmt = (DropSnapshotStmt) statements.get(0);
            Assert.assertEquals("Repository name should match", "repo", stmt.getRepoName());
            Assert.assertNotNull("WHERE clause should not be null", stmt.getWhere());
            
            System.out.println("✓ DROP SNAPSHOT with compound condition parsing test passed");
        } catch (Exception e) {
            Assert.fail("Failed to parse DROP SNAPSHOT with compound condition: " + e.getMessage());
        }
    }

    @Test
    public void testDropSnapshotToSql() {
        // Test that parsed statement can be converted back to SQL
        String originalSql = "DROP SNAPSHOT ON repo WHERE SNAPSHOT = 'backup1'";
        
        try {
            List<StatementBase> statements = SqlParser.parse(originalSql, 0);
            DropSnapshotStmt stmt = (DropSnapshotStmt) statements.get(0);
            
            String generatedSql = stmt.toSql();
            Assert.assertNotNull("Generated SQL should not be null", generatedSql);
            Assert.assertTrue("Generated SQL should contain DROP SNAPSHOT", 
                            generatedSql.contains("DROP SNAPSHOT"));
            Assert.assertTrue("Generated SQL should contain repository name", 
                            generatedSql.contains("repo"));
            
            System.out.println("✓ DROP SNAPSHOT toSql() test passed");
            System.out.println("  Original: " + originalSql);
            System.out.println("  Generated: " + generatedSql);
        } catch (Exception e) {
            Assert.fail("Failed to convert DROP SNAPSHOT statement to SQL: " + e.getMessage());
        }
    }

    @Test
    public void testDropSnapshotAstProperties() {
        // Test AST node properties
        String sql = "DROP SNAPSHOT ON test_repo WHERE SNAPSHOT = 'test_backup'";
        
        try {
            List<StatementBase> statements = SqlParser.parse(sql, 0);
            DropSnapshotStmt stmt = (DropSnapshotStmt) statements.get(0);
            
            // Test basic properties
            Assert.assertEquals("Repository name should match", "test_repo", stmt.getRepoName());
            Assert.assertNotNull("WHERE clause should not be null", stmt.getWhere());
            
            // Test setter methods
            stmt.setSnapshotName("manual_snapshot");
            Assert.assertEquals("Snapshot name should be set", "manual_snapshot", stmt.getSnapshotName());
            
            stmt.setTimestamp("2024-01-01-12-00-00");
            Assert.assertEquals("Timestamp should be set", "2024-01-01-12-00-00", stmt.getTimestamp());
            
            stmt.setTimestampOperator("<=");
            Assert.assertEquals("Timestamp operator should be set", "<=", stmt.getTimestampOperator());
            
            // Test snapshot names list
            stmt.addSnapshotName("snapshot1");
            stmt.addSnapshotName("snapshot2");
            Assert.assertEquals("Should have 2 snapshot names", 2, stmt.getSnapshotNames().size());
            Assert.assertTrue("Should contain snapshot1", stmt.getSnapshotNames().contains("snapshot1"));
            Assert.assertTrue("Should contain snapshot2", stmt.getSnapshotNames().contains("snapshot2"));
            
            System.out.println("✓ DROP SNAPSHOT AST properties test passed");
        } catch (Exception e) {
            Assert.fail("Failed to test DROP SNAPSHOT AST properties: " + e.getMessage());
        }
    }

    @Test
    public void testDropSnapshotVariousFormats() {
        // Test various SQL formats
        String[] testSqls = {
            "DROP SNAPSHOT ON repo WHERE SNAPSHOT = 'backup1'",
            "DROP SNAPSHOT ON `repo` WHERE SNAPSHOT = 'backup1'",
            "drop snapshot on repo where snapshot = 'backup1'",
            "DROP SNAPSHOT ON repo WHERE TIMESTAMP <= '2024-01-01-12-00-00'",
            "DROP SNAPSHOT ON repo WHERE TIMESTAMP >= '2024-01-01-12-00-00'",
            "DROP SNAPSHOT ON repo WHERE SNAPSHOT IN ('backup1', 'backup2')",
            "DROP SNAPSHOT ON repo WHERE SNAPSHOT = 'backup1' AND TIMESTAMP <= '2024-01-01-12-00-00'"
        };
        
        for (String sql : testSqls) {
            try {
                List<StatementBase> statements = SqlParser.parse(sql, 0);
                Assert.assertEquals("Should parse exactly one statement for: " + sql, 1, statements.size());
                Assert.assertTrue("Should be DropSnapshotStmt for: " + sql, 
                                statements.get(0) instanceof DropSnapshotStmt);
                
                DropSnapshotStmt stmt = (DropSnapshotStmt) statements.get(0);
                Assert.assertNotNull("Repository name should not be null for: " + sql, stmt.getRepoName());
                
                System.out.println("✓ Parsed successfully: " + sql);
            } catch (Exception e) {
                Assert.fail("Failed to parse SQL: " + sql + " - " + e.getMessage());
            }
        }
        
        System.out.println("✓ All DROP SNAPSHOT format variations test passed");
    }

    @Test
    public void testDropSnapshotInvalidSyntax() {
        // Test that invalid syntax is properly rejected
        String[] invalidSqls = {
            "DROP SNAPSHOT ON repo",  // Missing WHERE clause
            "DROP SNAPSHOT repo WHERE SNAPSHOT = 'backup1'",  // Missing ON
            "DROP SNAPSHOT ON WHERE SNAPSHOT = 'backup1'",  // Missing repo name
            "SNAPSHOT ON repo WHERE SNAPSHOT = 'backup1'",  // Missing DROP
        };
        
        for (String sql : invalidSqls) {
            try {
                SqlParser.parse(sql, 0);
                Assert.fail("Should have failed to parse invalid SQL: " + sql);
            } catch (Exception e) {
                System.out.println("✓ Correctly rejected invalid SQL: " + sql);
            }
        }
        
        System.out.println("✓ Invalid syntax rejection test passed");
    }

    @Test
    public void testDropSnapshotVisitorPattern() {
        // Test that the visitor pattern works correctly
        String sql = "DROP SNAPSHOT ON repo WHERE SNAPSHOT = 'backup1'";
        
        try {
            List<StatementBase> statements = SqlParser.parse(sql, 0);
            DropSnapshotStmt stmt = (DropSnapshotStmt) statements.get(0);
            
            // Create a simple visitor to test the pattern
            TestVisitor visitor = new TestVisitor();
            String result = stmt.accept(visitor, null);
            
            Assert.assertEquals("Visitor should return expected result", "drop_snapshot_visited", result);
            
            System.out.println("✓ DROP SNAPSHOT visitor pattern test passed");
        } catch (Exception e) {
            Assert.fail("Failed to test visitor pattern: " + e.getMessage());
        }
    }

    // Helper visitor class for testing
    private static class TestVisitor implements com.starrocks.sql.ast.AstVisitor<String, Void> {
        @Override
        public String visitDropSnapshotStatement(DropSnapshotStmt statement, Void context) {
            return "drop_snapshot_visited";
        }
    }
}
