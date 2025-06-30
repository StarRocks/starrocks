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

import com.starrocks.analysis.BinaryPredicate;
import com.starrocks.analysis.BinaryType;
import com.starrocks.analysis.CompoundPredicate;
import com.starrocks.analysis.InPredicate;
import com.starrocks.sql.ast.DropSnapshotStmt;
import com.starrocks.sql.ast.StatementBase;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class DropSnapshotParserTest {

    @Test
    public void testParseBasicDropSnapshot() {
        String sql = "DROP SNAPSHOT ON repo WHERE SNAPSHOT = 'backup1'";
        List<StatementBase> statements = SqlParser.parse(sql, 0);
        
        Assert.assertEquals(1, statements.size());
        Assert.assertTrue(statements.get(0) instanceof DropSnapshotStmt);
        
        DropSnapshotStmt stmt = (DropSnapshotStmt) statements.get(0);
        Assert.assertEquals("repo", stmt.getRepoName());
        Assert.assertNotNull(stmt.getWhere());
    }

    @Test
    public void testParseDropSnapshotWithQuotedRepo() {
        String sql = "DROP SNAPSHOT ON `repo` WHERE SNAPSHOT = 'backup1'";
        List<StatementBase> statements = SqlParser.parse(sql, 0);
        
        Assert.assertEquals(1, statements.size());
        Assert.assertTrue(statements.get(0) instanceof DropSnapshotStmt);
        
        DropSnapshotStmt stmt = (DropSnapshotStmt) statements.get(0);
        Assert.assertEquals("repo", stmt.getRepoName());
        Assert.assertNotNull(stmt.getWhere());
    }

    @Test
    public void testParseDropSnapshotWithTimestamp() {
        String sql = "DROP SNAPSHOT ON repo WHERE TIMESTAMP <= '2024-01-01-12-00-00'";
        List<StatementBase> statements = SqlParser.parse(sql, 0);
        
        Assert.assertEquals(1, statements.size());
        Assert.assertTrue(statements.get(0) instanceof DropSnapshotStmt);
        
        DropSnapshotStmt stmt = (DropSnapshotStmt) statements.get(0);
        Assert.assertEquals("repo", stmt.getRepoName());
        Assert.assertNotNull(stmt.getWhere());
        
        // Verify the WHERE clause is a binary predicate
        Assert.assertTrue(stmt.getWhere() instanceof BinaryPredicate);
        BinaryPredicate predicate = (BinaryPredicate) stmt.getWhere();
        Assert.assertEquals(BinaryType.LE, predicate.getOp());
    }

    @Test
    public void testParseDropSnapshotWithTimestampGE() {
        String sql = "DROP SNAPSHOT ON repo WHERE TIMESTAMP >= '2024-01-01-12-00-00'";
        List<StatementBase> statements = SqlParser.parse(sql, 0);
        
        Assert.assertEquals(1, statements.size());
        Assert.assertTrue(statements.get(0) instanceof DropSnapshotStmt);
        
        DropSnapshotStmt stmt = (DropSnapshotStmt) statements.get(0);
        Assert.assertEquals("repo", stmt.getRepoName());
        Assert.assertNotNull(stmt.getWhere());
        
        // Verify the WHERE clause is a binary predicate with >= operator
        Assert.assertTrue(stmt.getWhere() instanceof BinaryPredicate);
        BinaryPredicate predicate = (BinaryPredicate) stmt.getWhere();
        Assert.assertEquals(BinaryType.GE, predicate.getOp());
    }

    @Test
    public void testParseDropSnapshotWithCompoundCondition() {
        String sql = "DROP SNAPSHOT ON repo WHERE SNAPSHOT = 'backup1' AND TIMESTAMP <= '2024-01-01-12-00-00'";
        List<StatementBase> statements = SqlParser.parse(sql, 0);
        
        Assert.assertEquals(1, statements.size());
        Assert.assertTrue(statements.get(0) instanceof DropSnapshotStmt);
        
        DropSnapshotStmt stmt = (DropSnapshotStmt) statements.get(0);
        Assert.assertEquals("repo", stmt.getRepoName());
        Assert.assertNotNull(stmt.getWhere());
        
        // Verify the WHERE clause is a compound predicate
        Assert.assertTrue(stmt.getWhere() instanceof CompoundPredicate);
        CompoundPredicate compound = (CompoundPredicate) stmt.getWhere();
        Assert.assertEquals(CompoundPredicate.Operator.AND, compound.getOp());
        
        // Verify both children are binary predicates
        Assert.assertTrue(compound.getChild(0) instanceof BinaryPredicate);
        Assert.assertTrue(compound.getChild(1) instanceof BinaryPredicate);
    }

    @Test
    public void testParseDropSnapshotWithInClause() {
        String sql = "DROP SNAPSHOT ON repo WHERE SNAPSHOT IN ('backup1', 'backup2', 'backup3')";
        List<StatementBase> statements = SqlParser.parse(sql, 0);
        
        Assert.assertEquals(1, statements.size());
        Assert.assertTrue(statements.get(0) instanceof DropSnapshotStmt);
        
        DropSnapshotStmt stmt = (DropSnapshotStmt) statements.get(0);
        Assert.assertEquals("repo", stmt.getRepoName());
        Assert.assertNotNull(stmt.getWhere());
        
        // Verify the WHERE clause is an IN predicate
        Assert.assertTrue(stmt.getWhere() instanceof InPredicate);
        InPredicate inPredicate = (InPredicate) stmt.getWhere();
        Assert.assertEquals(3, inPredicate.getInElementNum());
    }

    @Test
    public void testParseDropSnapshotCaseInsensitive() {
        // Test case insensitive keywords
        String sql1 = "drop snapshot on repo where snapshot = 'backup1'";
        List<StatementBase> statements1 = SqlParser.parse(sql1, 0);
        Assert.assertEquals(1, statements1.size());
        Assert.assertTrue(statements1.get(0) instanceof DropSnapshotStmt);
        
        String sql2 = "Drop Snapshot On repo Where Snapshot = 'backup1'";
        List<StatementBase> statements2 = SqlParser.parse(sql2, 0);
        Assert.assertEquals(1, statements2.size());
        Assert.assertTrue(statements2.get(0) instanceof DropSnapshotStmt);
        
        String sql3 = "DROP SNAPSHOT ON repo WHERE SNAPSHOT = 'backup1'";
        List<StatementBase> statements3 = SqlParser.parse(sql3, 0);
        Assert.assertEquals(1, statements3.size());
        Assert.assertTrue(statements3.get(0) instanceof DropSnapshotStmt);
    }

    @Test
    public void testParseDropSnapshotWithDifferentQuotes() {
        // Test single quotes
        String sql1 = "DROP SNAPSHOT ON repo WHERE SNAPSHOT = 'backup1'";
        List<StatementBase> statements1 = SqlParser.parse(sql1, 0);
        Assert.assertEquals(1, statements1.size());
        Assert.assertTrue(statements1.get(0) instanceof DropSnapshotStmt);
        
        // Test double quotes
        String sql2 = "DROP SNAPSHOT ON repo WHERE SNAPSHOT = \"backup1\"";
        List<StatementBase> statements2 = SqlParser.parse(sql2, 0);
        Assert.assertEquals(1, statements2.size());
        Assert.assertTrue(statements2.get(0) instanceof DropSnapshotStmt);
    }

    @Test
    public void testParseDropSnapshotWithSpecialCharacters() {
        // Test snapshot names with special characters
        String sql1 = "DROP SNAPSHOT ON repo WHERE SNAPSHOT = 'backup_with_underscore'";
        List<StatementBase> statements1 = SqlParser.parse(sql1, 0);
        Assert.assertEquals(1, statements1.size());
        Assert.assertTrue(statements1.get(0) instanceof DropSnapshotStmt);
        
        String sql2 = "DROP SNAPSHOT ON repo WHERE SNAPSHOT = 'backup-with-dash'";
        List<StatementBase> statements2 = SqlParser.parse(sql2, 0);
        Assert.assertEquals(1, statements2.size());
        Assert.assertTrue(statements2.get(0) instanceof DropSnapshotStmt);
        
        String sql3 = "DROP SNAPSHOT ON repo WHERE SNAPSHOT = 'backup.with.dots'";
        List<StatementBase> statements3 = SqlParser.parse(sql3, 0);
        Assert.assertEquals(1, statements3.size());
        Assert.assertTrue(statements3.get(0) instanceof DropSnapshotStmt);
        
        String sql4 = "DROP SNAPSHOT ON repo WHERE SNAPSHOT = 'backup with spaces'";
        List<StatementBase> statements4 = SqlParser.parse(sql4, 0);
        Assert.assertEquals(1, statements4.size());
        Assert.assertTrue(statements4.get(0) instanceof DropSnapshotStmt);
    }

    @Test
    public void testParseDropSnapshotWithNumbers() {
        // Test repository and snapshot names with numbers
        String sql1 = "DROP SNAPSHOT ON repo123 WHERE SNAPSHOT = 'backup123'";
        List<StatementBase> statements1 = SqlParser.parse(sql1, 0);
        Assert.assertEquals(1, statements1.size());
        Assert.assertTrue(statements1.get(0) instanceof DropSnapshotStmt);
        
        DropSnapshotStmt stmt1 = (DropSnapshotStmt) statements1.get(0);
        Assert.assertEquals("repo123", stmt1.getRepoName());
        
        String sql2 = "DROP SNAPSHOT ON repo WHERE SNAPSHOT = '123backup'";
        List<StatementBase> statements2 = SqlParser.parse(sql2, 0);
        Assert.assertEquals(1, statements2.size());
        Assert.assertTrue(statements2.get(0) instanceof DropSnapshotStmt);
        
        String sql3 = "DROP SNAPSHOT ON repo WHERE SNAPSHOT = '123'";
        List<StatementBase> statements3 = SqlParser.parse(sql3, 0);
        Assert.assertEquals(1, statements3.size());
        Assert.assertTrue(statements3.get(0) instanceof DropSnapshotStmt);
    }

    @Test
    public void testParseDropSnapshotToSql() {
        String originalSql = "DROP SNAPSHOT ON repo WHERE SNAPSHOT = 'backup1'";
        List<StatementBase> statements = SqlParser.parse(originalSql, 0);
        
        Assert.assertEquals(1, statements.size());
        DropSnapshotStmt stmt = (DropSnapshotStmt) statements.get(0);
        
        String generatedSql = stmt.toSql();
        Assert.assertNotNull(generatedSql);
        Assert.assertTrue(generatedSql.contains("DROP SNAPSHOT ON repo"));
        Assert.assertTrue(generatedSql.contains("WHERE"));
    }

    @Test
    public void testParseDropSnapshotWhitespace() {
        // Test various whitespace scenarios
        String sql1 = "DROP SNAPSHOT ON repo WHERE SNAPSHOT='backup1'";
        List<StatementBase> statements1 = SqlParser.parse(sql1, 0);
        Assert.assertEquals(1, statements1.size());
        Assert.assertTrue(statements1.get(0) instanceof DropSnapshotStmt);
        
        String sql2 = "DROP SNAPSHOT ON repo WHERE SNAPSHOT = 'backup1' ";
        List<StatementBase> statements2 = SqlParser.parse(sql2, 0);
        Assert.assertEquals(1, statements2.size());
        Assert.assertTrue(statements2.get(0) instanceof DropSnapshotStmt);
        
        String sql3 = "DROP SNAPSHOT ON repo  WHERE  SNAPSHOT  =  'backup1'  ";
        List<StatementBase> statements3 = SqlParser.parse(sql3, 0);
        Assert.assertEquals(1, statements3.size());
        Assert.assertTrue(statements3.get(0) instanceof DropSnapshotStmt);
    }

    @Test
    public void testParseDropSnapshotComplexTimestamp() {
        // Test various timestamp formats
        String sql1 = "DROP SNAPSHOT ON repo WHERE TIMESTAMP <= '2024-12-31-23-59-59'";
        List<StatementBase> statements1 = SqlParser.parse(sql1, 0);
        Assert.assertEquals(1, statements1.size());
        Assert.assertTrue(statements1.get(0) instanceof DropSnapshotStmt);
        
        String sql2 = "DROP SNAPSHOT ON repo WHERE TIMESTAMP >= '2020-01-01-00-00-00'";
        List<StatementBase> statements2 = SqlParser.parse(sql2, 0);
        Assert.assertEquals(1, statements2.size());
        Assert.assertTrue(statements2.get(0) instanceof DropSnapshotStmt);
    }

    @Test(expected = ParsingException.class)
    public void testParseDropSnapshotInvalidSyntax() {
        // Test invalid syntax - missing WHERE clause
        String sql = "DROP SNAPSHOT ON repo";
        SqlParser.parse(sql, 0);
    }

    @Test
    public void testParseDropSnapshotValidSyntaxInvalidSemantics() {
        // Test that invalid operators parse successfully (semantic validation happens later)
        String sql = "DROP SNAPSHOT ON repo WHERE SNAPSHOT > 'backup1'";
        StatementBase stmt = SqlParser.parse(sql, 0).get(0);
        Assert.assertTrue(stmt instanceof DropSnapshotStmt);

        // Test LIKE operator also parses successfully
        sql = "DROP SNAPSHOT ON repo WHERE SNAPSHOT LIKE 'backup%'";
        stmt = SqlParser.parse(sql, 0).get(0);
        Assert.assertTrue(stmt instanceof DropSnapshotStmt);
    }
}
