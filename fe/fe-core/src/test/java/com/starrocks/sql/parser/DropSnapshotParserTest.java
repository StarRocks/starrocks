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

import com.starrocks.sql.ast.DropSnapshotStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.expression.BinaryPredicate;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.ast.expression.CompoundPredicate;
import com.starrocks.sql.ast.expression.InPredicate;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

class DropSnapshotParserTest {

    @Test
    void testParseBasicDropSnapshot() {
        String sql = "DROP SNAPSHOT ON repo WHERE SNAPSHOT = 'backup1'";
        List<StatementBase> statements = SqlParser.parse(sql, 0);
        
        Assertions.assertEquals(1, statements.size());
        Assertions.assertTrue(statements.get(0) instanceof DropSnapshotStmt);
        
        DropSnapshotStmt stmt = (DropSnapshotStmt) statements.get(0);
        Assertions.assertEquals("repo", stmt.getRepoName());
        Assertions.assertNotNull(stmt.getWhere());
    }

    @Test
    void testParseDropSnapshotWithQuotedRepo() {
        String sql = "DROP SNAPSHOT ON `repo` WHERE SNAPSHOT = 'backup1'";
        List<StatementBase> statements = SqlParser.parse(sql, 0);
        
        Assertions.assertEquals(1, statements.size());
        Assertions.assertTrue(statements.get(0) instanceof DropSnapshotStmt);
        
        DropSnapshotStmt stmt = (DropSnapshotStmt) statements.get(0);
        Assertions.assertEquals("repo", stmt.getRepoName());
        Assertions.assertNotNull(stmt.getWhere());
    }

    @Test
    void testParseDropSnapshotWithTimestamp() {
        String sql = "DROP SNAPSHOT ON repo WHERE TIMESTAMP <= '2024-01-01-12-00-00'";
        List<StatementBase> statements = SqlParser.parse(sql, 0);
        
        Assertions.assertEquals(1, statements.size());
        Assertions.assertTrue(statements.get(0) instanceof DropSnapshotStmt);
        
        DropSnapshotStmt stmt = (DropSnapshotStmt) statements.get(0);
        Assertions.assertEquals("repo", stmt.getRepoName());
        Assertions.assertNotNull(stmt.getWhere());
        
        // Verify the WHERE clause is a binary predicate
        Assertions.assertTrue(stmt.getWhere() instanceof BinaryPredicate);
        BinaryPredicate predicate = (BinaryPredicate) stmt.getWhere();
        Assertions.assertEquals(BinaryType.LE, predicate.getOp());
    }

    @Test
    void testParseDropSnapshotWithTimestampGE() {
        String sql = "DROP SNAPSHOT ON repo WHERE TIMESTAMP >= '2024-01-01-12-00-00'";
        List<StatementBase> statements = SqlParser.parse(sql, 0);
        
        Assertions.assertEquals(1, statements.size());
        Assertions.assertTrue(statements.get(0) instanceof DropSnapshotStmt);
        
        DropSnapshotStmt stmt = (DropSnapshotStmt) statements.get(0);
        Assertions.assertEquals("repo", stmt.getRepoName());
        Assertions.assertNotNull(stmt.getWhere());
        
        // Verify the WHERE clause is a binary predicate with >= operator
        Assertions.assertTrue(stmt.getWhere() instanceof BinaryPredicate);
        BinaryPredicate predicate = (BinaryPredicate) stmt.getWhere();
        Assertions.assertEquals(BinaryType.GE, predicate.getOp());
    }

    @Test
    void testParseDropSnapshotWithCompoundCondition() {
        String sql = "DROP SNAPSHOT ON repo WHERE SNAPSHOT = 'backup1' AND TIMESTAMP <= '2024-01-01-12-00-00'";
        List<StatementBase> statements = SqlParser.parse(sql, 0);
        
        Assertions.assertEquals(1, statements.size());
        Assertions.assertTrue(statements.get(0) instanceof DropSnapshotStmt);
        
        DropSnapshotStmt stmt = (DropSnapshotStmt) statements.get(0);
        Assertions.assertEquals("repo", stmt.getRepoName());
        Assertions.assertNotNull(stmt.getWhere());
        
        // Verify the WHERE clause is a compound predicate
        Assertions.assertTrue(stmt.getWhere() instanceof CompoundPredicate);
        CompoundPredicate compound = (CompoundPredicate) stmt.getWhere();
        Assertions.assertEquals(CompoundPredicate.Operator.AND, compound.getOp());
        
        // Verify both children are binary predicates
        Assertions.assertTrue(compound.getChild(0) instanceof BinaryPredicate);
        Assertions.assertTrue(compound.getChild(1) instanceof BinaryPredicate);
    }

    @Test
    void testParseDropSnapshotWithInClause() {
        String sql = "DROP SNAPSHOT ON repo WHERE SNAPSHOT IN ('backup1', 'backup2', 'backup3')";
        List<StatementBase> statements = SqlParser.parse(sql, 0);
        
        Assertions.assertEquals(1, statements.size());
        Assertions.assertTrue(statements.get(0) instanceof DropSnapshotStmt);
        
        DropSnapshotStmt stmt = (DropSnapshotStmt) statements.get(0);
        Assertions.assertEquals("repo", stmt.getRepoName());
        Assertions.assertNotNull(stmt.getWhere());
        
        // Verify the WHERE clause is an IN predicate
        Assertions.assertTrue(stmt.getWhere() instanceof InPredicate);
        InPredicate inPredicate = (InPredicate) stmt.getWhere();
        Assertions.assertEquals(3, inPredicate.getInElementNum());
    }

    @Test
    void testParseDropSnapshotCaseInsensitive() {
        // Test case insensitive keywords
        String sql1 = "drop snapshot on repo where snapshot = 'backup1'";
        List<StatementBase> statements1 = SqlParser.parse(sql1, 0);
        Assertions.assertEquals(1, statements1.size());
        Assertions.assertTrue(statements1.get(0) instanceof DropSnapshotStmt);
        
        String sql2 = "Drop Snapshot On repo Where Snapshot = 'backup1'";
        List<StatementBase> statements2 = SqlParser.parse(sql2, 0);
        Assertions.assertEquals(1, statements2.size());
        Assertions.assertTrue(statements2.get(0) instanceof DropSnapshotStmt);
        
        String sql3 = "DROP SNAPSHOT ON repo WHERE SNAPSHOT = 'backup1'";
        List<StatementBase> statements3 = SqlParser.parse(sql3, 0);
        Assertions.assertEquals(1, statements3.size());
        Assertions.assertTrue(statements3.get(0) instanceof DropSnapshotStmt);
    }

    @Test
    void testParseDropSnapshotWithDifferentQuotes() {
        // Test single quotes
        String sql1 = "DROP SNAPSHOT ON repo WHERE SNAPSHOT = 'backup1'";
        List<StatementBase> statements1 = SqlParser.parse(sql1, 0);
        Assertions.assertEquals(1, statements1.size());
        Assertions.assertTrue(statements1.get(0) instanceof DropSnapshotStmt);
        
        // Test double quotes
        String sql2 = "DROP SNAPSHOT ON repo WHERE SNAPSHOT = \"backup1\"";
        List<StatementBase> statements2 = SqlParser.parse(sql2, 0);
        Assertions.assertEquals(1, statements2.size());
        Assertions.assertTrue(statements2.get(0) instanceof DropSnapshotStmt);
    }

    @Test
    void testParseDropSnapshotWithSpecialCharacters() {
        // Test snapshot names with special characters
        String sql1 = "DROP SNAPSHOT ON repo WHERE SNAPSHOT = 'backup_with_underscore'";
        List<StatementBase> statements1 = SqlParser.parse(sql1, 0);
        Assertions.assertEquals(1, statements1.size());
        Assertions.assertTrue(statements1.get(0) instanceof DropSnapshotStmt);
        
        String sql2 = "DROP SNAPSHOT ON repo WHERE SNAPSHOT = 'backup-with-dash'";
        List<StatementBase> statements2 = SqlParser.parse(sql2, 0);
        Assertions.assertEquals(1, statements2.size());
        Assertions.assertTrue(statements2.get(0) instanceof DropSnapshotStmt);
        
        String sql3 = "DROP SNAPSHOT ON repo WHERE SNAPSHOT = 'backup.with.dots'";
        List<StatementBase> statements3 = SqlParser.parse(sql3, 0);
        Assertions.assertEquals(1, statements3.size());
        Assertions.assertTrue(statements3.get(0) instanceof DropSnapshotStmt);
        
        String sql4 = "DROP SNAPSHOT ON repo WHERE SNAPSHOT = 'backup with spaces'";
        List<StatementBase> statements4 = SqlParser.parse(sql4, 0);
        Assertions.assertEquals(1, statements4.size());
        Assertions.assertTrue(statements4.get(0) instanceof DropSnapshotStmt);
    }

    @Test
    void testParseDropSnapshotWithNumbers() {
        // Test repository and snapshot names with numbers
        String sql1 = "DROP SNAPSHOT ON repo123 WHERE SNAPSHOT = 'backup123'";
        List<StatementBase> statements1 = SqlParser.parse(sql1, 0);
        Assertions.assertEquals(1, statements1.size());
        Assertions.assertTrue(statements1.get(0) instanceof DropSnapshotStmt);
        
        DropSnapshotStmt stmt1 = (DropSnapshotStmt) statements1.get(0);
        Assertions.assertEquals("repo123", stmt1.getRepoName());
        
        String sql2 = "DROP SNAPSHOT ON repo WHERE SNAPSHOT = '123backup'";
        List<StatementBase> statements2 = SqlParser.parse(sql2, 0);
        Assertions.assertEquals(1, statements2.size());
        Assertions.assertTrue(statements2.get(0) instanceof DropSnapshotStmt);
        
        String sql3 = "DROP SNAPSHOT ON repo WHERE SNAPSHOT = '123'";
        List<StatementBase> statements3 = SqlParser.parse(sql3, 0);
        Assertions.assertEquals(1, statements3.size());
        Assertions.assertTrue(statements3.get(0) instanceof DropSnapshotStmt);
    }

    @Test
    void testParseDropSnapshotToSql() {
        String originalSql = "DROP SNAPSHOT ON repo WHERE SNAPSHOT = 'backup1'";
        List<StatementBase> statements = SqlParser.parse(originalSql, 0);
        
        Assertions.assertEquals(1, statements.size());
        DropSnapshotStmt stmt = (DropSnapshotStmt) statements.get(0);
        
        String generatedSql = stmt.toSql();
        Assertions.assertNotNull(generatedSql);
        Assertions.assertTrue(generatedSql.contains("DROP SNAPSHOT ON repo"));
        Assertions.assertTrue(generatedSql.contains("WHERE"));
    }

    @Test
    void testParseDropSnapshotWhitespace() {
        // Test various whitespace scenarios
        String sql1 = "DROP SNAPSHOT ON repo WHERE SNAPSHOT='backup1'";
        List<StatementBase> statements1 = SqlParser.parse(sql1, 0);
        Assertions.assertEquals(1, statements1.size());
        Assertions.assertTrue(statements1.get(0) instanceof DropSnapshotStmt);
        
        String sql2 = "DROP SNAPSHOT ON repo WHERE SNAPSHOT = 'backup1' ";
        List<StatementBase> statements2 = SqlParser.parse(sql2, 0);
        Assertions.assertEquals(1, statements2.size());
        Assertions.assertTrue(statements2.get(0) instanceof DropSnapshotStmt);
        
        String sql3 = "DROP SNAPSHOT ON repo  WHERE  SNAPSHOT  =  'backup1'  ";
        List<StatementBase> statements3 = SqlParser.parse(sql3, 0);
        Assertions.assertEquals(1, statements3.size());
        Assertions.assertTrue(statements3.get(0) instanceof DropSnapshotStmt);
    }

    @Test
    void testParseDropSnapshotComplexTimestamp() {
        // Test various timestamp formats
        String sql1 = "DROP SNAPSHOT ON repo WHERE TIMESTAMP <= '2024-12-31-23-59-59'";
        List<StatementBase> statements1 = SqlParser.parse(sql1, 0);
        Assertions.assertEquals(1, statements1.size());
        Assertions.assertTrue(statements1.get(0) instanceof DropSnapshotStmt);
        
        String sql2 = "DROP SNAPSHOT ON repo WHERE TIMESTAMP >= '2020-01-01-00-00-00'";
        List<StatementBase> statements2 = SqlParser.parse(sql2, 0);
        Assertions.assertEquals(1, statements2.size());
        Assertions.assertTrue(statements2.get(0) instanceof DropSnapshotStmt);
    }

    @Test
    void testParseDropSnapshotInvalidSyntax() {
        // Test invalid syntax - missing WHERE clause
        String sql = "DROP SNAPSHOT ON repo";

        Assertions.assertThrows(ParsingException.class, () -> SqlParser.parse(sql, 0));
    }

    @Test
    void testParseDropSnapshotValidSyntaxInvalidSemantics() {
        // Test that invalid operators parse successfully (semantic validation happens later)
        String sql = "DROP SNAPSHOT ON repo WHERE SNAPSHOT > 'backup1'";
        StatementBase stmt = SqlParser.parse(sql, 0).get(0);
        Assertions.assertTrue(stmt instanceof DropSnapshotStmt);

        // Test LIKE operator also parses successfully
        sql = "DROP SNAPSHOT ON repo WHERE SNAPSHOT LIKE 'backup%'";
        stmt = SqlParser.parse(sql, 0).get(0);
        Assertions.assertTrue(stmt instanceof DropSnapshotStmt);
    }
}
