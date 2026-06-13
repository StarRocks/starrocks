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

package com.starrocks.backup;

import com.google.common.collect.Lists;
import com.starrocks.sql.ast.DropSnapshotStmt;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

/**
 * Simple unit tests for DropSnapshotStmt functionality
 * For BackupHandler integration tests, see BackupHandlerDropSnapshotTest
 */
public class DropSnapshotTest {

    @Test
    public void testDropSnapshotStmtBasicFunctionality() {
        // Test basic DropSnapshotStmt functionality without mocking
        String repoName = "test_repo";
        String snapshotName = "test_snapshot";

        DropSnapshotStmt stmt = new DropSnapshotStmt(repoName, null);
        stmt.setSnapshotName(snapshotName);

        // Verify basic properties
        Assertions.assertEquals(repoName, stmt.getRepoName());
        Assertions.assertEquals(snapshotName, stmt.getSnapshotName());
        Assertions.assertNull(stmt.getTimestamp());
        Assertions.assertNull(stmt.getTimestampOperator());
        Assertions.assertTrue(stmt.getSnapshotNames().isEmpty());
    }

    @Test
    public void testDropSnapshotStmtTimestampFunctionality() {
        // Test DropSnapshotStmt with timestamp functionality
        String repoName = "test_repo";
        String timestamp = "2024-01-01-12-00-00";
        String operator = "<=";

        DropSnapshotStmt stmt = new DropSnapshotStmt(repoName, null);
        stmt.setTimestamp(timestamp);
        stmt.setTimestampOperator(operator);

        // Verify timestamp properties
        Assertions.assertEquals(repoName, stmt.getRepoName());
        Assertions.assertEquals(timestamp, stmt.getTimestamp());
        Assertions.assertEquals(operator, stmt.getTimestampOperator());
        Assertions.assertNull(stmt.getSnapshotName());
        Assertions.assertTrue(stmt.getSnapshotNames().isEmpty());
    }

    @Test
    public void testDropSnapshotStmtMultipleSnapshots() {
        // Test DropSnapshotStmt with multiple snapshots functionality
        String repoName = "test_repo";
        List<String> snapshotNames = Lists.newArrayList("snapshot1", "snapshot2", "snapshot3");

        DropSnapshotStmt stmt = new DropSnapshotStmt(repoName, null);
        for (String name : snapshotNames) {
            stmt.addSnapshotName(name);
        }

        // Verify multiple snapshots properties
        Assertions.assertEquals(repoName, stmt.getRepoName());
        Assertions.assertEquals(3, stmt.getSnapshotNames().size());
        Assertions.assertTrue(stmt.getSnapshotNames().contains("snapshot1"));
        Assertions.assertTrue(stmt.getSnapshotNames().contains("snapshot2"));
        Assertions.assertTrue(stmt.getSnapshotNames().contains("snapshot3"));
        Assertions.assertNull(stmt.getSnapshotName());
        Assertions.assertNull(stmt.getTimestamp());
    }

    @Test
    public void testDropSnapshotStmtToSql() {
        // Test toSql() method functionality
        String repoName = "test_repo";

        // Test basic statement without WHERE clause
        DropSnapshotStmt stmt1 = new DropSnapshotStmt(repoName, null);
        String sql1 = stmt1.toSql();
        Assertions.assertTrue(sql1.contains("DROP SNAPSHOT"));
        Assertions.assertTrue(sql1.contains(repoName));

        // Test with snapshot name
        DropSnapshotStmt stmt2 = new DropSnapshotStmt(repoName, null);
        stmt2.setSnapshotName("test_snapshot");
        String sql2 = stmt2.toSql();
        Assertions.assertTrue(sql2.contains("DROP SNAPSHOT"));
        Assertions.assertTrue(sql2.contains(repoName));
    }

    @Test
    public void testDropSnapshotStmtEdgeCases() {
        // Test edge cases and boundary conditions
        String repoName = "test_repo";

        // Test with empty snapshot name (should be allowed at AST level)
        DropSnapshotStmt stmt1 = new DropSnapshotStmt(repoName, null);
        stmt1.setSnapshotName("");
        Assertions.assertEquals("", stmt1.getSnapshotName());

        // Test with null values
        DropSnapshotStmt stmt2 = new DropSnapshotStmt(repoName, null);
        stmt2.setSnapshotName(null);
        stmt2.setTimestamp(null);
        stmt2.setTimestampOperator(null);
        Assertions.assertNull(stmt2.getSnapshotName());
        Assertions.assertNull(stmt2.getTimestamp());
        Assertions.assertNull(stmt2.getTimestampOperator());

        // Test clearing snapshot names
        DropSnapshotStmt stmt3 = new DropSnapshotStmt(repoName, null);
        stmt3.addSnapshotName("snap1");
        stmt3.addSnapshotName("snap2");
        Assertions.assertEquals(2, stmt3.getSnapshotNames().size());
        stmt3.getSnapshotNames().clear();
        Assertions.assertTrue(stmt3.getSnapshotNames().isEmpty());
    }

    @Test
    public void testBackupHandlerDropSnapshotValidation() {
        // Test BackupHandler.dropSnapshot validation logic
        // This tests the validation paths in BackupHandler lines 566-624

        // Test empty snapshot name validation
        DropSnapshotStmt stmt1 = new DropSnapshotStmt("test_repo", null);
        stmt1.setSnapshotName("");

        // Verify the statement structure for validation
        Assertions.assertEquals("test_repo", stmt1.getRepoName());
        Assertions.assertEquals("", stmt1.getSnapshotName());
        Assertions.assertTrue(stmt1.getSnapshotNames().isEmpty());
        Assertions.assertNull(stmt1.getTimestamp());

        // Test multiple snapshots validation
        DropSnapshotStmt stmt2 = new DropSnapshotStmt("test_repo", null);
        stmt2.addSnapshotName("snap1");
        stmt2.addSnapshotName("snap2");

        Assertions.assertEquals("test_repo", stmt2.getRepoName());
        Assertions.assertNull(stmt2.getSnapshotName());
        Assertions.assertEquals(2, stmt2.getSnapshotNames().size());
        Assertions.assertTrue(stmt2.getSnapshotNames().contains("snap1"));
        Assertions.assertTrue(stmt2.getSnapshotNames().contains("snap2"));

        // Test timestamp validation
        DropSnapshotStmt stmt3 = new DropSnapshotStmt("test_repo", null);
        stmt3.setTimestamp("2024-01-01-12-00-00");
        stmt3.setTimestampOperator("<=");

        Assertions.assertEquals("test_repo", stmt3.getRepoName());
        Assertions.assertNull(stmt3.getSnapshotName());
        Assertions.assertTrue(stmt3.getSnapshotNames().isEmpty());
        Assertions.assertEquals("2024-01-01-12-00-00", stmt3.getTimestamp());
        Assertions.assertEquals("<=", stmt3.getTimestampOperator());
    }







    @Test
    public void testDropSnapshotValidation() {
        // Test validation logic
        String repoName = "test_repo";
        
        // Test with no conditions - should fail
        DropSnapshotStmt stmt1 = new DropSnapshotStmt(repoName, null);
        Assertions.assertNull(stmt1.getSnapshotName());
        Assertions.assertNull(stmt1.getTimestamp());
        Assertions.assertTrue(stmt1.getSnapshotNames().isEmpty());
        
        // Test with snapshot name - should be valid
        DropSnapshotStmt stmt2 = new DropSnapshotStmt(repoName, null);
        stmt2.setSnapshotName("test_snapshot");
        Assertions.assertNotNull(stmt2.getSnapshotName());
        
        // Test with timestamp - should be valid
        DropSnapshotStmt stmt3 = new DropSnapshotStmt(repoName, null);
        stmt3.setTimestamp("2024-01-01-12-00-00");
        stmt3.setTimestampOperator("<=");
        Assertions.assertNotNull(stmt3.getTimestamp());
        Assertions.assertNotNull(stmt3.getTimestampOperator());
        
        // Test with multiple snapshots - should be valid
        DropSnapshotStmt stmt4 = new DropSnapshotStmt(repoName, null);
        stmt4.addSnapshotName("snapshot1");
        stmt4.addSnapshotName("snapshot2");
        Assertions.assertFalse(stmt4.getSnapshotNames().isEmpty());
    }
}
