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

package com.starrocks.epack.qe;

import com.starrocks.epack.sql.ast.DescribeFailoverGroupStmt;
import com.starrocks.epack.sql.ast.PolicyName;
import com.starrocks.epack.sql.ast.PolicyType;
import com.starrocks.epack.sql.ast.ShowCreatePasswordPolicyStmt;
import com.starrocks.epack.sql.ast.ShowCreatePolicyStmt;
import com.starrocks.epack.sql.ast.ShowFailoverGroupsStmt;
import com.starrocks.epack.sql.ast.ShowPasswordPolicyStmt;
import com.starrocks.epack.sql.ast.ShowPolicyStmt;
import com.starrocks.epack.sql.ast.ShowRoleMappingStatement;
import com.starrocks.qe.ShowResultMetaFactory;
import com.starrocks.qe.ShowResultSetMetaData;
import com.starrocks.sql.parser.NodePosition;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Test for enterprise pack ShowStmt subclasses metadata
 */
public class ShowStmtMetaTest {

    @Test
    public void testShowFailoverGroupsStmt() {
        ShowFailoverGroupsStmt stmt = new ShowFailoverGroupsStmt("test_pattern", NodePosition.ZERO);
        ShowResultSetMetaData metaData = new ShowResultMetaFactory().getMetadata(stmt);
        Assertions.assertEquals(13, metaData.getColumnCount());
        Assertions.assertEquals("Id", metaData.getColumn(0).getName());
        Assertions.assertEquals("Name", metaData.getColumn(1).getName());
        Assertions.assertEquals("Role", metaData.getColumn(2).getName());
        Assertions.assertEquals("State", metaData.getColumn(3).getName());
        Assertions.assertEquals("Schedule", metaData.getColumn(4).getName());
        Assertions.assertEquals("IsSuspended", metaData.getColumn(5).getName());
        Assertions.assertEquals("ScheduledTime", metaData.getColumn(6).getName());
        Assertions.assertEquals("FinishedTime", metaData.getColumn(7).getName());
        Assertions.assertEquals("FinishedRound", metaData.getColumn(8).getName());
        Assertions.assertEquals("ReplicatedJournalId", metaData.getColumn(9).getName());
        Assertions.assertEquals("LastScheduledTime", metaData.getColumn(10).getName());
        Assertions.assertEquals("LastFinishedTime", metaData.getColumn(11).getName());
        Assertions.assertEquals("Errors", metaData.getColumn(12).getName());
    }

    @Test
    public void testShowPasswordPolicyStmt() {
        ShowPasswordPolicyStmt stmt = new ShowPasswordPolicyStmt(NodePosition.ZERO);
        ShowResultSetMetaData metaData = new ShowResultMetaFactory().getMetadata(stmt);
        Assertions.assertEquals(4, metaData.getColumnCount());
        Assertions.assertEquals("Policy", metaData.getColumn(0).getName());
        Assertions.assertEquals("Comment", metaData.getColumn(1).getName());
        Assertions.assertEquals("Properties", metaData.getColumn(2).getName());
        Assertions.assertEquals("IS_SYSTEM_DEFAULT_POLICY", metaData.getColumn(3).getName());
    }

    @Test
    public void testShowCreatePasswordPolicyStmt() {
        ShowCreatePasswordPolicyStmt stmt = new ShowCreatePasswordPolicyStmt("test_policy", NodePosition.ZERO);
        ShowResultSetMetaData metaData = new ShowResultMetaFactory().getMetadata(stmt);
        Assertions.assertEquals(2, metaData.getColumnCount());
        Assertions.assertEquals("Policy", metaData.getColumn(0).getName());
        Assertions.assertEquals("Create Policy", metaData.getColumn(1).getName());
    }

    @Test
    public void testShowPolicyStmt() {
        ShowPolicyStmt stmt = new ShowPolicyStmt("test_catalog", "test_db", PolicyType.MASKING, NodePosition.ZERO);
        ShowResultSetMetaData metaData = new ShowResultMetaFactory().getMetadata(stmt);
        Assertions.assertEquals(4, metaData.getColumnCount());
        Assertions.assertEquals("Name", metaData.getColumn(0).getName());
        Assertions.assertEquals("Type", metaData.getColumn(1).getName());
        Assertions.assertEquals("Catalog", metaData.getColumn(2).getName());
        Assertions.assertEquals("Database", metaData.getColumn(3).getName());
    }

    @Test
    public void testShowRoleMappingStatement() {
        ShowRoleMappingStatement stmt = new ShowRoleMappingStatement(NodePosition.ZERO);
        ShowResultSetMetaData metaData = new ShowResultMetaFactory().getMetadata(stmt);
        Assertions.assertEquals(5, metaData.getColumnCount());
        Assertions.assertEquals("Name", metaData.getColumn(0).getName());
        Assertions.assertEquals("IntegrationName", metaData.getColumn(1).getName());
        Assertions.assertEquals("Role", metaData.getColumn(2).getName());
        Assertions.assertEquals("LdapGroupList", metaData.getColumn(3).getName());
        Assertions.assertEquals("LastRefreshCompleteTime", metaData.getColumn(4).getName());
    }

    @Test
    public void testShowCreatePolicyStmt() {
        PolicyName policyName = new PolicyName("test_catalog", "test_db", "test_policy", NodePosition.ZERO);
        ShowCreatePolicyStmt stmt = new ShowCreatePolicyStmt(PolicyType.MASKING, policyName, NodePosition.ZERO);
        ShowResultSetMetaData metaData = new ShowResultMetaFactory().getMetadata(stmt);
        Assertions.assertEquals(2, metaData.getColumnCount());
        Assertions.assertEquals("Policy", metaData.getColumn(0).getName());
        Assertions.assertEquals("Create Policy", metaData.getColumn(1).getName());
    }

    @Test
    public void testDescribeFailoverGroupStmt() {
        DescribeFailoverGroupStmt stmt = new DescribeFailoverGroupStmt("test_group", NodePosition.ZERO);
        ShowResultSetMetaData metaData = new ShowResultMetaFactory().getMetadata(stmt);
        Assertions.assertEquals(8, metaData.getColumnCount());
        Assertions.assertEquals("Id", metaData.getColumn(0).getName());
        Assertions.assertEquals("Name", metaData.getColumn(1).getName());
        Assertions.assertEquals("Include Tables", metaData.getColumn(2).getName());
        Assertions.assertEquals("Exclude Tables", metaData.getColumn(3).getName());
        Assertions.assertEquals("Members", metaData.getColumn(4).getName());
        Assertions.assertEquals("Schedule", metaData.getColumn(5).getName());
        Assertions.assertEquals("Comment", metaData.getColumn(6).getName());
        Assertions.assertEquals("Properties", metaData.getColumn(7).getName());
    }
}
