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

import com.starrocks.qe.SqlModeHelper;
import com.starrocks.sql.parser.ParsingException;
import com.starrocks.sql.parser.SqlParser;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class DropComputeNodeClauseTest {

    @Test
    public void testDropComputeNodeIntoWarehouse() {
        String sqlText = "ALTER SYSTEM DROP COMPUTE NODE 'backend01:9010' FROM WAREHOUSE warehouse1";
        AlterSystemStmt stmt =
                (AlterSystemStmt) SqlParser.parseSingleStatement(sqlText, SqlModeHelper.MODE_DEFAULT);
        DropComputeNodeClause dropStmt = (DropComputeNodeClause) stmt.getAlterClause();
        Assertions.assertEquals("warehouse1", dropStmt.getWarehouse());
        Assertions.assertTrue(dropStmt.getCNGroupName().isEmpty());
    }
    @Test
    public void testDropComputeNodeWithoutWarehouse() {
        String sqlText = "ALTER SYSTEM DROP COMPUTE NODE 'backend01:9010'";
        AlterSystemStmt stmt =
                (AlterSystemStmt) SqlParser.parseSingleStatement(sqlText, SqlModeHelper.MODE_DEFAULT);
        DropComputeNodeClause dropStmt = (DropComputeNodeClause) stmt.getAlterClause();
        Assertions.assertTrue(dropStmt.getWarehouse().isEmpty());
        Assertions.assertTrue(dropStmt.getCNGroupName().isEmpty());
    }

    @Test
    public void testDropComputeNodeIntoWarehouseCnGroup() {
        String sqlText = "ALTER SYSTEM DROP COMPUTE NODE 'backend01:9010' FROM WAREHOUSE warehouse1 CNGROUP cngroup1";
        AlterSystemStmt stmt =
                (AlterSystemStmt) SqlParser.parseSingleStatement(sqlText, SqlModeHelper.MODE_DEFAULT);
        DropComputeNodeClause dropStmt = (DropComputeNodeClause) stmt.getAlterClause();
        Assertions.assertEquals("warehouse1", dropStmt.getWarehouse());
        Assertions.assertEquals("cngroup1", dropStmt.getCNGroupName());
    }

    @Test
    public void testDropComputeNodeIntoWarehouseCnGroupBadStatement() {
        {
            String sqlText = "ALTER SYSTEM DROP COMPUTE NODE 'backend01:9010' FROM WAREHOUSE warehouse1 CNGROUP";
            Assertions.assertThrows(ParsingException.class,
                    () -> SqlParser.parseSingleStatement(sqlText, SqlModeHelper.MODE_DEFAULT));
        }
        {
            String sqlText = "ALTER SYSTEM DROP COMPUTE NODE 'backend01:9010' FROM WAREHOUSE warehouse1 cngroup1";
            Assertions.assertThrows(ParsingException.class,
                    () -> SqlParser.parseSingleStatement(sqlText, SqlModeHelper.MODE_DEFAULT));
        }
    }
}
