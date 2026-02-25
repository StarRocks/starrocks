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

public class AddBackendClauseTest {

    @Test
    public void testAddBackendIntoWarehouse() {
        String sqlText = "ALTER SYSTEM ADD BACKEND 'backend01:9010' INTO WAREHOUSE warehouse1";
        AlterSystemStmt stmt =
                (AlterSystemStmt) SqlParser.parseSingleStatement(sqlText, SqlModeHelper.MODE_DEFAULT);
        AddBackendClause addStmt = (AddBackendClause) stmt.getAlterClause();
        Assertions.assertEquals("warehouse1", addStmt.getWarehouse());
        Assertions.assertTrue(addStmt.getCNGroupName().isEmpty());
    }

    @Test
    public void testAddBackendIntoWarehouseCnGroup() {
        String sqlText = "ALTER SYSTEM ADD BACKEND 'backend01:9010' INTO WAREHOUSE warehouse1 CNGROUP cngroup1";
        AlterSystemStmt stmt =
                (AlterSystemStmt) SqlParser.parseSingleStatement(sqlText, SqlModeHelper.MODE_DEFAULT);
        AddBackendClause addStmt = (AddBackendClause) stmt.getAlterClause();
        Assertions.assertEquals("warehouse1", addStmt.getWarehouse());
        Assertions.assertEquals("cngroup1", addStmt.getCNGroupName());
    }

    @Test
    public void testAddBackendIntoWarehouseCnGroupBadStatement() {
        {
            String sqlText = "ALTER SYSTEM ADD BACKEND 'backend01:9010' INTO WAREHOUSE warehouse1 CNGROUP";
            Assertions.assertThrows(ParsingException.class,
                    () -> SqlParser.parseSingleStatement(sqlText, SqlModeHelper.MODE_DEFAULT));
        }
        {
            String sqlText = "ALTER SYSTEM ADD BACKEND 'backend01:9010' INTO WAREHOUSE warehouse1 cngroup1";
            Assertions.assertThrows(ParsingException.class,
                    () -> SqlParser.parseSingleStatement(sqlText, SqlModeHelper.MODE_DEFAULT));
        }
    }
}
