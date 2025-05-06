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

package com.starrocks.sql.ast.warehouse.cngroup;

import com.starrocks.qe.SqlModeHelper;
import com.starrocks.sql.parser.ParsingException;
import com.starrocks.sql.parser.SqlParser;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class AlterCnGroupStmtTest {

    @Test
    public void testAlterCnGroupStatement() {
        {
            String sqlText =
                    "ALTER WAREHOUSE warehouse1 MODIFY CNGROUP cngroup1 SET('location' = 'a', 'rack' = 'b')";
            AlterCnGroupStmt stmt =
                    (AlterCnGroupStmt) SqlParser.parseSingleStatement(sqlText, SqlModeHelper.MODE_DEFAULT);
            Assert.assertEquals("warehouse1", stmt.getWarehouseName());
            Assert.assertEquals("cngroup1", stmt.getCnGroupName());
            Map<String, String> properties = new HashMap<>();
            properties.put("location", "a");
            properties.put("rack", "b");
            Assert.assertEquals(properties, stmt.getProperties());
        }

        {
            String sqlText = "ALTER WAREHOUSE warehouse1 MODIFY CNGROUP cngroup1";
            Assert.assertThrows(ParsingException.class,
                    () -> SqlParser.parseSingleStatement(sqlText, SqlModeHelper.MODE_DEFAULT));
        }
    }
}
