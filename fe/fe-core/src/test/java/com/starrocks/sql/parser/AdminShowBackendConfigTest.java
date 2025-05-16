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

import com.starrocks.sql.ast.AdminShowBackendConfigStmt;
import com.starrocks.sql.ast.AdminSetConfigStmt.ConfigType;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.Test;

public class AdminShowBackendConfigTest {
    
    @Test
    public void testParse() throws Exception {
        // Test basic command
        String sql = "ADMIN SHOW BACKEND CONFIG";
        AdminShowBackendConfigStmt stmt = (AdminShowBackendConfigStmt) UtFrameUtils.parseStmtWithNewParser(sql, null);
        Assert.assertEquals(ConfigType.BACKEND, stmt.getType());
        Assert.assertNull(stmt.getPattern());
        
        // Test with LIKE pattern
        sql = "ADMIN SHOW BACKEND CONFIG LIKE 'memory'";
        stmt = (AdminShowBackendConfigStmt) UtFrameUtils.parseStmtWithNewParser(sql, null);
        Assert.assertEquals(ConfigType.BACKEND, stmt.getType());
        Assert.assertEquals("memory", stmt.getPattern());
    }
    
    @Test
    public void testMetadata() {
        // Test metadata
        AdminShowBackendConfigStmt stmt = new AdminShowBackendConfigStmt(ConfigType.BACKEND, null);
        Assert.assertEquals(5, stmt.getMetaData().getColumnCount());
        Assert.assertEquals("Host", stmt.getMetaData().getColumn(0).getName());
        Assert.assertEquals("Key", stmt.getMetaData().getColumn(1).getName());
        Assert.assertEquals("Value", stmt.getMetaData().getColumn(2).getName());
        Assert.assertEquals("Type", stmt.getMetaData().getColumn(3).getName());
        Assert.assertEquals("IsMutable", stmt.getMetaData().getColumn(4).getName());
    }
} 