// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/analysis/ShowEnginesStmtTest.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.analysis;

import com.starrocks.sql.parser.SqlParser;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.Analyzer;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Test;

public class ShowEnginesStmtTest {
    @Mocked
    private ConnectContext ctx;

    @Test
    public void testNormal() {
        {
            ShowEnginesStmt stmt = new ShowEnginesStmt();
            Analyzer.analyze(stmt, ctx);
            Assert.assertEquals("SHOW ENGINES", stmt.toString());
            Assert.assertNull(stmt.getPattern());
            Assert.assertNull(stmt.getWhere());
        }
        {
            ShowEnginesStmt stmt = new ShowEnginesStmt("OLAP");
            Analyzer.analyze(stmt, ctx);
            Assert.assertEquals("SHOW ENGINES LIKE 'OLAP'", stmt.toString());
            Assert.assertEquals("OLAP", stmt.getPattern());
            Assert.assertNull(stmt.getWhere());
        }
        {
            ShowEnginesStmt stmt =
                    (ShowEnginesStmt) SqlParser.parse("SHOW ENGINES WHERE ENGINE = 'OLAP'", 32).get(0);
            Analyzer.analyze(stmt, ctx);
            Assert.assertEquals("SHOW ENGINES WHERE ENGINE = 'OLAP'", stmt.toString());
            Assert.assertNull(stmt.getPattern());
            Assert.assertEquals("ENGINE = 'OLAP'", stmt.getWhere().toSql());
        }

    }

}