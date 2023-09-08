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

package com.starrocks.analysis;

import com.starrocks.catalog.ResourceGroupClassifier;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.Test;

public class ResourceGroupMgrTest {

    @Test
    public void testResourceGroupTypeFromStatement() {
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();

        {
            String sql = "LOAD LABEL label0 (DATA INFILE('path/k2=1/file1') INTO TABLE t2 FORMAT AS 'orc' (k1,k33,v) " +
                    "COLUMNS FROM PATH AS (k2) set (k3 = substr(k33,1,5))) WITH BROKER 'broker0'";
            StatementBase stmt = SqlParser.parse(sql, ctx.getSessionVariable().getSqlMode()).get(0);
            ResourceGroupClassifier.QueryType queryType = ResourceGroupClassifier.QueryType.fromStatement(stmt);
            Assert.assertEquals(ResourceGroupClassifier.QueryType.INSERT, queryType);
        }
        {
            String sql = "insert into t1 select * from t1";
            StatementBase stmt = SqlParser.parse(sql, ctx.getSessionVariable().getSqlMode()).get(0);
            ResourceGroupClassifier.QueryType queryType = ResourceGroupClassifier.QueryType.fromStatement(stmt);
            Assert.assertEquals(ResourceGroupClassifier.QueryType.INSERT, queryType);
        }
        {
            String sql = "select * from t1";
            StatementBase stmt = SqlParser.parse(sql, ctx.getSessionVariable().getSqlMode()).get(0);
            ResourceGroupClassifier.QueryType queryType = ResourceGroupClassifier.QueryType.fromStatement(stmt);
            Assert.assertEquals(ResourceGroupClassifier.QueryType.SELECT, queryType);
        }
    }
}
