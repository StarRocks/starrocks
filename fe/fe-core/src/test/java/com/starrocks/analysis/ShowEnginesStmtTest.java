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

import com.starrocks.common.exception.UserException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ShowResultSetMetaData;
import com.starrocks.sql.ast.ShowEnginesStmt;
import org.junit.Assert;
import org.junit.Test;

public class ShowEnginesStmtTest {
    @Test
    public void testNormal() throws UserException {
        ShowEnginesStmt stmt = new ShowEnginesStmt();
        com.starrocks.sql.analyzer.Analyzer.analyze(stmt, new ConnectContext());
        ShowResultSetMetaData metaData = stmt.getMetaData();
        Assert.assertNotNull(metaData);
        Assert.assertEquals(6, metaData.getColumnCount());
        Assert.assertEquals("Engine", metaData.getColumn(0).getName());
        Assert.assertEquals("Support", metaData.getColumn(1).getName());
        Assert.assertEquals("Comment", metaData.getColumn(2).getName());
        Assert.assertEquals("Transactions", metaData.getColumn(3).getName());
        Assert.assertEquals("XA", metaData.getColumn(4).getName());
        Assert.assertEquals("Savepoints", metaData.getColumn(5).getName());
    }

}