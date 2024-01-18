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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/load/loadv2/InsertLoadJobTest.java

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

package com.starrocks.load.loadv2;

import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.common.exception.MetaNotFoundException;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.server.GlobalStateMgr;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Test;

import java.util.Set;

public class InsertLoadJobTest {

    @Test
    public void testGetTableNames(@Mocked GlobalStateMgr globalStateMgr,
                                  @Injectable Database database,
                                  @Injectable Table table) throws MetaNotFoundException {
        InsertLoadJob insertLoadJob = new InsertLoadJob("label", 1L, 1L, 1000, "", "");
        String tableName = "table1";
        new Expectations() {
            {
                globalStateMgr.getDb(anyLong);
                result = database;
                database.getTable(anyLong);
                result = table;
                table.getName();
                result = tableName;
            }
        };
        Set<String> tableNames = insertLoadJob.getTableNamesForShow();
        Assert.assertEquals(1, tableNames.size());
        Assert.assertEquals(true, tableNames.contains(tableName));
        Assert.assertEquals(JobState.FINISHED, insertLoadJob.getState());
        Assert.assertEquals(Integer.valueOf(100), Deencapsulation.getField(insertLoadJob, "progress"));

    }
}
