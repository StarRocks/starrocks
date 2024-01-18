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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/persist/AlterViewInfoTest.java

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

package com.starrocks.persist;

import com.google.common.collect.Lists;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Type;
import com.starrocks.common.exception.AnalysisException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

public class AlterViewInfoTest {
    private static String fileName = "./AlterViewInfoTest";

    private final long dbId = 10000L;
    private final long tableId = 30000L;
    private final String inlineViewDef = "Select a1, a2 From test_tbl Order By a1";
    private final long sqlMode = 0L;

    @After
    public void tearDown() {
        File file = new File(fileName);
        file.delete();
    }

    @Test
    public void testSerialzeAlterViewInfo() throws IOException, AnalysisException {
        // 1. Write objects to file
        File file = new File(fileName);
        file.createNewFile();
        DataOutputStream out = new DataOutputStream(new FileOutputStream(file));

        Column column1 = new Column("col1", Type.BIGINT);
        Column column2 = new Column("col2", Type.DOUBLE);

        AlterViewInfo alterViewInfo =
                new AlterViewInfo(dbId, tableId, inlineViewDef, Lists.newArrayList(column1, column2), sqlMode, null);
        alterViewInfo.write(out);
        out.flush();
        out.close();

        // 2. Read objects from file
        DataInputStream in = new DataInputStream(new FileInputStream(file));

        AlterViewInfo readAlterViewInfo = AlterViewInfo.read(in);
        Assert.assertEquals(alterViewInfo, readAlterViewInfo);

        in.close();
    }

}
