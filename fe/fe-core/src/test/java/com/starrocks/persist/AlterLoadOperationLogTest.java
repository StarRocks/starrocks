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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/persist/AlterRoutineLoadOperationLogTest.java

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

import com.google.common.collect.Maps;
import com.starrocks.common.exception.AnalysisException;
import com.starrocks.sql.ast.LoadStmt;
import org.junit.Assert;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Map;

public class AlterLoadOperationLogTest {
    private static String fileName = "./AlterLoadOperationLogTest";

    @Test
    public void testSerialzeAlterViewInfo() throws IOException, AnalysisException {
        // 1. Write objects to file
        File file = new File(fileName);
        file.createNewFile();
        file.deleteOnExit();
        DataOutputStream out = new DataOutputStream(new FileOutputStream(file));

        long jobId = 1000;
        Map<String, String> jobProperties = Maps.newHashMap();
        jobProperties.put(LoadStmt.PRIORITY, "NORMAL");

        AlterLoadJobOperationLog log = new AlterLoadJobOperationLog(jobId,
                        jobProperties);
        log.write(out);
        out.flush();
        out.close();

        // 2. Read objects from file
        DataInputStream in = new DataInputStream(new FileInputStream(file));

        AlterLoadJobOperationLog log2 = AlterLoadJobOperationLog.read(in);
        Assert.assertEquals(1, log2.getJobProperties().size());
        Assert.assertEquals("NORMAL", log2.getJobProperties().get(LoadStmt.PRIORITY));

        in.close();
    }

}
