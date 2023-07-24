// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.load.pipe;

import org.junit.Assert;
import org.junit.Test;

public class FileListRepoTest {

    @Test
    public void testPipeFileRecord() {
        String json = "{\"data\": [1, \"a.parquet\", \"123asdf\", 1024, \"UNLOADED\", " +
                "\"2023-07-01 01:01:01\", \"2023-07-01 01:01:01\", " +
                "\"2023-07-01 01:01:01\", \"2023-07-01 01:01:01\" " +
                "]}";
        FileListTableRepo.PipeFileRecord record = FileListTableRepo.PipeFileRecord.fromJson(json);
        String valueList = record.toValueList();
        Assert.assertEquals("(1, 'a.parquet', '123asdf', 1024, 'UNLOADED', " +
                        "'2023-07-01 01:01:01', '2023-07-01 01:01:01', " +
                        "'2023-07-01 01:01:01', '2023-07-01 01:01:01')",
                valueList);

        // contains null value
        json = "{\"data\": [1, \"a.parquet\", \"\", 1024, \"UNLOADED\", " +
                "\"\", \"2023-07-01 01:01:01\", " +
                "\"2023-07-01 01:01:01\", \"2023-07-01 01:01:01\" " +
                "]}";
        record = FileListTableRepo.PipeFileRecord.fromJson(json);
        valueList = record.toValueList();
        Assert.assertEquals("(1, 'a.parquet', '', 1024, 'UNLOADED', " +
                        "'', '2023-07-01 01:01:01', " +
                        "'2023-07-01 01:01:01', '2023-07-01 01:01:01')",
                valueList);
    }
}
