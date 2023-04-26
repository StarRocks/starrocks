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


package com.starrocks.persist;

import com.google.common.collect.Maps;
import com.starrocks.analysis.RoutineLoadDataSourceProperties;
import com.starrocks.common.AnalysisException;
import com.starrocks.sql.ast.CreateRoutineLoadStmt;
import org.junit.Assert;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Map;

public class AlterRoutineLoadOperationLogTest {
    private static String fileName = "./AlterRoutineLoadOperationLogTest";

    @Test
    public void testSerialzeAlterViewInfo() throws IOException, AnalysisException {
        // 1. Write objects to file
        File file = new File(fileName);
        file.createNewFile();
        file.deleteOnExit();
        DataOutputStream out = new DataOutputStream(new FileOutputStream(file));

        long jobId = 1000;
        Map<String, String> jobProperties = Maps.newHashMap();
        jobProperties.put(CreateRoutineLoadStmt.DESIRED_CONCURRENT_NUMBER_PROPERTY, "5");

        String typeName = "kafka";
        Map<String, String> dataSourceProperties = Maps.newHashMap();
        dataSourceProperties.put(CreateRoutineLoadStmt.KAFKA_PARTITIONS_PROPERTY, "0, 1");
        dataSourceProperties.put(CreateRoutineLoadStmt.KAFKA_OFFSETS_PROPERTY, "10000, 20000");
        dataSourceProperties.put("property.group.id", "mygroup");
        RoutineLoadDataSourceProperties routineLoadDataSourceProperties = new RoutineLoadDataSourceProperties(typeName,
                dataSourceProperties);
        routineLoadDataSourceProperties.analyze();

        AlterRoutineLoadJobOperationLog log = new AlterRoutineLoadJobOperationLog(jobId,
                jobProperties, routineLoadDataSourceProperties, null);
        log.write(out);
        out.flush();
        out.close();

        // 2. Read objects from file
        DataInputStream in = new DataInputStream(new FileInputStream(file));

        AlterRoutineLoadJobOperationLog log2 = AlterRoutineLoadJobOperationLog.read(in);
        Assert.assertEquals(1, log2.getJobProperties().size());
        Assert.assertEquals("5", log2.getJobProperties().get(CreateRoutineLoadStmt.DESIRED_CONCURRENT_NUMBER_PROPERTY));
        Assert.assertEquals(1, log2.getDataSourceProperties().getCustomKafkaProperties().size());
        Assert.assertEquals("mygroup", log2.getDataSourceProperties().getCustomKafkaProperties().get("group.id"));
        Assert.assertEquals(routineLoadDataSourceProperties.getKafkaPartitionOffsets().get(0),
                log2.getDataSourceProperties().getKafkaPartitionOffsets().get(0));
        Assert.assertEquals(routineLoadDataSourceProperties.getKafkaPartitionOffsets().get(1),
                log2.getDataSourceProperties().getKafkaPartitionOffsets().get(1));

        in.close();
    }

}
