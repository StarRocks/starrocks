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

import com.google.common.collect.Lists;
import com.starrocks.catalog.DataProperty;
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
import java.util.List;

public class BatchModifyPartitionsInfoTest {
    private static String fileName = "./BatchModifyPartitionsInfoTest";

    private static final long DB_ID = 10000L;
    private static final long TB_ID = 30000L;
    private static final long PARTITION_ID_1 = 40000L;
    private static final long PARTITION_ID_2 = 40001L;
    private static final long PARTITION_ID_3 = 40002L;

    @After
    public void tearDown() {
        File file = new File(fileName);
        file.delete();
    }

    @Test
    public void testSerialzeBatchModifyPartitionsInfo() throws IOException, AnalysisException {
        List<ModifyPartitionInfo> partitionInfos = Lists.newArrayList();
        // 1. Write objects to file
        File file = new File(fileName);
        file.createNewFile();
        DataOutputStream out = new DataOutputStream(new FileOutputStream(file));

        List<Long> partitionIds = Lists.newArrayList(PARTITION_ID_1, PARTITION_ID_2, PARTITION_ID_3);
        for (long partitionId : partitionIds) {
            partitionInfos.add(new ModifyPartitionInfo(DB_ID, TB_ID, partitionId,
                    DataProperty.DEFAULT_DATA_PROPERTY, (short) 3, true));
        }

        BatchModifyPartitionsInfo batchModifyPartitionsInfo = new BatchModifyPartitionsInfo(partitionInfos);
        batchModifyPartitionsInfo.write(out);
        out.flush();
        out.close();

        // 2. Read objects from file
        DataInputStream in = new DataInputStream(new FileInputStream(file));

        BatchModifyPartitionsInfo readBatchModifyPartitionsInfo = BatchModifyPartitionsInfo.read(in);
        Assert.assertEquals(batchModifyPartitionsInfo, readBatchModifyPartitionsInfo);

        in.close();
    }
}
