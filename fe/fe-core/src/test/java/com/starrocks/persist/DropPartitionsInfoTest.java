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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/persist/DropPartitionInfoTest.java

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

import org.junit.Assert;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.List;

public class DropPartitionsInfoTest {
    @Test
    public void testSerialization() throws Exception {
        // 1. Write objects to file
        File file = new File("./dropPartitionsInfo");
        file.createNewFile();
        DataOutputStream dos = new DataOutputStream(new FileOutputStream(file));
        List<String> partitionNames = new ArrayList<>();
        partitionNames.add("test_partition1");
        partitionNames.add("test_partition2");
        DropPartitionsInfo info1 = new DropPartitionsInfo(1L, 2L, false, true, partitionNames);
        info1.write(dos);

        dos.flush();
        dos.close();

        // 2. Read objects from file
        DataInputStream dis = new DataInputStream(new FileInputStream(file));

        DropPartitionsInfo rInfo1 = DropPartitionsInfo.read(dis);

        Assert.assertEquals(Long.valueOf(1L), rInfo1.getDbId());
        Assert.assertEquals(Long.valueOf(2L), rInfo1.getTableId());
        Assert.assertEquals(partitionNames, rInfo1.getPartitionNames());
        Assert.assertFalse(rInfo1.isTempPartition());
        Assert.assertTrue(rInfo1.isForceDrop());

        Assert.assertEquals(rInfo1, info1);
        Assert.assertFalse(rInfo1.equals(this));
        Assert.assertNotEquals(info1, new DropPartitionsInfo(-1L, 2L, false, true, partitionNames));
        Assert.assertNotEquals(info1, new DropPartitionsInfo(1L, -2L, false, true, partitionNames));
        List<String> partitionNames1 = new ArrayList<>();
        partitionNames1.add("test_partition2");
        partitionNames1.add("test_partition3");
        Assert.assertNotEquals(info1, new DropPartitionsInfo(1L, 2L, false, true, partitionNames1));
        Assert.assertNotEquals(info1, new DropPartitionsInfo(1L, 2L, true, true, partitionNames));
        Assert.assertNotEquals(info1, new DropPartitionsInfo(1L, 2L, false, false, partitionNames));
        Assert.assertEquals(info1, new DropPartitionsInfo(1L, 2L, false, true, partitionNames));

        // 3. delete files
        dis.close();
        file.delete();
    }
}
