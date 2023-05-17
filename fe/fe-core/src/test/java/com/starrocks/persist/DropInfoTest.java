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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/persist/DropInfoTest.java

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

public class DropInfoTest {
    @Test
    public void testSerialization() throws Exception {
        // 1. Write objects to file
        File file = new File("./dropInfo");
        file.createNewFile();
        DataOutputStream dos = new DataOutputStream(new FileOutputStream(file));

        DropInfo info1 = new DropInfo();
        info1.write(dos);

        DropInfo info2 = new DropInfo(1, 2, -1, true);
        info2.write(dos);

        dos.flush();
        dos.close();

        // 2. Read objects from file
        DataInputStream dis = new DataInputStream(new FileInputStream(file));

        DropInfo rInfo1 = DropInfo.read(dis);
        Assert.assertTrue(rInfo1.equals(info1));

        DropInfo rInfo2 = DropInfo.read(dis);
        Assert.assertTrue(rInfo2.equals(info2));

        Assert.assertEquals(1, rInfo2.getDbId());
        Assert.assertEquals(2, rInfo2.getTableId());
        Assert.assertTrue(rInfo2.isForceDrop());

        Assert.assertTrue(rInfo2.equals(rInfo2));
        Assert.assertFalse(rInfo2.equals(this));
        Assert.assertFalse(info2.equals(new DropInfo(0, 2, -1L, true)));
        Assert.assertFalse(info2.equals(new DropInfo(1, 0, -1L, true)));
        Assert.assertFalse(info2.equals(new DropInfo(1, 2, -1L, false)));
        Assert.assertTrue(info2.equals(new DropInfo(1, 2, -1L, true)));

        // 3. delete files
        dis.close();
        file.delete();
    }
}
