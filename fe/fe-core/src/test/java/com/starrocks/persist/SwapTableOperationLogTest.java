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

import org.junit.Assert;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;

public class SwapTableOperationLogTest {
    @Test
    public void testSerialization() throws Exception {
        // 1. Write objects to file
        File file = new File("./SwapTableOperationLogTest");
        file.createNewFile();
        DataOutputStream dos = new DataOutputStream(new FileOutputStream(file));

        SwapTableOperationLog log = new SwapTableOperationLog(1, 2, 3);
        log.write(dos);

        dos.flush();
        dos.close();

        // 2. Read objects from file
        DataInputStream dis = new DataInputStream(new FileInputStream(file));

        SwapTableOperationLog readLog = SwapTableOperationLog.read(dis);
        Assert.assertTrue(readLog.getDbId() == log.getDbId());
        Assert.assertTrue(readLog.getNewTblId() == log.getNewTblId());
        Assert.assertTrue(readLog.getOrigTblId() == log.getOrigTblId());

        // 3. delete files
        dis.close();
        file.delete();
    }
}
