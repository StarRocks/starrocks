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


package com.starrocks.lake;

import com.starrocks.common.FeConstants;
import com.starrocks.server.GlobalStateMgr;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;

public class LakeTabletTest {
    @Mocked
    private GlobalStateMgr globalStateMgr;

    @Test
    public void testSerialization() throws Exception {
        new Expectations(globalStateMgr) {
            {
                GlobalStateMgr.getCurrentStateJournalVersion();
                minTimes = 0;
                result = FeConstants.META_VERSION;
            }
        };

        LakeTablet tablet = new LakeTablet(1L);
        tablet.setDataSize(3L);
        tablet.setRowCount(4L);

        // Serialize
        File file = new File("./LakeTabletSerializationTest");
        file.createNewFile();
        try (DataOutputStream dos = new DataOutputStream(new FileOutputStream(file))) {
            tablet.write(dos);
            dos.flush();
        }

        // Deserialize
        LakeTablet newTablet = null;
        try (DataInputStream dis = new DataInputStream(new FileInputStream(file))) {
            newTablet = LakeTablet.read(dis);
        }

        // Check
        Assert.assertNotNull(newTablet);
        Assert.assertEquals(1L, newTablet.getId());
        Assert.assertEquals(1L, newTablet.getShardId());
        Assert.assertEquals(3L, newTablet.getDataSize(true));
        Assert.assertEquals(4L, newTablet.getRowCount(0L));

        file.delete();
    }
}
