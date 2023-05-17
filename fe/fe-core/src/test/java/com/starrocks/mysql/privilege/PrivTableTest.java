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


package com.starrocks.mysql.privilege;

import com.starrocks.analysis.CompoundPredicate;
import com.starrocks.sql.ast.UserIdentity;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;

public class PrivTableTest {
    private static final Logger LOG = LogManager.getLogger(PrivTableTest.class);

    /**
     * test serialized & deserialized
     */
    @Test
    public void testSerializedAndDeserialized() throws Exception {
        // use ResourcePrivTable because PrivTable is a abstract class
        ResourcePrivTable table = new ResourcePrivTable();
        PrivBitSet resourceUsage = PrivBitSet.of(Privilege.USAGE_PRIV);
        PrivPredicate onlyUsage = PrivPredicate.of(PrivBitSet.of(Privilege.USAGE_PRIV), CompoundPredicate.Operator.AND);
        UserIdentity user1 = UserIdentity.createAnalyzedUserIdentWithIp("user1", "%");
        String resource1 = "resource1";

        // 1. only one entry
        // 1.1 grant
        ResourcePrivEntry entry1 = ResourcePrivEntry.create(
                user1.getHost(), resource1, user1.getQualifiedUser(), false, resourceUsage);
        table.addEntry(entry1, true, false);
        LOG.info("current table: {}", table);
        // 1.2 dump to file
        File tempFile = File.createTempFile("ImpersonateUserPrivTableTest", ".image");
        LOG.info("dump to file {}", tempFile.getAbsolutePath());
        DataOutputStream dos = new DataOutputStream(new FileOutputStream(tempFile));
        table.write(dos);
        dos.close();

        // 1.3 load from file
        DataInputStream dis = new DataInputStream(new FileInputStream(tempFile));
        ResourcePrivTable loadTable = (ResourcePrivTable) ResourcePrivTable.read(dis);
        LOG.info("load table: {}", loadTable);

        // 1.4 check & cleanup
        Assert.assertEquals(1, table.size());
        PrivBitSet privBitSet = new PrivBitSet();
        loadTable.getPrivs(user1, resource1, privBitSet);
        Assert.assertTrue(privBitSet.satisfy(onlyUsage));
        tempFile.delete();

        // 2. add one entry, make it two
        // 2.1 grant
        UserIdentity user2 = UserIdentity.createAnalyzedUserIdentWithIp("user2", "%");
        String resource2 = "resource2";
        ResourcePrivEntry entry2 = ResourcePrivEntry.create(
                user2.getHost(), resource2, user2.getQualifiedUser(), false, resourceUsage);
        table.addEntry(entry2, true, false);
        LOG.info("current table: {}", table);

        // 2.2. dump to file
        tempFile = File.createTempFile("ImpersonateUserPrivTableTest", ".image");
        LOG.info("dump to file {}", tempFile.getAbsolutePath());
        dos = new DataOutputStream(new FileOutputStream(tempFile));
        table.write(dos);
        dos.close();

        // 2.3 load from file
        dis = new DataInputStream(new FileInputStream(tempFile));
        loadTable = (ResourcePrivTable) ResourcePrivTable.read(dis);
        LOG.info("load table: {}", loadTable);

        // 2.4 check & cleanup
        Assert.assertEquals(2, table.size());
        privBitSet = new PrivBitSet();
        loadTable.getPrivs(user1, resource1, privBitSet);
        Assert.assertTrue(privBitSet.satisfy(onlyUsage));
        privBitSet = new PrivBitSet();
        loadTable.getPrivs(user2, resource2, privBitSet);
        Assert.assertTrue(privBitSet.satisfy(onlyUsage));
        tempFile.delete();
    }
}
