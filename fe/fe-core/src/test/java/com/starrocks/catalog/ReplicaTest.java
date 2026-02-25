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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/catalog/ReplicaTest.java

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

package com.starrocks.catalog;

import com.starrocks.catalog.Replica.ReplicaState;
import com.starrocks.server.GlobalStateMgr;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ReplicaTest {

    // replica serialize and deserialize test will use globalStateMgr so that it should be mocked
    @Mocked
    GlobalStateMgr globalStateMgr;

    private Replica replica;
    private long replicaId;
    private long backendId;
    private long version;
    private long dataSize;
    private long rowCount;

    @BeforeEach
    public void setUp() {
        replicaId = 10000;
        backendId = 20000;
        version = 2;
        dataSize = 9999;
        rowCount = 1024;
        replica = new Replica(replicaId, backendId, version, 0, dataSize, rowCount, ReplicaState.NORMAL, 0, version);
    }

    @Test
    public void getMethodTest() {
        Assertions.assertEquals(replicaId, replica.getId());
        Assertions.assertEquals(backendId, replica.getBackendId());
        Assertions.assertEquals(version, replica.getVersion());
        Assertions.assertEquals(dataSize, replica.getDataSize());
        Assertions.assertEquals(rowCount, replica.getRowCount());

        // update new version
        long newVersion = version + 1;
        long newDataSize = dataSize + 100;
        long newRowCount = rowCount + 10;
        replica.updateRowCount(newVersion, newDataSize, newRowCount);
        Assertions.assertEquals(newVersion, replica.getVersion());
        Assertions.assertEquals(newDataSize, replica.getDataSize());
        Assertions.assertEquals(newRowCount, replica.getRowCount());

        // check version catch up
        Assertions.assertFalse(replica.checkVersionCatchUp(5, false));
        Assertions.assertTrue(replica.checkVersionCatchUp(newVersion, false));
    }

    @Test
    public void testUpdateVersion1() {
        Replica originalReplica = new Replica(10000, 20000, 3, 0, 100, 78, ReplicaState.NORMAL, 0, 3);
        // new version is little than original version, it is invalid the version will not update
        originalReplica.updateVersionInfo(2, 100, 78);
        assertEquals(3, originalReplica.getVersion());
    }

    @Test
    public void testUpdateVersion2() {
        Replica originalReplica = new Replica(10000, 20000, 3, 0, 100, 78, ReplicaState.NORMAL, 0, 0);
        originalReplica.updateVersionInfo(3, 100, 78);
        // if new version >= current version and last success version <= new version, then last success version should be updated
        assertEquals(3, originalReplica.getLastSuccessVersion());
        assertEquals(3, originalReplica.getVersion());
    }

    @Test
    public void testUpdateVersion3() {
        // version(3) ---> last failed version (8) ---> last success version(10)
        Replica originalReplica = new Replica(10000, 20000, 3, 0, 100, 78, ReplicaState.NORMAL, 0, 0);
        originalReplica.updateLastFailedVersion(8);
        assertEquals(3, originalReplica.getLastSuccessVersion());
        assertEquals(3, originalReplica.getVersion());
        assertEquals(8, originalReplica.getLastFailedVersion());

        // update last success version 10
        originalReplica.updateVersionInfo(originalReplica.getVersion(),
                originalReplica.getLastFailedVersion(),
                10);
        assertEquals(10, originalReplica.getLastSuccessVersion());
        assertEquals(3, originalReplica.getVersion());
        assertEquals(8, originalReplica.getLastFailedVersion());

        // update version to 8, the last success version and version should be 10
        originalReplica.updateRowCount(8, 100, 78);
        assertEquals(10, originalReplica.getLastSuccessVersion());
        assertEquals(10, originalReplica.getVersion());
        assertEquals(-1, originalReplica.getLastFailedVersion());

        // update last failed version to 12
        originalReplica.updateLastFailedVersion(12);
        assertEquals(10, originalReplica.getLastSuccessVersion());
        assertEquals(10, originalReplica.getVersion());
        assertEquals(12, originalReplica.getLastFailedVersion());

        // update last success version to 15
        originalReplica.updateVersionInfo(originalReplica.getVersion(),
                originalReplica.getLastFailedVersion(), 15);
        assertEquals(15, originalReplica.getLastSuccessVersion());
        assertEquals(10, originalReplica.getVersion());
        assertEquals(12, originalReplica.getLastFailedVersion());

        // update last failed version to 18
        originalReplica.updateLastFailedVersion(18);
        assertEquals(10, originalReplica.getLastSuccessVersion());
        assertEquals(10, originalReplica.getVersion());
        assertEquals(18, originalReplica.getLastFailedVersion());

        // update version to 17 then version and success version is 17
        originalReplica.updateRowCount(17, 100, 78);
        assertEquals(17, originalReplica.getLastSuccessVersion());
        assertEquals(17, originalReplica.getVersion());
        assertEquals(18, originalReplica.getLastFailedVersion());

        // update version to 18, then version and last success version should be 18 and failed version should be -1
        originalReplica.updateRowCount(18, 100, 78);
        assertEquals(18, originalReplica.getLastSuccessVersion());
        assertEquals(18, originalReplica.getVersion());
        assertEquals(-1, originalReplica.getLastFailedVersion());
    }

    @Test
    public void testUpdateVersion4() {
        Replica originalReplica = new Replica(10000, 20000, 3, 0, 100, 78, ReplicaState.NORMAL, 0, 6);
        originalReplica.updateForRestore(2, 10, 20);
        assertEquals(2, originalReplica.getMinReadableVersion());
    }

    @Test
    public void testSetChecksum() {
        Replica originalReplica = new Replica(10000, 20000, 3, 0, 100, 78, ReplicaState.NORMAL, 0, 6);
        originalReplica.setChecksum(1024);
        assertEquals(1024, originalReplica.getChecksum());
    }
}

