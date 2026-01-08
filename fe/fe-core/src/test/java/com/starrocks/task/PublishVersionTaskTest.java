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

package com.starrocks.task;

import com.starrocks.catalog.GlobalStateMgrTestUtil;
import com.starrocks.catalog.Replica;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TTabletVersionPair;
import com.starrocks.transaction.TransactionType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationTargetException;
import java.util.Collections;

public class PublishVersionTaskTest {

    @BeforeEach
    public void setUp() throws InvocationTargetException, NoSuchMethodException, InstantiationException,
            IllegalAccessException {
        GlobalStateMgrTestUtil.createTestState();
    }

    @Test
    public void testUpdateReplicaVersionsWithMinReadableVersion() {
        long backendId = GlobalStateMgrTestUtil.testBackendId1;
        long dbId = GlobalStateMgrTestUtil.testDbId1;
        long tabletId = GlobalStateMgrTestUtil.testTabletId1;
        long baseVersion = GlobalStateMgrTestUtil.testStartVersion;

        Replica replica = GlobalStateMgr.getCurrentState().getTabletInvertedIndex().getReplica(tabletId, backendId);
        Assertions.assertNotNull(replica);
        Assertions.assertEquals(baseVersion, replica.getVersion());
        Assertions.assertEquals(0, replica.getMinReadableVersion());

        PublishVersionTask task = new PublishVersionTask(backendId, 1L, dbId, 0L,
                Collections.emptyList(), null, null, 0L, null, false, TransactionType.TXN_REPLICATION);

        TTabletVersionPair tabletVersionPair = new TTabletVersionPair();
        tabletVersionPair.setTablet_id(tabletId);
        tabletVersionPair.setVersion(baseVersion + 5);
        tabletVersionPair.setMin_readable_version(baseVersion + 3);

        task.updateReplicaVersions(Collections.singletonList(tabletVersionPair));

        Assertions.assertEquals(baseVersion + 3, replica.getMinReadableVersion());
        Assertions.assertEquals(baseVersion + 5, replica.getVersion());
    }
}
