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

package com.starrocks.catalog;

import com.starrocks.catalog.MaterializedIndex.IndexExtState;
import com.starrocks.catalog.MaterializedIndex.IndexState;
import com.starrocks.common.FeConstants;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.transaction.TransactionType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class PhysicalPartitionImplTest {
    private FakeGlobalStateMgr fakeGlobalStateMgr;

    private GlobalStateMgr globalStateMgr;

    @BeforeEach
    public void setUp() {
        fakeGlobalStateMgr = new FakeGlobalStateMgr();
        globalStateMgr = Deencapsulation.newInstance(GlobalStateMgr.class);

        FakeGlobalStateMgr.setGlobalStateMgr(globalStateMgr);
        FakeGlobalStateMgr.setMetaVersion(FeConstants.META_VERSION);
    }

    @Test
    public void testPhysicalPartition() throws Exception {
        PhysicalPartition p = new PhysicalPartition(1, "", 1, new MaterializedIndex());
        Assertions.assertEquals(1, p.getId());
        Assertions.assertEquals(1, p.getParentId());
        Assertions.assertFalse(p.isImmutable());
        p.setImmutable(true);
        Assertions.assertTrue(p.isImmutable());
        Assertions.assertEquals(1, p.getVisibleVersion());
        Assertions.assertFalse(p.isFirstLoad());

        p.hashCode();

        p.updateVersionForRestore(2);
        Assertions.assertEquals(2, p.getVisibleVersion());
        Assertions.assertTrue(p.isFirstLoad());

        p.updateVisibleVersion(3);
        Assertions.assertEquals(3, p.getVisibleVersion());

        p.updateVisibleVersion(4, System.currentTimeMillis(), 1);
        Assertions.assertEquals(4, p.getVisibleVersion());
        Assertions.assertEquals(1, p.getVisibleTxnId());

        Assertions.assertTrue(p.getVisibleVersionTime() <= System.currentTimeMillis());

        p.setNextVersion(6);
        Assertions.assertEquals(6, p.getNextVersion());
        Assertions.assertEquals(5, p.getCommittedVersion());

        p.setDataVersion(5);
        Assertions.assertEquals(5, p.getDataVersion());

        p.setNextDataVersion(6);
        Assertions.assertEquals(6, p.getNextDataVersion());
        Assertions.assertEquals(5, p.getCommittedDataVersion());

        p.createRollupIndex(new MaterializedIndex(1));
        p.createRollupIndex(new MaterializedIndex(2, IndexState.SHADOW));

        Assertions.assertEquals(0, p.getBaseIndex().getId());
        Assertions.assertNotNull(p.getIndex(0));
        Assertions.assertNotNull(p.getIndex(1));
        Assertions.assertNotNull(p.getIndex(2));
        Assertions.assertNull(p.getIndex(3));

        Assertions.assertEquals(3, p.getMaterializedIndices(IndexExtState.ALL).size());
        Assertions.assertEquals(2, p.getMaterializedIndices(IndexExtState.VISIBLE).size());
        Assertions.assertEquals(1, p.getMaterializedIndices(IndexExtState.SHADOW).size());

        Assertions.assertTrue(p.hasMaterializedView());
        Assertions.assertTrue(p.hasStorageData());
        Assertions.assertEquals(0, p.storageDataSize());
        Assertions.assertEquals(0, p.storageRowCount());
        Assertions.assertEquals(0, p.storageReplicaCount());
        Assertions.assertEquals(0, p.getTabletMaxDataSize());

        p.toString();

        Assertions.assertFalse(p.visualiseShadowIndex(1, false));
        Assertions.assertTrue(p.visualiseShadowIndex(2, false));

        p.createRollupIndex(new MaterializedIndex(3, IndexState.SHADOW));
        Assertions.assertTrue(p.visualiseShadowIndex(3, true));

        Assertions.assertTrue(p.equals(p));
        Assertions.assertFalse(p.equals(new Partition(0, 11, "", null, null)));

        PhysicalPartition p2 = new PhysicalPartition(1, "", 1, new MaterializedIndex());
        Assertions.assertFalse(p.equals(p2));
        p2.setBaseIndex(new MaterializedIndex(1));

        p.createRollupIndex(new MaterializedIndex(4, IndexState.SHADOW));
        p.deleteRollupIndex(0);
        p.deleteRollupIndex(1);
        p.deleteRollupIndex(2);
        p.deleteRollupIndex(4);

        Assertions.assertFalse(p.equals(p2));

        p.setIdForRestore(3);
        Assertions.assertEquals(3, p.getId());
        Assertions.assertEquals(1, p.getBeforeRestoreId());

        p.setParentId(3);
        Assertions.assertEquals(3, p.getParentId());

        p.setMinRetainVersion(1);
        Assertions.assertEquals(1, p.getMinRetainVersion());
        p.setLastVacuumTime(1);
        Assertions.assertEquals(1, p.getLastVacuumTime());

        p.setMinRetainVersion(3);
        p.setMetadataSwitchVersion(1);
        Assertions.assertEquals(1, p.getMinRetainVersion());
        p.setMetadataSwitchVersion(0);
        Assertions.assertEquals(3, p.getMinRetainVersion());

        p.setDataVersion(0);
        p.setNextDataVersion(0);
        p.setVersionEpoch(0);
        p.setVersionTxnType(null);
        p.gsonPostProcess();
        Assertions.assertEquals(p.getDataVersion(), p.getVisibleVersion());
        Assertions.assertEquals(p.getNextDataVersion(), p.getNextVersion());
        Assertions.assertTrue(p.getVersionEpoch() > 0);
        Assertions.assertEquals(p.getVersionTxnType(), TransactionType.TXN_NORMAL);
    }

    @Test
    public void testPhysicalPartitionEqual() throws Exception {
        PhysicalPartition p1 = new PhysicalPartition(1, "", 1, new MaterializedIndex());
        PhysicalPartition p2 = new PhysicalPartition(1, "", 1, new MaterializedIndex());
        Assertions.assertTrue(p1.equals(p2));

        p1.createRollupIndex(new MaterializedIndex());
        p2.createRollupIndex(new MaterializedIndex());
        Assertions.assertTrue(p1.equals(p2));

        p1.createRollupIndex(new MaterializedIndex(1));
        p2.createRollupIndex(new MaterializedIndex(2));
        Assertions.assertFalse(p1.equals(p2));
    }
}
