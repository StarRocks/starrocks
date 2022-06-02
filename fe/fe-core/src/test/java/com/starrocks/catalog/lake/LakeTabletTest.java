// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.catalog.lake;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.catalog.Replica;
import com.starrocks.catalog.StarOSAgent;
import com.starrocks.catalog.lake.LakeTablet;
import com.starrocks.common.FeConstants;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.Backend;
import com.starrocks.system.SystemInfoService;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.List;
import java.util.Map;

public class LakeTabletTest {
    @Mocked
    private GlobalStateMgr globalStateMgr;

    @Test
    public void testSerialization() throws Exception {
        new Expectations(globalStateMgr) {
            {
                GlobalStateMgr.getCurrentStateJournalVersion();
                minTimes = 0;
                result = FeConstants.meta_version;
            }
        };

        LakeTablet tablet = new LakeTablet(1L, 2L);
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
        Assert.assertEquals(2L, newTablet.getShardId());
        Assert.assertEquals(3L, newTablet.getDataSize(true));
        Assert.assertEquals(4L, newTablet.getRowCount(0L));

        file.delete();
    }

    /*
    @Test
    public void testGetBackend(@Mocked SystemInfoService systemInfoService) {
        Map<Long, Backend> idToBackend = Maps.newHashMap();
        long backendId = 1L;
        idToBackend.put(backendId, new Backend(backendId, "127.0.0.1", 9050));

        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                result = globalStateMgr;
                globalStateMgr.getStarOSAgent();
                result = new StarOSAgent();
                GlobalStateMgr.getCurrentSystemInfo();
                result = systemInfoService;
                systemInfoService.getIdToBackend();
                result = ImmutableMap.copyOf(idToBackend);
                systemInfoService.getBackendIdByHost(anyString);
                result = backendId;
            }
        };

        LakeTablet tablet = new LakeTablet(1L, 2L);
        Assert.assertEquals(Sets.newHashSet(backendId), tablet.getBackendIds());
        Assert.assertEquals(backendId, tablet.getPrimaryBackendId());
        List<Replica> allQuerableReplicas = Lists.newArrayList();
        List<Replica> localReplicas = Lists.newArrayList();
        tablet.getQueryableReplicas(allQuerableReplicas, localReplicas, 0, backendId, 0);
        Assert.assertEquals(1, allQuerableReplicas.size());
        Assert.assertEquals(backendId, allQuerableReplicas.get(0).getBackendId());
        Assert.assertEquals(1, localReplicas.size());
        Assert.assertEquals(backendId, localReplicas.get(0).getBackendId());
    }
    */
}