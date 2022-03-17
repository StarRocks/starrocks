// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.catalog;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.common.FeConstants;
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
import java.util.Map;

public class StarOSTabletTest {
    @Mocked
    private Catalog catalog;

    @Test
    public void testSerialization() throws Exception {
        new Expectations(catalog) {
            {
                Catalog.getCurrentCatalogJournalVersion();
                minTimes = 0;
                result = FeConstants.meta_version;
            }
        };

        StarOSTablet tablet = new StarOSTablet(1L, 2L);
        tablet.setDataSize(3L);
        tablet.setRowCount(4L);

        // Serialize
        File file = new File("./StarOSTabletSerializationTest");
        file.createNewFile();
        try (DataOutputStream dos = new DataOutputStream(new FileOutputStream(file))) {
            tablet.write(dos);
            dos.flush();
        }

        // Deserialize
        StarOSTablet newTablet = null;
        try (DataInputStream dis = new DataInputStream(new FileInputStream(file))) {
            newTablet = StarOSTablet.read(dis);
        }

        // Check
        Assert.assertNotNull(newTablet);
        Assert.assertEquals(1L, newTablet.getId());
        Assert.assertEquals(2L, newTablet.getShardId());
        Assert.assertEquals(3L, newTablet.getDataSize());
        Assert.assertEquals(4L, newTablet.getRowCount(0L));

        file.delete();
    }

    @Test
    public void testGetBackend(@Mocked SystemInfoService systemInfoService) {
        Map<Long, Backend> idToBackend = Maps.newHashMap();
        long backendId = 1L;
        idToBackend.put(backendId, new Backend(backendId, "127.0.0.1", 9050));

        new Expectations() {
            {
                Catalog.getCurrentCatalog();
                result = catalog;
                catalog.getStarOSAgent();
                result = new StarOSAgent();
                Catalog.getCurrentSystemInfo();
                result = systemInfoService;
                systemInfoService.getIdToBackend();
                result = ImmutableMap.copyOf(idToBackend);
                systemInfoService.getBackendIdByHost(anyString);
                result = backendId;
            }
        };

        StarOSTablet tablet = new StarOSTablet(1L, 2L);
        Assert.assertEquals(Sets.newHashSet(backendId), tablet.getBackendIds());
        Assert.assertEquals(backendId, tablet.getPrimaryBackendId());
    }
}