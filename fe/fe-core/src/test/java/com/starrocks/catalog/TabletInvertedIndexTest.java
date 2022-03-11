package com.starrocks.catalog;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.starrocks.system.Backend;
import com.starrocks.system.SystemInfoService;
import com.starrocks.thrift.TStorageMedium;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TabletInvertedIndexTest {

    @Mocked
    private Catalog catalog;

    @Test
    public void getBackendStorageTypeCntTest() {
        TabletInvertedIndex tabletIndex = new TabletInvertedIndex();
        long mockBackendId = 100;

        SystemInfoService systemInfoService = new SystemInfoService();
        new Expectations() {
            {
                Catalog.getCurrentSystemInfo();
                minTimes = 0;
                result = systemInfoService;
            }};

        int backendStorageTypeCnt = tabletIndex.getBackendStorageTypeCnt(mockBackendId);
        Assert.assertEquals(-1, backendStorageTypeCnt);

        Backend backend = new Backend(100L, "192.168.1.1", 9050);
        ImmutableMap<String, DiskInfo> disks = ImmutableMap.<String, DiskInfo>builder().build();
        backend.setDisks(disks);
        systemInfoService.addBackend(backend);
        backendStorageTypeCnt = tabletIndex.getBackendStorageTypeCnt(mockBackendId);
        Assert.assertEquals(0, backendStorageTypeCnt);

        DiskInfo diskInfo1 = new DiskInfo("/tmp/abc");
        diskInfo1.setStorageMedium(TStorageMedium.HDD);
        disks = ImmutableMap.of("/tmp/abc", diskInfo1);
        backend.setDisks(disks);
        backendStorageTypeCnt = tabletIndex.getBackendStorageTypeCnt(mockBackendId);
        Assert.assertEquals(1, backendStorageTypeCnt);

        DiskInfo diskInfo2 = new DiskInfo("/tmp/abc");
        diskInfo2.setStorageMedium(TStorageMedium.SSD);
        disks = ImmutableMap.of("/tmp/abc", diskInfo1, "/tmp/abcd", diskInfo2);
        backend.setDisks(disks);
        backendStorageTypeCnt = tabletIndex.getBackendStorageTypeCnt(mockBackendId);
        Assert.assertEquals(2, backendStorageTypeCnt);

        diskInfo2.setStorageMedium(TStorageMedium.HDD);
        disks = ImmutableMap.of("/tmp/abc", diskInfo1, "/tmp/abcd", diskInfo2);
        backend.setDisks(disks);
        backendStorageTypeCnt = tabletIndex.getBackendStorageTypeCnt(mockBackendId);
        Assert.assertEquals(1, backendStorageTypeCnt);

    }

}
