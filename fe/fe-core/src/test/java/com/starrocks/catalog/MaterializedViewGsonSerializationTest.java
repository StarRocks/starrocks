

package com.starrocks.catalog;

import com.starrocks.persist.gson.GsonUtils;
import org.junit.Assert;
import org.junit.Test;

public class MaterializedViewGsonSerializationTest {

    @Test
    public void testSerialize() {
        MaterializedViewPartitionNameRefInfo
                materializedViewPartitionNameRefInfo = new MaterializedViewPartitionNameRefInfo();
        materializedViewPartitionNameRefInfo.setDbId(10001);
        materializedViewPartitionNameRefInfo.setMvId(10003);
        materializedViewPartitionNameRefInfo.setTablePartitionName("p1");
        materializedViewPartitionNameRefInfo.setMvPartitionName("m1");

        MaterializedViewPartitionNameRefInfo materializedViewPartitionNameRefInfo1 =
                GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(materializedViewPartitionNameRefInfo),
                        MaterializedViewPartitionNameRefInfo.class);
        Assert.assertEquals(materializedViewPartitionNameRefInfo.getDbId(), materializedViewPartitionNameRefInfo1.getDbId());
        Assert.assertEquals(materializedViewPartitionNameRefInfo.getMvId(), materializedViewPartitionNameRefInfo1.getMvId());
        Assert.assertEquals(materializedViewPartitionNameRefInfo.getMvPartitionName(),
                materializedViewPartitionNameRefInfo1.getMvPartitionName());
        Assert.assertEquals(materializedViewPartitionNameRefInfo.getTablePartitionName(),
                materializedViewPartitionNameRefInfo1.getTablePartitionName());

        MaterializedViewPartitionVersionInfo
                materializedViewPartitionVersionInfo = new MaterializedViewPartitionVersionInfo();
        materializedViewPartitionVersionInfo.setDbId(10001);
        materializedViewPartitionVersionInfo.setMvId(10003);
        materializedViewPartitionVersionInfo.setTableId(10002);
        materializedViewPartitionVersionInfo.setTablePartitionName("p1");
        materializedViewPartitionVersionInfo.setTablePartitionVersion(3);

        MaterializedViewPartitionVersionInfo materializedViewPartitionVersionInfo1 =
                GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(materializedViewPartitionVersionInfo),
                        MaterializedViewPartitionVersionInfo.class);
        Assert.assertEquals(materializedViewPartitionVersionInfo.getDbId(), materializedViewPartitionVersionInfo1.getDbId());
        Assert.assertEquals(materializedViewPartitionVersionInfo.getMvId(), materializedViewPartitionVersionInfo1.getMvId());
        Assert.assertEquals(materializedViewPartitionVersionInfo.getTableId(),
                materializedViewPartitionVersionInfo1.getTableId());
        Assert.assertEquals(materializedViewPartitionVersionInfo.getTablePartitionName(),
                materializedViewPartitionVersionInfo1.getTablePartitionName());
        Assert.assertEquals(materializedViewPartitionVersionInfo.getTablePartitionVersion(),
                materializedViewPartitionVersionInfo1.getTablePartitionVersion());
    }
}
