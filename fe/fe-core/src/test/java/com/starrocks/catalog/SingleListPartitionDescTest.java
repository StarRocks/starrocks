// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.catalog;

import com.google.common.collect.Lists;
import com.starrocks.analysis.ColumnDef;
import com.starrocks.analysis.SingleItemListPartitionDesc;
import com.starrocks.analysis.TypeDef;
import com.starrocks.common.AnalysisException;
import com.starrocks.thrift.TStorageMedium;
import com.starrocks.thrift.TTabletType;
import org.junit.Assert;
import org.junit.Test;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SingleListPartitionDescTest {

    @Test
    public void testToSQL() {
        String partitionName = "p1";
        List<String> values = Lists.newArrayList("tianjin", "guangdong");
        boolean ifNotExists = false;
        Map<String, String> partitionProperties = new HashMap<>();
        partitionProperties.put("storage_medium", "SSD");
        partitionProperties.put("replication_num", "1");
        partitionProperties.put("in_memory", "true");
        partitionProperties.put("tablet_type", "memory");
        partitionProperties.put("storage_cooldown_time", "2122-07-09 12:12:12");

        SingleItemListPartitionDesc partitionDesc =
                new SingleItemListPartitionDesc(ifNotExists, partitionName, values, partitionProperties);
        String sql =
                "PARTITION p1 VALUES IN " +
                        "('tianjin','guangdong') (\"storage_cooldown_time\" = \"2122-07-09 12:12:12\", " +
                        "\"storage_medium\" = \"SSD\", \"replication_num\" = \"1\", " +
                        "\"tablet_type\" = \"memory\", \"in_memory\" = \"true\")";
        Assert.assertEquals(sql, partitionDesc.toSql());
    }

    @Test
    public void testGetMethods() throws ParseException, AnalysisException {
        ColumnDef province = new ColumnDef("province", TypeDef.createVarchar(64));
        province.setAggregateType(AggregateType.NONE);
        List<ColumnDef> columnDefLists = Lists.newArrayList(province);

        String partitionName = "p1";
        List<String> values = Lists.newArrayList("guangdong", "tianjin");
        boolean ifNotExists = false;
        Map<String, String> partitionProperties = new HashMap<>();
        partitionProperties.put("storage_medium", "SSD");
        partitionProperties.put("replication_num", "1");
        partitionProperties.put("in_memory", "true");
        partitionProperties.put("tablet_type", "memory");
        partitionProperties.put("storage_cooldown_time", "2122-07-09 12:12:12");

        SingleItemListPartitionDesc partitionDesc = new SingleItemListPartitionDesc(ifNotExists, partitionName,
                values, partitionProperties);
        partitionDesc.analyze(columnDefLists, null);

        Assert.assertEquals(partitionName, partitionDesc.getPartitionName());
        Assert.assertEquals(PartitionType.LIST, partitionDesc.getType());
        Assert.assertEquals(1, partitionDesc.getReplicationNum());
        Assert.assertEquals(TTabletType.TABLET_TYPE_MEMORY, partitionDesc.getTabletType());
        Assert.assertEquals(true, partitionDesc.isInMemory());

        DataProperty dataProperty = partitionDesc.getPartitionDataProperty();
        Assert.assertEquals(TStorageMedium.SSD, dataProperty.getStorageMedium());
        DateFormat sf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        long time = sf.parse("2122-07-09 12:12:12").getTime();
        Assert.assertEquals(time, dataProperty.getCooldownTimeMs());

        List<String> valuesFromGet = partitionDesc.getValues();
        Assert.assertEquals(valuesFromGet.size(), values.size());
        for (int i = 0; i < valuesFromGet.size(); i++) {
            Assert.assertEquals(valuesFromGet.get(i), values.get(i));
        }
    }

}
