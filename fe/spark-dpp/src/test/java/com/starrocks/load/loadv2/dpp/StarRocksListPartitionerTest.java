// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.load.loadv2.dpp;

import com.starrocks.load.loadv2.etl.EtlJobConfig;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class StarRocksListPartitionerTest {
    @Test
    public void testListPartitioner() {
        List<List<Object>> inKeys1 = new ArrayList<>();
        String[] arr1 = {"2023-01-01", "us"};
        inKeys1.add(Arrays.asList(arr1));
        EtlJobConfig.EtlPartition partition1 = new EtlJobConfig.EtlPartition(
                10000, inKeys1, 3);

        List<List<Object>> inKeys2 = new ArrayList<>();
        String[] arr2 = {"2023-01-01", "cn"};
        inKeys2.add(Arrays.asList(arr2));
        EtlJobConfig.EtlPartition partition2 = new EtlJobConfig.EtlPartition(
                10001, inKeys2, 3);

        List<List<Object>> inKeys3 = new ArrayList<>();
        String[] arr31 = {"2022-02-01", "cn"};
        inKeys3.add(Arrays.asList(arr31));
        String[] arr32 = {"2022-02-01", "us"};
        inKeys3.add(Arrays.asList(arr32));
        EtlJobConfig.EtlPartition partition3 = new EtlJobConfig.EtlPartition(
                10002, inKeys3, 3);

        List<EtlJobConfig.EtlPartition> partitions = new ArrayList<>();
        partitions.add(partition1);
        partitions.add(partition2);
        partitions.add(partition3);
        List<String> partitionColumns = new ArrayList<>();
        partitionColumns.add("dt");
        partitionColumns.add("country_code");
        List<String> bucketColumns = new ArrayList<>();
        bucketColumns.add("key");
        EtlJobConfig.EtlPartitionInfo partitionInfo = new EtlJobConfig.EtlPartitionInfo(
                "LIST", partitionColumns, bucketColumns, partitions);
        List<StarRocksListPartitioner.PartitionListKey> partitionListKeys = new ArrayList<>();
        for (EtlJobConfig.EtlPartition partition : partitions) {
            StarRocksListPartitioner.PartitionListKey partitionListKey = new StarRocksListPartitioner.PartitionListKey();
            for (List<Object> objectList : partition.inKeys) {
                List<Object> inKeyColumns = new ArrayList<>();
                inKeyColumns.addAll(objectList);
                partitionListKey.inKeys.add(new DppColumns(inKeyColumns));
            }
            partitionListKeys.add(partitionListKey);
        }
        List<Integer> partitionKeyIndexes = new ArrayList<>();
        partitionKeyIndexes.add(0);
        partitionKeyIndexes.add(1);
        StarRocksListPartitioner listPartitioner =
                new StarRocksListPartitioner(partitionInfo, partitionKeyIndexes, partitionListKeys);
        int num = listPartitioner.numPartitions();
        Assert.assertEquals(3, num);

        List<Object> fields1 = new ArrayList<>();
        fields1.add(-100);
        fields1.add("name");
        DppColumns record1 = new DppColumns(fields1);
        int id1 = listPartitioner.getPartition(record1);
        Assert.assertEquals(-1, id1);

        List<Object> fields2 = new ArrayList<>();
        fields2.add("2023-01-01");
        fields2.add("cn");
        fields2.add("123455");
        DppColumns record2 = new DppColumns(fields2);
        int id2 = listPartitioner.getPartition(record2);
        Assert.assertEquals(1, id2);

        List<Object> fields3 = new ArrayList<>();
        fields3.add("cn");
        fields3.add("2023-01-01");
        fields3.add("123455");
        DppColumns record3 = new DppColumns(fields3);
        int id3 = listPartitioner.getPartition(record3);
        Assert.assertEquals(-1, id3);

        List<Object> fields4 = new ArrayList<>();
        fields4.add("2022-02-01");
        fields4.add("cn");
        fields4.add("123455");
        DppColumns record4 = new DppColumns(fields4);
        int id4 = listPartitioner.getPartition(record4);
        Assert.assertEquals(2, id4);

    }
}
