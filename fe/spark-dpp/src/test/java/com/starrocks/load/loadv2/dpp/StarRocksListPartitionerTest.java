// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

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
        List<EtlJobConfig.EtlPartition> partitions = new ArrayList<>();
        partitions.add(partition1);
        partitions.add(partition2);
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
        Assert.assertEquals(2, num);

        List<Object> fields1 = new ArrayList<>();
        fields1.add(-100);
        fields1.add("name");
        DppColumns record1 = new DppColumns(fields1);
        int id1 = listPartitioner.getPartition(record1);
        Assert.assertEquals(-1, id1);

        List<Object> fields2 = new ArrayList<>();
        fields2.add("2023-01-01");
        fields2.add("cn");
        DppColumns record2 = new DppColumns(fields2);
        int id2 = listPartitioner.getPartition(record2);
        Assert.assertEquals(1, id2);
    }
}
