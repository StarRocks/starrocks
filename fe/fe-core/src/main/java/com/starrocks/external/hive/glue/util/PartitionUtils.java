// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.external.hive.glue.util;

import com.amazonaws.services.glue.model.EntityNotFoundException;
import com.amazonaws.services.glue.model.InvalidInputException;
import com.amazonaws.services.glue.model.Partition;
import com.amazonaws.services.glue.model.PartitionValueList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;

public final class PartitionUtils {

    public static Map<PartitionKey, Partition> buildPartitionMap(final List<Partition> partitions) {
        Map<PartitionKey, Partition> partitionValuesMap = Maps.newHashMap();
        for (Partition partition : partitions) {
            partitionValuesMap.put(new PartitionKey(partition), partition);
        }
        return partitionValuesMap;
    }

    public static List<PartitionValueList> getPartitionValuesList(final Map<PartitionKey, Partition> partitionMap) {
        List<PartitionValueList> partitionValuesList = Lists.newArrayList();
        for (Map.Entry<PartitionKey, Partition> entry : partitionMap.entrySet()) {
            partitionValuesList.add(new PartitionValueList().withValues(entry.getValue().getValues()));
        }
        return partitionValuesList;
    }

    public static boolean isInvalidUserInputException(Exception e) {
        // exceptions caused by invalid requests, in which case we know all partitions creation failed
        return e instanceof EntityNotFoundException || e instanceof InvalidInputException;
    }

}
