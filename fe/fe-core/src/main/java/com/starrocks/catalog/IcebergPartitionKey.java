// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.catalog;

<<<<<<< HEAD
=======
import com.google.common.collect.ImmutableList;
import com.starrocks.connector.iceberg.IcebergApiConverter;

import java.util.List;

>>>>>>> 83dbcebb84 ([Enhancement] Compatible with the default partition value of the new hudi version (#29606))
public class IcebergPartitionKey extends PartitionKey implements NullablePartitionKey {
    public IcebergPartitionKey() {
        super();
    }

    @Override
<<<<<<< HEAD
    public String nullPartitionValue() {
        return IcebergTable.PARTITION_NULL_VALUE;
=======
    public List<String> nullPartitionValueList() {
        return ImmutableList.of(IcebergApiConverter.PARTITION_NULL_VALUE);
>>>>>>> 83dbcebb84 ([Enhancement] Compatible with the default partition value of the new hudi version (#29606))
    }
}
