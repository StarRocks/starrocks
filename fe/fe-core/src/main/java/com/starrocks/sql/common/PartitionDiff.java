// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.common;

import com.google.common.collect.Range;
import com.starrocks.catalog.PartitionKey;

import java.util.Map;

public class PartitionDiff {

    Map<String, Range<PartitionKey>> adds;

    Map<String, Range<PartitionKey>> deletes;

    public PartitionDiff(Map<String, Range<PartitionKey>> adds, Map<String, Range<PartitionKey>> deletes) {
        this.adds = adds;
        this.deletes = deletes;
    }

    public Map<String, Range<PartitionKey>> getAdds() {
        return adds;
    }

    public void setAdds(Map<String, Range<PartitionKey>> adds) {
        this.adds = adds;
    }

    public Map<String, Range<PartitionKey>> getDeletes() {
        return deletes;
    }

    public void setDeletes(Map<String, Range<PartitionKey>> deletes) {
        this.deletes = deletes;
    }
}
