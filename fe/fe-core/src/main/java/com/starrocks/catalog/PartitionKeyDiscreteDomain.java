package com.starrocks.catalog;

import com.google.common.base.Preconditions;
import com.google.common.collect.DiscreteDomain;

import java.io.Serializable;

// used in partition pruner to canonical predicate range
public class PartitionKeyDiscreteDomain extends DiscreteDomain<PartitionKey> implements Serializable {
    private static final PartitionKeyDiscreteDomain INSTANCE = new PartitionKeyDiscreteDomain();
    private static final long serialVersionUID = 0L;

    public PartitionKey next(PartitionKey value) {
        return value.successor();
    }

    public PartitionKey previous(PartitionKey value) {
        return value.predecessor();
    }

    PartitionKey offset(PartitionKey origin, long distance) {
        Preconditions.checkState(distance >= 0);
        throw new UnsupportedOperationException("offset");
    }

    public long distance(PartitionKey start, PartitionKey end) {
        throw new UnsupportedOperationException("distance");
    }

    private Object readResolve() {
        return INSTANCE;
    }

    public String toString() {
        return "PartitionKeyDiscreteDomain()";
    }
}
