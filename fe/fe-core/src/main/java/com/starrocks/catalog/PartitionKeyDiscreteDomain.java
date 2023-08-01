// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.catalog;

import com.google.common.collect.DiscreteDomain;

import java.io.Serializable;

// used in partition pruner to canonical predicate range
public class PartitionKeyDiscreteDomain extends DiscreteDomain<PartitionKey> implements Serializable {
    private static final PartitionKeyDiscreteDomain INSTANCE = new PartitionKeyDiscreteDomain();
    private static final long serialVersionUID = 0L;

    @Override
    public PartitionKey next(PartitionKey value) {
        return value.successor();
    }

    @Override
    public PartitionKey previous(PartitionKey value) {
        return value.predecessor();
    }

    @Override
    public long distance(PartitionKey start, PartitionKey end) {
        throw new UnsupportedOperationException("distance");
    }

    private Object readResolve() {
        return INSTANCE;
    }

    @Override
    public String toString() {
        return "PartitionKeyDiscreteDomain()";
    }
}
