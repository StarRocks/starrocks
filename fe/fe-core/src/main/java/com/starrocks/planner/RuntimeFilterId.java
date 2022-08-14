// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.planner;

import com.starrocks.common.Id;
import com.starrocks.common.IdGenerator;

// The unique id for join runtime filter
public class RuntimeFilterId extends Id<RuntimeFilterId> {
    public RuntimeFilterId(int id) {
        super(id);
    }

    public static IdGenerator<RuntimeFilterId> createGenerator() {
        return new IdGenerator<RuntimeFilterId>() {
            @Override
            public RuntimeFilterId getNextId() {
                return new RuntimeFilterId(nextId++);
            }

            @Override
            public RuntimeFilterId getMaxId() {
                return new RuntimeFilterId(nextId - 1);
            }
        };
    }
}
