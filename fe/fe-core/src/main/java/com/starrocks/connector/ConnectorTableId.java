// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.connector;

import com.starrocks.common.Id;
import com.starrocks.common.IdGenerator;

public class ConnectorTableId extends Id<ConnectorTableId> {
    public ConnectorTableId(int id) {
        super(id);
    }

    public static IdGenerator<ConnectorTableId> createGenerator() {
        return new IdGenerator<ConnectorTableId>() {
            @Override
            public ConnectorTableId getNextId() {
                return new ConnectorTableId(nextId++);
            }

            @Override
            public ConnectorTableId getMaxId() {
                return new ConnectorTableId(nextId - 1);
            }
        };
    }

    @Override
    public String toString() {
        return String.format("F%02d", id);
    }
}