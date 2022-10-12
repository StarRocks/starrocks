// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.connector;

import com.starrocks.common.Id;
import com.starrocks.common.IdGenerator;

public class ConnectorDatabaseId extends Id<ConnectorDatabaseId> {
    public ConnectorDatabaseId(int id) {
        super(id);
    }

    public static IdGenerator<ConnectorDatabaseId> createGenerator() {
        return new IdGenerator<ConnectorDatabaseId>() {
            @Override
            public ConnectorDatabaseId getNextId() {
                return new ConnectorDatabaseId(nextId++);
            }

            @Override
            public ConnectorDatabaseId getMaxId() {
                return new ConnectorDatabaseId(nextId - 1);
            }
        };
    }

    @Override
    public String toString() {
        return String.format("F%02d", id);
    }
}
