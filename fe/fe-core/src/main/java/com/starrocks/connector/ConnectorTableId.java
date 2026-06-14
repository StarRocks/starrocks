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

package com.starrocks.connector;

import java.util.concurrent.atomic.AtomicLong;

/**
 * In-memory, per-FE-process ID for external connector tables (Hive/Iceberg/Hudi/Paimon/...).
 * Not persisted and not synchronized across FEs — every FE assigns its own sequence and the
 * counter resets on restart. Safe to use as a transient handle for the lifetime of an FE process;
 * never store it on disk or send it to another FE.
 */
public class ConnectorTableId {

    public static final Generator CONNECTOR_ID_GENERATOR = new Generator();
    private static final long CONNECTOR_TABLE_ID_OFFSET = 100_000_000L;

    private final long id;

    public ConnectorTableId(long id) {
        this.id = id + CONNECTOR_TABLE_ID_OFFSET;
    }

    public long asLong() {
        return id;
    }

    @Override
    public String toString() {
        return String.format("F%02d", id);
    }

    public static final class Generator {
        private final AtomicLong nextId = new AtomicLong(0);

        public ConnectorTableId getNextId() {
            return new ConnectorTableId(nextId.getAndIncrement());
        }

        public ConnectorTableId getMaxId() {
            return new ConnectorTableId(nextId.get() - 1);
        }
    }
}
