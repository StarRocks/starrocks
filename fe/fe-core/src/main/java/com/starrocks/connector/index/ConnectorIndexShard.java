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

package com.starrocks.connector.index;

import java.util.Objects;

/** Inclusive row-id range evaluated as one distributed connector-index shard. */
public final class ConnectorIndexShard {
    private final long from;
    private final long to;

    public ConnectorIndexShard(long from, long to) {
        if (from < 0 || to < from) {
            throw new IllegalArgumentException("Invalid connector index shard [" + from + ", " + to + "]");
        }
        this.from = from;
        this.to = to;
    }

    public long getFrom() {
        return from;
    }

    public long getTo() {
        return to;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof ConnectorIndexShard)) {
            return false;
        }
        ConnectorIndexShard that = (ConnectorIndexShard) other;
        return from == that.from && to == that.to;
    }

    @Override
    public int hashCode() {
        return Objects.hash(from, to);
    }

    @Override
    public String toString() {
        return "[" + from + ", " + to + "]";
    }
}
