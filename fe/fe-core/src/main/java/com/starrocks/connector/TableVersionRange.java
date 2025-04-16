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

import org.apache.commons.lang3.builder.ToStringBuilder;

import java.util.Objects;
import java.util.Optional;

// This is a query options. The main function is to ensure that the same version is used during the query process.
// For tables that support time travel with query period, before calling the `ConnectorMetadata` interface,
// you need to obtain the table version by `getTableVersionRange` interface for passing,
// otherwise it will be used as an empty table.

public class TableVersionRange {
    private final Optional<Long> start;
    private final Optional<Long> end;

    public static TableVersionRange empty() {
        return new TableVersionRange(Optional.empty(), Optional.empty());
    }

    public static TableVersionRange withEnd(Optional<Long> end) {
        return new TableVersionRange(Optional.empty(), end);
    }

    public TableVersionRange(Optional<Long> start, Optional<Long> end) {
        this.start = start;
        this.end = end;
    }

    public Optional<Long> start() {
        return start;
    }

    public Optional<Long> end() {
        return end;
    }

    public boolean isEmpty() {
        return start.isEmpty() && end.isEmpty();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("start", start)
                .append("end", end)
                .toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        TableVersionRange that = (TableVersionRange) o;
        return Objects.equals(start, that.start) && Objects.equals(end, that.end);
    }

    @Override
    public int hashCode() {
        return Objects.hash(start, end);
    }
}
