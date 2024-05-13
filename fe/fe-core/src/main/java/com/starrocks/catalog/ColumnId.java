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

import java.util.Comparator;
import java.util.Objects;

public class ColumnId {
    private final String id;

    public ColumnId(String id) {
        this.id = id;
    }

    public String getId() {
        return id;
    }

    public static ColumnId create(String id) {
        return new ColumnId(id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ColumnId columnId = (ColumnId) o;
        return Objects.equals(id, columnId.id);
    }

    public boolean equalsIgnoreCase(ColumnId anotherId) {
        if (this == anotherId) {
            return true;
        }

        if (anotherId == null) {
            return false;
        }

        return id != null ? id.equalsIgnoreCase(anotherId.id) : anotherId.id == null;
    }

    @Override
    public String toString() {
        return id;
    }

    public static final Comparator<ColumnId> CASE_INSENSITIVE_ORDER =
            new ColumnId.CaseInsensitiveComparator();

    private static class CaseInsensitiveComparator implements Comparator<ColumnId> {
        public int compare(ColumnId n1, ColumnId n2) {
            return String.CASE_INSENSITIVE_ORDER.compare(n1.id, n2.id);
        }
    }
}
