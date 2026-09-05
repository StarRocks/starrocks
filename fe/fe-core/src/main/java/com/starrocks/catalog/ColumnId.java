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


import com.starrocks.common.util.ParseUtil;

import java.util.Comparator;
import java.util.Objects;

public class ColumnId {

    private final String id;

    private ColumnId(String id) {
        this.id = id;
    }

    public static ColumnId create(String id) {
        return new ColumnId(id);
    }

    public String getId() {
        return id;
    }

    @Override
    public int hashCode() {
        return Objects.hash(getId());
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
        return Objects.equals(getId(), columnId.getId());
    }

    public boolean equalsIgnoreCase(ColumnId anotherColumnId) {
        if (this == anotherColumnId) {
            return true;
        }

        if (anotherColumnId == null) {
            return false;
        }

        String myId = getId();
        String anotherId = anotherColumnId.getId();

        return myId != null ? myId.equalsIgnoreCase(anotherId) : anotherId == null;
    }

    @Override
    public String toString() {
        return getId();
    }

    public String toSql(boolean isBackQuoted) {
        if (isBackQuoted) {
            return ParseUtil.backquote(id);
        }
        return id;
    }

    public static final Comparator<ColumnId> CASE_INSENSITIVE_ORDER =
            new ColumnId.CaseInsensitiveComparator();

    private static class CaseInsensitiveComparator implements Comparator<ColumnId> {
        public int compare(ColumnId n1, ColumnId n2) {
            return String.CASE_INSENSITIVE_ORDER.compare(n1.getId(), n2.getId());
        }
    }
}
