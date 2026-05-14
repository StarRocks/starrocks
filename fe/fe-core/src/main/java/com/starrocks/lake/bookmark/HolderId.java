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

package com.starrocks.lake.bookmark;

import com.google.gson.annotations.SerializedName;
import com.starrocks.catalog.MvId;

import java.util.Objects;

/**
 * Opaque string identity of a bookmark holder. Two holders are equal iff their
 * strings are equal; uniqueness across holders is the caller's responsibility.
 * {@link #forMv} is the only built-in encoder — other holder kinds construct
 * directly via {@link #HolderId(String)}.
 */
public final class HolderId {
    @SerializedName("s")
    private final String id;

    public HolderId(String id) {
        this.id = Objects.requireNonNull(id, "id");
    }

    public static HolderId forMv(MvId mvId) {
        return new HolderId("mv:" + mvId.getDbId() + "-" + mvId.getId());
    }

    public String getId() {
        return id;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof HolderId)) {
            return false;
        }
        return id.equals(((HolderId) o).id);
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }

    @Override
    public String toString() {
        return id;
    }
}
