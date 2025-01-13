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

package com.starrocks.persist;

import com.google.gson.annotations.SerializedName;
import com.starrocks.common.io.JsonWriter;

import java.util.Objects;

public class SqlBlackListPersistInfo extends JsonWriter {
    public SqlBlackListPersistInfo(long id, String pattern) {
        this.id = id;
        this.pattern = pattern;
    }

    @SerializedName("id")
    public final long id;

    @SerializedName("pattern")
    public final String pattern;

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SqlBlackListPersistInfo that = (SqlBlackListPersistInfo) o;
        return id == that.id && Objects.equals(pattern, that.pattern);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, pattern);
    }
}
