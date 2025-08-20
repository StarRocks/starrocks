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

package com.starrocks.connector.redis;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

/**
 * Groups the field descriptions for value or key.
 */
public class RedisTableFieldGroup {
    private final String dataFormat;
    private final List<RedisTableFieldDescription> fields;
    private final String name;

    @JsonCreator
    public RedisTableFieldGroup(
            @JsonProperty("dataFormat") String dataFormat,
            @JsonProperty("name") String name,
            @JsonProperty("fields") List<RedisTableFieldDescription> fields) {
        this.dataFormat = requireNonNull(dataFormat, "dataFormat is null");
        this.name = name;
        if (!dataFormat.equals("set") && !dataFormat.equals("zset")) {
            this.fields = ImmutableList.copyOf(requireNonNull(fields, "fields is null"));
        } else {
            this.fields = null;
        }
    }

    @JsonProperty
    public String getDataFormat() {
        return dataFormat;
    }

    @JsonProperty
    public String getName() {
        return name;
    }

    @JsonProperty
    public List<RedisTableFieldDescription> getFields() {
        return fields;
    }

    @Override
    public String toString() {
        return toStringHelper(this)
                .add("dataFormat", dataFormat)
                .add("name", name)
                .add("fields", fields)
                .toString();
    }
}
