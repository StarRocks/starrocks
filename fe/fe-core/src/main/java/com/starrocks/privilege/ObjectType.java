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

package com.starrocks.privilege;

import com.google.common.collect.ImmutableMap;
import com.google.gson.annotations.SerializedName;

import java.util.Map;

public enum ObjectType {
    TABLE(1),
    DATABASE(2),
    SYSTEM(3),
    USER(4),
    RESOURCE(5),
    VIEW(6),
    CATALOG(7),
    MATERIALIZED_VIEW(8),
    FUNCTION(9),
    RESOURCE_GROUP(10),
    GLOBAL_FUNCTION(11);

    @SerializedName("id")
    private final int id;

    ObjectType(int id) {
        this.id = id;
    }

    public int getId() {
        return id;
    }

    public static final Map<String, ObjectType> OBJECT_TO_PLURAL = new ImmutableMap.Builder<String, ObjectType>()
            .put("TABLES", ObjectType.TABLE)
            .put("DATABASES", ObjectType.DATABASE)
            .put("USERS", ObjectType.USER)
            .put("RESOURCES", ObjectType.RESOURCE)
            .put("VIEWS", ObjectType.VIEW)
            .put("CATALOGS", ObjectType.CATALOG)
            .put("MATERIALIZED VIEWS", ObjectType.MATERIALIZED_VIEW)
            .put("FUNCTIONS", ObjectType.FUNCTION)
            .put("RESOURCE GROUPS", ObjectType.RESOURCE_GROUP)
            .put("GLOBAL FUNCTIONS", ObjectType.GLOBAL_FUNCTION).build();
}
