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

public class ObjectType {
    @SerializedName("id")
    private final int id;

    private ObjectType(int id) {
        this.id = id;
    }

    public int getId() {
        return id;
    }

    public static final ObjectType TABLE = new ObjectType(1);
    public static final ObjectType DATABASE = new ObjectType(2);
    public static final ObjectType SYSTEM = new ObjectType(3);
    public static final ObjectType USER = new ObjectType(4);
    public static final ObjectType RESOURCE = new ObjectType(5);
    public static final ObjectType VIEW = new ObjectType(6);
    public static final ObjectType CATALOG = new ObjectType(7);
    public static final ObjectType MATERIALIZED_VIEW = new ObjectType(8);
    public static final ObjectType FUNCTION = new ObjectType(9);
    public static final ObjectType RESOURCE_GROUP = new ObjectType(10);
    public static final ObjectType GLOBAL_FUNCTION = new ObjectType(11);
    public static final ObjectType STORAGE_VOLUME = new ObjectType(12);

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
            .put("GLOBAL FUNCTIONS", ObjectType.GLOBAL_FUNCTION)
            .put("STORAGE VOLUMES", ObjectType.STORAGE_VOLUME).build();
}
