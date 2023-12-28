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
import com.google.common.collect.ImmutableSet;
import com.google.gson.annotations.SerializedName;
import com.starrocks.common.Pair;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public class ObjectType {
    @SerializedName("id")
    private final int id;

    private ObjectType(int id) {
        this.id = id;
    }

    public int getId() {
        return id;
    }

    public String name() {
        if (OBJECT_TO_NAME.get(id) != null) {
            return OBJECT_TO_NAME.get(id).first;
        } else {
            return "UNKNOWN";
        }
    }

    public String plural() {
        if (OBJECT_TO_NAME.get(id) != null) {
            return OBJECT_TO_NAME.get(id).second;
        } else {
            return "UNKNOWN";
        }
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
    public static final ObjectType PIPE = new ObjectType(13);
    public static final ObjectType TABLE_COLUMN = new ObjectType(14);
    public static final ObjectType VIEW_COLUMN = new ObjectType(15);
    public static final ObjectType MATERIALIZED_VIEW_COLUMN = new ObjectType(16);

    public static final Set<ObjectType> VALID_OBJECT_TYPE = new ImmutableSet.Builder<ObjectType>().add(
            TABLE,
            DATABASE,
            SYSTEM,
            USER,
            RESOURCE,
            VIEW,
            CATALOG,
            MATERIALIZED_VIEW,
            FUNCTION,
            RESOURCE_GROUP,
            GLOBAL_FUNCTION,
            STORAGE_VOLUME,
            PIPE,
            TABLE_COLUMN,
            VIEW_COLUMN,
            MATERIALIZED_VIEW_COLUMN
    ).build();

    public static final Map<Integer, Pair<String, String>> OBJECT_TO_NAME =
            new ImmutableMap.Builder<Integer, Pair<String, String>>()
                    .put(1, new Pair<>("TABLE", "TABLES"))
                    .put(2, new Pair<>("DATABASE", "DATABASES"))
                    .put(3, new Pair<>("SYSTEM", "SYSTEM"))
                    .put(4, new Pair<>("USER", "USERS"))
                    .put(5, new Pair<>("RESOURCE", "RESOURCES"))
                    .put(6, new Pair<>("VIEW", "VIEWS"))
                    .put(7, new Pair<>("CATALOG", "CATALOGS"))
                    .put(8, new Pair<>("MATERIALIZED VIEW", "MATERIALIZED VIEWS"))
                    .put(9, new Pair<>("FUNCTION", "FUNCTIONS"))
                    .put(10, new Pair<>("RESOURCE GROUP", "RESOURCE GROUPS"))
                    .put(11, new Pair<>("GLOBAL FUNCTION", "GLOBAL FUNCTIONS"))
                    .put(12, new Pair<>("STORAGE VOLUME", "STORAGE VOLUMES"))
                    .put(13, new Pair<>("PIPE", "PIPES"))
                    .put(14, new Pair<>("TABLE_COLUMN", "TABLE COLUMNS"))
                    .put(15, new Pair<>("VIEW_COLUMN", "VIEW COLUMNS"))
                    .put(16, new Pair<>("MATERIALIZED_VIEW_COLUMN", "MATERIALIZED VIEW COLUMNS"))
                    .build();

    public static final Map<String, ObjectType> NAME_TO_OBJECT = VALID_OBJECT_TYPE.stream().collect(Collectors.toMap(
            ObjectType::name, Function.identity()));

    public static final Map<String, ObjectType> PLURAL_TO_OBJECT = VALID_OBJECT_TYPE.stream().collect(Collectors.toMap(
            ObjectType::plural, Function.identity()));

    @Override
    public String toString() {
        return name();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ObjectType that = (ObjectType) o;
        return id == that.id;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }
}
