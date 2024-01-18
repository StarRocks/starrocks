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

package com.starrocks.authz.authorization;

import com.google.gson.annotations.SerializedName;

public enum ObjectTypeDeprecate {
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
    GLOBAL_FUNCTION(11),
    STORAGE_VOLUME(12),
    PIPE(13);

    @SerializedName("id")
    private final int id;

    ObjectTypeDeprecate(int id) {
        this.id = id;
    }

    public int getId() {
        return id;
    }

    public ObjectType toObjectType() {
        if (this.equals(TABLE)) {
            return ObjectType.TABLE;
        } else if (this.equals(DATABASE)) {
            return ObjectType.DATABASE;
        } else if (this.equals(SYSTEM)) {
            return ObjectType.SYSTEM;
        } else if (this.equals(USER)) {
            return ObjectType.USER;
        } else if (this.equals(RESOURCE)) {
            return ObjectType.RESOURCE;
        } else if (this.equals(VIEW)) {
            return ObjectType.VIEW;
        } else if (this.equals(CATALOG)) {
            return ObjectType.CATALOG;
        } else if (this.equals(MATERIALIZED_VIEW)) {
            return ObjectType.MATERIALIZED_VIEW;
        } else if (this.equals(FUNCTION)) {
            return ObjectType.FUNCTION;
        } else if (this.equals(RESOURCE_GROUP)) {
            return ObjectType.RESOURCE_GROUP;
        } else if (this.equals(GLOBAL_FUNCTION)) {
            return ObjectType.GLOBAL_FUNCTION;
        } else if (this.equals(STORAGE_VOLUME)) {
            return ObjectType.STORAGE_VOLUME;
        } else if (this.equals(PIPE)) {
            return ObjectType.PIPE;
        }
        return null;
    }

    public static ObjectTypeDeprecate toObjectTypeDeprecate(ObjectType objectType) {
        if (objectType.equals(ObjectType.TABLE)) {
            return TABLE;
        } else if (objectType.equals(ObjectType.DATABASE)) {
            return DATABASE;
        } else if (objectType.equals(ObjectType.SYSTEM)) {
            return SYSTEM;
        } else if (objectType.equals(ObjectType.USER)) {
            return USER;
        } else if (objectType.equals(ObjectType.RESOURCE)) {
            return RESOURCE;
        } else if (objectType.equals(ObjectType.VIEW)) {
            return VIEW;
        } else if (objectType.equals(ObjectType.CATALOG)) {
            return CATALOG;
        } else if (objectType.equals(ObjectType.MATERIALIZED_VIEW)) {
            return MATERIALIZED_VIEW;
        } else if (objectType.equals(ObjectType.FUNCTION)) {
            return FUNCTION;
        } else if (objectType.equals(ObjectType.RESOURCE_GROUP)) {
            return RESOURCE_GROUP;
        } else if (objectType.equals(ObjectType.GLOBAL_FUNCTION)) {
            return GLOBAL_FUNCTION;
        } else if (objectType.equals(ObjectType.STORAGE_VOLUME)) {
            return STORAGE_VOLUME;
        } else if (objectType.equals(ObjectType.PIPE)) {
            return PIPE;
        }
        return null;
    }
}
