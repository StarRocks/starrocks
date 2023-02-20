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

import com.google.gson.annotations.SerializedName;

public enum PrivilegeType {
    GRANT(1),
    NODE(2),
    OPERATE(3),

    DELETE(4),
    DROP(5),
    INSERT(6),
    SELECT(7),
    ALTER(8),
    EXPORT(9),
    UPDATE(10),
    USAGE(11),
    PLUGIN(12),
    FILE(13),
    BLACKLIST(14),
    REPOSITORY(15),
    REFRESH(16),
    IMPERSONATE(17),

    CREATE_DATABASE(18),
    CREATE_TABLE(19),
    CREATE_VIEW(20),
    CREATE_FUNCTION(21),
    CREATE_GLOBAL_FUNCTION(22),
    CREATE_MATERIALIZED_VIEW(23),
    CREATE_RESOURCE(24),
    CREATE_RESOURCE_GROUP(25),
    CREATE_EXTERNAL_CATALOG(26);

    @SerializedName("i")
    private final int id;

    PrivilegeType(int id) {
        this.id = id;
    }

    public int getId() {
        return id;
    }
}
