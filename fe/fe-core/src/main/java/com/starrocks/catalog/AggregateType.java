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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/catalog/AggregateType.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.catalog;

public enum AggregateType {
    SUM("SUM"),
    MIN("MIN"),
    MAX("MAX"),
    REPLACE("REPLACE"),
    REPLACE_IF_NOT_NULL("REPLACE_IF_NOT_NULL"),
    HLL_UNION("HLL_UNION"),
    NONE("NONE"),
    BITMAP_UNION("BITMAP_UNION"),
    PERCENTILE_UNION("PERCENTILE_UNION"),
    AGG_STATE_UNION("AGG_STATE_UNION");

    private final String sqlName;

    AggregateType(String sqlName) {
        this.sqlName = sqlName;
    }

    public String toSql() {
        return sqlName;
    }

    @Override
    public String toString() {
        return toSql();
    }

    /**
     * NONE is particular, which is equals to null to make some buggy code compatible
     */
    public static boolean isNullOrNone(AggregateType type) {
        return type == null || type == NONE;
    }

    public boolean isReplaceFamily() {
        switch (this) {
            case REPLACE:
            case REPLACE_IF_NOT_NULL:
                return true;
            default:
                return false;
        }
    }
}

