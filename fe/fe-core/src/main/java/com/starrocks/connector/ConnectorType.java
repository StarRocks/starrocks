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

package com.starrocks.connector;

import com.google.common.collect.ImmutableSet;

import java.util.Set;

public enum ConnectorType {

    ELASTICSEARCH("es"),
    HIVE("hive"),
    ICEBERG("iceberg"),
    JDBC("jdbc"),
    HUDI("hudi");

    public static Set<String> SUPPORT_TYPE_SET = ImmutableSet.of(
            ELASTICSEARCH.getName(),
            HIVE.getName(),
            ICEBERG.getName(),
            JDBC.getName(),
            HUDI.getName()
    );

    ConnectorType(String name) {
        this.name = name;
    }

    private String name;

    public String getName() {
        return name;
    }

    public static boolean isSupport(String name) {
        return SUPPORT_TYPE_SET.contains(name);
    }

    public static ConnectorType from(String name) {
        switch (name) {
            case "es":
                return ELASTICSEARCH;
            case "hive":
                return HIVE;
            case "iceberg":
                return ICEBERG;
            case "jdbc":
                return JDBC;
            case "hudi":
                return HUDI;
            default:
                throw new IllegalStateException("Unexpected value: " + name);
        }
    }

}
