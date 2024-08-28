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

package com.starrocks.connector.metadata;

public enum MetadataTableType {
    LOGICAL_ICEBERG_METADATA("logical_iceberg_metadata"),
    REFS("refs"),
    HISTORY("history"),
    METADATA_LOG_ENTRIES("metadata_log_entries"),
    SNAPSHOTS("snapshots"),
    MANIFESTS("manifests"),
    FILES("files"),
    PARTITIONS("partitions");

    public final String typeString;

    MetadataTableType(String typeString) {
        this.typeString = typeString;
    }

    public static MetadataTableType get(String type) {
        for (MetadataTableType typeEnum : values()) {
            if (typeEnum.typeString.equalsIgnoreCase(type)) {
                return typeEnum;
            }
        }
        throw new IllegalArgumentException("unknown type " + type);
    }
}
