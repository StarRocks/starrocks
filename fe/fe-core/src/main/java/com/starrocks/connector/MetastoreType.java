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

import com.starrocks.connector.exception.StarRocksConnectorException;

public enum MetastoreType {
    HMS,
    GLUE,
    DLF;

    public static MetastoreType get(String type) {
        if (type.equalsIgnoreCase("hive")) {
            return HMS;
        }

        for (MetastoreType metastoreType : MetastoreType.values()) {
            if (metastoreType.name().equalsIgnoreCase(type)) {
                return metastoreType;
            }
        }
        throw new StarRocksConnectorException("Unsupported metastore type %s", type);
    }
}