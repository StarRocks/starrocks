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


package com.starrocks.catalog;

import com.google.common.base.Preconditions;

import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;

public class ExternalCatalog extends Catalog {

    public ExternalCatalog(long id, String name, String comment, Map<String, String> config) {
        super(id, name, config, comment);
        Preconditions.checkNotNull(config.get(CATALOG_TYPE));
    }

    // old database uuid format: external_catalog_name.db_name
    // new database uuid format: db_name
    public static String getDbNameFromUUID(String uuid) {
        // To be in compatible with code before external table privilege is supported
        return uuid.contains(".") ? uuid.split("\\.")[1] : uuid;
    }

    // old table uuid format: external_catalog_name.db_name.table_name.creation_time
    // new table uuid format: table_name
    public static String getTableNameFromUUID(String uuid) {
        // To be in compatible with code before external table privilege is supported
        return uuid.contains(".") ? uuid.split("\\.")[2] : uuid;
    }

    // old table uuid format: external_catalog_name.db_name.table_name.creation_time
    // new table uuid format: table_name
    public static String getCompatibleTableUUID(String uuid) {
        return uuid.contains(".") ? uuid.split("\\.")[2] : uuid;
    }

    // old database uuid format: external_catalog_name.db_name
    // new database uuid format: db_name
    public static String getCompatibleDbUUID(String uuid) {
        return uuid.contains(".") ? uuid.split("\\.")[1] : uuid;
    }
}
