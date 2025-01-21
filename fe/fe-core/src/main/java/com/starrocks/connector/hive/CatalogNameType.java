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

package com.starrocks.connector.hive;

/**
 * This class will be used for register background refresh in `ConnectorTableMetadataProcessor`.
 * As the unified catalog feature is implemented, we can not use the catalog name as the key
 * in the `cacheUpdateProcessors` map of the `ConnectorTableMetadataProcessor`.
 * So here we introduce this class and use it as the key for that map.
 */
public class CatalogNameType {

    private final String catalogName;
    private final String catalogType;

    public CatalogNameType(String catalogName, String catalogType) {
        this.catalogName = catalogName;
        this.catalogType = catalogType;
    }

    public String getCatalogName() {
        return this.catalogName;
    }

    public String getCatalogType() {
        return this.catalogType;
    }

}
