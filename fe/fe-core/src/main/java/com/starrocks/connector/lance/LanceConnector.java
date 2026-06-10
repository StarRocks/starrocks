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

package com.starrocks.connector.lance;

import com.google.common.base.Strings;
import com.starrocks.connector.Connector;
import com.starrocks.connector.ConnectorContext;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.exception.StarRocksConnectorException;

import java.util.Map;

/**
 * External catalog connector for the Lance format:
 * <pre>
 * CREATE EXTERNAL CATALOG lance_cat PROPERTIES (
 *     "type" = "lance",
 *     "lance.catalog.warehouse" = "oss://bucket/warehouse",
 *     "aws.s3.access_key" = "...", "aws.s3.secret_key" = "...", "aws.s3.endpoint" = "..."
 * );
 * </pre>
 * A database maps to a sub-directory of the warehouse and a table maps to a {@code <name>.lance}
 * dataset under that directory. Storage credentials are taken from the catalog PROPERTIES and
 * propagated to each table so the scan path ({@code LanceConfig#buildStorageOptions}) can use them.
 * No credentials are hardcoded.
 */
public class LanceConnector implements Connector {
    public static final String LANCE_CATALOG_WAREHOUSE = "lance.catalog.warehouse";

    private final String catalogName;
    private final Map<String, String> properties;

    public LanceConnector(ConnectorContext context) {
        this.catalogName = context.getCatalogName();
        this.properties = context.getProperties();
        if (Strings.isNullOrEmpty(properties.get(LANCE_CATALOG_WAREHOUSE))) {
            throw new StarRocksConnectorException("The property %s must be set for a lance catalog.",
                    LANCE_CATALOG_WAREHOUSE);
        }
    }

    @Override
    public ConnectorMetadata getMetadata() {
        return new LanceMetadata(catalogName, properties);
    }
}
