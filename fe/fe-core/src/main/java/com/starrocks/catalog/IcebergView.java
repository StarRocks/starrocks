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

import com.google.common.base.Strings;
import com.starrocks.sql.ast.TableRelation;

import java.util.List;

public class IcebergView extends ConnectorView {
    private final String defaultCatalogName;
    private final String defaultDbName;
    private final String location;
    public static final String STARROCKS_DIALECT = "starrocks";

    public IcebergView(long id, String catalogName, String dbName, String name, List<Column> schema,
                       String definition, String defaultCatalogName, String defaultDbName, String location) {
        super(id, catalogName, dbName, name, schema, definition, TableType.ICEBERG_VIEW);
        this.defaultCatalogName = defaultCatalogName;
        this.defaultDbName = defaultDbName;
        this.location = location;
    }

    @Override
    protected void formatRelations(List<TableRelation> tableRelations, List<String> cteRelationNames) {
        for (TableRelation tableRelation : tableRelations) {
            // iceberg view query statement with external catalog which created by starrocks must have catalog name
            if (Strings.isNullOrEmpty(tableRelation.getName().getCatalog())) {
                tableRelation.getName().setCatalog(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME);
            }

            if (Strings.isNullOrEmpty(tableRelation.getName().getDb())) {
                tableRelation.getName().setDb(defaultDbName);
            }
        }
    }

    @Override
    public String getTableLocation() {
        return location;
    }
}
