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

public class PaimonView extends ConnectorView {

    public PaimonView(long id, String catalogName, String dbName, String name, List<Column> schema,
                      String definition) {
        super(id, catalogName, dbName, name, schema, definition, TableType.PAIMON_VIEW);
    }

    @Override
    protected void formatRelations(List<TableRelation> tableRelations, List<String> cteRelationNames) {
        for (TableRelation tableRelation : tableRelations) {
            if (com.google.common.base.Strings.isNullOrEmpty(tableRelation.getName().getCatalog())) {
                tableRelation.getName().setCatalog(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME);
            }

            if (Strings.isNullOrEmpty(tableRelation.getName().getDb())) {
                tableRelation.getName().setDb(super.dbName);
            }
        }
    }
}
