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
import com.starrocks.analysis.ParseNode;
import com.starrocks.analysis.TableName;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.TableRelation;
import com.starrocks.sql.common.StarRocksPlannerException;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class HiveView extends ConnectorView {
    public enum Type {
        Trino,
        Hive
    }

    public static final String PRESTO_VIEW_PREFIX = "/* Presto View: ";
    public static final String PRESTO_VIEW_SUFFIX = " */";
    private final HiveView.Type viewType;

    public HiveView(long id, String catalogName, String dbName, String name, List<Column> schema, String definition, Type type) {
        super(id, catalogName, dbName, name, schema, definition, TableType.HIVE_VIEW);
        this.viewType = requireNonNull(type, "Hive view type is null");
    }

    @Override
    public QueryStatement doGetQueryStatement(SessionVariable sessionVariable) throws StarRocksPlannerException {
        if (viewType == HiveView.Type.Trino) {
            sessionVariable.setSqlDialect("trino");
        }

        return super.doGetQueryStatement(sessionVariable);
    }

    public ParseNode rollback(SessionVariable sessionVariable) {
        if (viewType == Type.Trino) {
            // try to parse with starrocks sql dialect
            sessionVariable.setSqlDialect("starrocks");
            return com.starrocks.sql.parser.SqlParser.parse(inlineViewDef, sessionVariable).get(0);
        } else {
            return super.rollback(sessionVariable);
        }
    }

    public void formatRelations(List<TableRelation> tableRelations, List<String> cteRelationNames) {
        for (TableRelation tableRelation : tableRelations) {
            TableName name = tableRelation.getName();

            // do not fill catalog and database name to cte relation
            if (Strings.isNullOrEmpty(name.getCatalog()) &&
                    Strings.isNullOrEmpty(name.getDb()) &&
                    cteRelationNames.contains(name.getTbl())) {
                return;
            }

            tableRelation.getName().setCatalog(catalogName);
            if (Strings.isNullOrEmpty(tableRelation.getName().getDb())) {
                tableRelation.getName().setDb(dbName);
            }
        }
    }
}
