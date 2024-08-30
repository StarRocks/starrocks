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

package com.starrocks.sql.automv.qe;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.starrocks.analysis.ParseNode;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Catalog;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.server.CatalogMgr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.TableRelation;
import com.starrocks.sql.automv.pieces.FQTable;

import java.util.Map;

public final class FQTableCollector implements AopAstHandler {
    private final Map<String, FQTable> fQTableMap = Maps.newHashMap();

    public void preProcess(Object node) {
        Preconditions.checkArgument(node instanceof ParseNode);
        if (!(node instanceof TableRelation)) {
            return;
        }
        TableRelation tableRel = (TableRelation) node;
        if (fQTableMap.containsKey(tableRel.getTable().getUUID())) {
            return;
        }
        TableName tableName = tableRel.getName();
        Catalog catalog = GlobalStateMgr.getCurrentState().getCatalogMgr().getCatalogByName(tableName.getCatalog());
        Database database = GlobalStateMgr.getCurrentState().getMetadataMgr().getDb(tableName.getCatalog(), tableName.getDb());
        if (database == null) {
            throw new SemanticException("Database %s is not found", tableName.getCatalogAndDb());
        }
        Table table = tableRel.getTable();
        Preconditions.checkArgument(CatalogMgr.isInternalCatalog(tableName.getCatalog()) || catalog != null);
        FQTable fqTable = FQTable.of(catalog, database, table, tableName);
        fQTableMap.put(table.getUUID(), fqTable);
    }

    @Override
    public void postProcess(Object object) {

    }

    public Map<String, FQTable> getFQTableMap() {
        return ImmutableMap.copyOf(fQTableMap);
    }
}