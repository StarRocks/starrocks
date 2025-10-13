// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.sql.ast.expression;

import com.starrocks.catalog.Table;

/**
 * Legacy base table reference class for actual tables (OLAP, MySQL, etc.).
 * This class extends TableRefPersist and is used for metadata serialization
 * and storage operations. Part of the legacy table reference hierarchy that
 * is being migrated to the new AST-based TableRef class.
 * <p>
 * This class represents resolved table references with actual Table objects,
 * containing the complex analysis state required for backward compatibility
 * with existing serialization formats.
 */
public class BaseTableRefPersist extends TableRefPersist {

    private Table table;

    public BaseTableRefPersist(TableRefPersist ref, Table table, TableName tableName) {
        super(ref);
        this.table = table;
        this.name = tableName;
        // Set implicit aliases if no explicit one was given.
        if (hasExplicitAlias()) {
            return;
        }
        aliases = new String[] {name.toString(), tableName.getNoClusterString(), table.getName()};
    }

    protected BaseTableRefPersist(BaseTableRefPersist other) {
        super(other);
        name = other.name;
        table = other.table;
    }

    @Override
    public TableRefPersist clone() {
        return new BaseTableRefPersist(this);
    }
}
