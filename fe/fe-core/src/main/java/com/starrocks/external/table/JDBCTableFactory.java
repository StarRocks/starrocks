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

package com.starrocks.external.table;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.JDBCTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.exception.DdlException;
import com.starrocks.server.AbstractTableFactory;
import com.starrocks.server.LocalMetastore;
import com.starrocks.sql.ast.CreateTableStmt;

import java.util.List;
import java.util.Map;
import javax.validation.constraints.NotNull;

public class JDBCTableFactory implements AbstractTableFactory {
    public static final JDBCTableFactory INSTANCE = new JDBCTableFactory();

    private JDBCTableFactory() {
    }

    @Override
    @NotNull
    public Table createTable(LocalMetastore metastore, Database database, CreateTableStmt stmt) throws DdlException {
        String tableName = stmt.getTableName();
        List<Column> columns = stmt.getColumns();
        Map<String, String> properties = stmt.getProperties();
        long tableId = metastore.getNextId();
        return new JDBCTable(tableId, tableName, columns, properties);
    }
}
