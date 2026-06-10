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

package com.starrocks.server;

import com.starrocks.catalog.Database;
import com.starrocks.catalog.LanceTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.DdlException;
import com.starrocks.sql.ast.CreateTableStmt;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.validation.constraints.NotNull;


public class LanceTableFactory implements AbstractTableFactory {

    private static final Logger LOG = LogManager.getLogger(LanceTableFactory.class);
    public static final LanceTableFactory INSTANCE = new LanceTableFactory();

    private LanceTableFactory() {}

    @Override
    @NotNull
    public Table createTable(LocalMetastore metastore, Database db, CreateTableStmt stmt) throws DdlException {
        GlobalStateMgr gsm = GlobalStateMgr.getCurrentState();
        long tableId = gsm.getNextId();
        return LanceTable.builder()
                .setId(tableId)
                .setSrTableName(stmt.getTableName())
                .setColumns(stmt.getColumns())
                .setLanceProperties(stmt.getProperties())
                .build();
    }
}
