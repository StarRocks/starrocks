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
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.catalog.TableName;
import com.starrocks.sql.automv.util.PrettyPrinter;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class TableNamePlus {
    private final TableName tableName;

    private TableNamePlus(TableName tableName) {
        this.tableName = Objects.requireNonNull(tableName);
    }

    public static TableNamePlus of(TableName tableName) {
        Preconditions.checkArgument(tableName.isFullyQualified());
        String catalogName =
                Optional.ofNullable(tableName.getCatalog()).orElse(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME);
        String dbName = tableName.getDb();
        String tblName = tableName.getTbl();
        return new TableNamePlus(new TableName(catalogName, dbName, tblName));
    }

    public String getFqName() {
        PrettyPrinter printer = new PrettyPrinter();
        List<String> items = Arrays.asList(tableName.getCatalog(), tableName.getDb(), tableName.getTbl());
        printer.addItemsBacktickQuoted(".", items);
        return printer.getResult();
    }
}
