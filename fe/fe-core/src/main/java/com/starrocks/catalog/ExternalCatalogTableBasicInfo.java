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

import com.starrocks.server.CatalogMgr;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.starrocks.service.InformationSchemaDataSource.DEFAULT_EMPTY_NUM;

public class ExternalCatalogTableBasicInfo implements TableBasicInfo {
    private final String catalogName;
    private final String dbName;
    private final String tableName;
    private final Table.TableType tableType;

    public ExternalCatalogTableBasicInfo(String catalogName, String dbName, String tableName, Table.TableType tableType) {
        checkArgument(!CatalogMgr.isInternalCatalog(catalogName));
        checkArgument(tableType != Table.TableType.OLAP);
        this.catalogName = checkNotNull(catalogName);
        this.dbName = checkNotNull(dbName);
        this.tableName = checkNotNull(tableName);
        this.tableType = checkNotNull(tableType);
    }

    @Override
    public String getCatalogName() {
        return catalogName;
    }

    @Override
    public String getName() {
        return tableName;
    }

    @Override
    public String getComment() {
        return "";
    }

    @Override
    public String getMysqlType() {
        return "BASE TABLE";
    }

    @Override
    public String getEngine() {
        return Table.TableType.serialize(this.tableType);
    }

    @Override
    public Table.TableType getType() {
        return tableType;
    }

    @Override
    public boolean isOlapView() {
        return false;
    }

    @Override
    public boolean isMaterializedView() {
        return false;
    }

    @Override
    public boolean isNativeTableOrMaterializedView() {
        return false;
    }

    @Override
    public long getCreateTime() {
        return DEFAULT_EMPTY_NUM;
    }

    @Override
    public long getLastCheckTime() {
        return DEFAULT_EMPTY_NUM;
    }
}
