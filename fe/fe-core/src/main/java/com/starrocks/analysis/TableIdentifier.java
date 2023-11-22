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

package com.starrocks.analysis;

import com.google.common.base.Objects;
import com.starrocks.sql.ast.TableRelation;

import static com.starrocks.sql.ast.TableRelation.TableHint._SYNC_MV_;

// store all information to get table like object from metadata
public class TableIdentifier {
    private final TableName tableName;
    private final TableRelation.TableHint tableHint;

    public TableIdentifier(TableName tableName, TableRelation.TableHint tableHint) {
        this.tableName = tableName;
        this.tableHint = tableHint;
    }

    public TableName getTableName() {
        return tableName;
    }

    public TableRelation.TableHint getTableHint() {
        return tableHint;
    }

    public boolean isSyncMv() {
        return _SYNC_MV_ == tableHint;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(tableName, tableHint);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TableIdentifier that = (TableIdentifier) o;
        return Objects.equal(tableName, that.tableName) && Objects.equal(tableHint, that.tableHint);
    }
}
