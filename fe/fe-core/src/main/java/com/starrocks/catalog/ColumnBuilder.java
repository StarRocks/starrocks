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

import com.starrocks.persist.ColumnIdExpr;
import com.starrocks.sql.ast.ColumnDef;
import com.starrocks.sql.ast.expression.Expr;

public class ColumnBuilder {
    public static Column buildColumn(ColumnDef columnDef) {
        // Decide final default value here instead of mutating ColumnDef
        ColumnDef.DefaultValueDef effectiveDefault = getDefaultValueDef(columnDef);

        Column col = new Column(columnDef.getName(),
                columnDef.getTypeDef().getType(),
                columnDef.isKey(),
                columnDef.getAggregateType(),
                columnDef.getAggStateDesc(),
                columnDef.isAllowNull(),
                effectiveDefault,
                columnDef.getComment(),
                Column.COLUMN_UNIQUE_ID_INIT_VALUE);
        col.setIsAutoIncrement(columnDef.isAutoIncrement());
        return col;
    }

    public static Column buildGeneratedColumn(Table table, ColumnDef columnDef) {
        // Decide final default value here instead of mutating ColumnDef
        ColumnDef.DefaultValueDef effectiveDefault = getDefaultValueDef(columnDef);

        Column col = new Column(columnDef.getName(),
                columnDef.getTypeDef().getType(),
                columnDef.isKey(),
                columnDef.getAggregateType(),
                columnDef.getAggStateDesc(),
                columnDef.isAllowNull(),
                effectiveDefault,
                columnDef.getComment(),
                Column.COLUMN_UNIQUE_ID_INIT_VALUE);
        col.setIsAutoIncrement(columnDef.isAutoIncrement());

        Expr generatedColumnExpr = columnDef.getGeneratedColumnExpr();
        if (generatedColumnExpr != null) {
            if (table != null) {
                col.setGeneratedColumnExpr(ColumnIdExpr.create(table.getNameToColumn(), generatedColumnExpr));
            } else {
                col.setGeneratedColumnExpr(ColumnIdExpr.create(generatedColumnExpr));
            }
        }
        return col;
    }

    private static ColumnDef.DefaultValueDef getDefaultValueDef(ColumnDef columnDef) {
        ColumnDef.DefaultValueDef effectiveDefault = columnDef.getDefaultValueDef();
        Type type = columnDef.getTypeDef().getType();
        AggregateType aggregateType = columnDef.getAggregateType();
        if (type.isHllType() || type.isBitmapType()) {
            effectiveDefault = ColumnDef.DefaultValueDef.EMPTY_VALUE;
        }
        if (aggregateType == AggregateType.REPLACE_IF_NOT_NULL && !effectiveDefault.isSet) {
            effectiveDefault = ColumnDef.DefaultValueDef.NULL_DEFAULT_VALUE;
        }
        return effectiveDefault;
    }
}
