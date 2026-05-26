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

import com.starrocks.common.DdlException;
import com.starrocks.type.ScalarType;
import com.starrocks.type.Type;

import java.util.List;

public class OlapTableSchemaValidator {
    public static final int MAX_VARCHAR_LENGTH_FOR_TABLE_ROLE = 1024 * 1024;

    private OlapTableSchemaValidator() {
    }

    public static boolean isLargeVarcharColumn(Column column) {
        Type type = column.getType();
        return type.isVarchar()
                && type instanceof ScalarType
                && ((ScalarType) type).getLength() > MAX_VARCHAR_LENGTH_FOR_TABLE_ROLE;
    }

    public static void checkLargeVarcharColumn(Column column, String role) throws DdlException {
        if (isLargeVarcharColumn(column)) {
            throw new DdlException(String.format(
                    "VARCHAR column '%s' with length greater than %d bytes can not be used as %s",
                    Column.removeNamePrefix(column.getName()), MAX_VARCHAR_LENGTH_FOR_TABLE_ROLE, role));
        }
    }

    public static void checkLargeVarcharColumns(List<Column> columns, String role) throws DdlException {
        for (Column column : columns) {
            checkLargeVarcharColumn(column, role);
        }
    }

    public static void checkKeyColumns(List<Column> schema) throws DdlException {
        for (Column column : schema) {
            if (column.isKey()) {
                checkLargeVarcharColumn(column, "key column");
            }
        }
    }

    public static void checkSortKeyColumns(List<Column> schema, List<Integer> sortKeyIdxes) throws DdlException {
        if (sortKeyIdxes == null) {
            return;
        }
        for (Integer sortKeyIdx : sortKeyIdxes) {
            checkLargeVarcharColumn(schema.get(sortKeyIdx), "sort key column");
        }
    }

    public static void checkNamedSortKeyColumns(List<Column> schema, List<String> sortKeys) throws DdlException {
        if (sortKeys == null) {
            return;
        }
        for (String sortKey : sortKeys) {
            for (Column column : schema) {
                if (column.getName().equalsIgnoreCase(sortKey)) {
                    checkLargeVarcharColumn(column, "sort key column");
                    break;
                }
            }
        }
    }
}
