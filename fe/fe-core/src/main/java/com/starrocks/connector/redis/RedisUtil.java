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

package com.starrocks.connector.redis;

import com.starrocks.catalog.Column;
import com.starrocks.type.IntegerType;
import com.starrocks.type.Type;
import com.starrocks.type.TypeFactory;
import com.starrocks.type.VarcharType;

import java.util.ArrayList;
import java.util.List;

public class RedisUtil {

    public static List<Column> convertColumnSchema(RedisTableDescription table) {
        List<Column> columns = new ArrayList<>();
        List<RedisTableFieldDescription> tableFields = new ArrayList<>();
        tableFields.addAll(table.getKey().getFields());
        tableFields.addAll(table.getValue().getFields());
        for (RedisTableFieldDescription fieldDescription : tableFields) {
            Type type = convertType(fieldDescription.getType());
            Column column = new Column(fieldDescription.getName(), type, true);
            columns.add(column);
        }
        return columns;
    }

    /**
     * Transfer redis type to sr type.
     **/
    public static Type convertType(String redisType) {
        switch (redisType.toLowerCase()) {
            case "varchar":
                return VarcharType.VARCHAR;
            case "bigint":
                return IntegerType.BIGINT;
            default:
                return TypeFactory.createDefaultCatalogString();
        }
    }
}
