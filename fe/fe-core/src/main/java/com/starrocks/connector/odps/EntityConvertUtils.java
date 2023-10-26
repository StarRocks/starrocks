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

package com.starrocks.connector.odps;

import com.aliyun.odps.type.TypeInfo;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Type;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class EntityConvertUtils {
    public static Type convertType(TypeInfo typeInfo) {
        switch (typeInfo.getOdpsType()) {
            case BIGINT:
                return Type.BIGINT;
            case INT:
                return Type.INT;
            case SMALLINT:
                return Type.SMALLINT;
            case TINYINT:
                return Type.TINYINT;
            case FLOAT:
                return Type.FLOAT;
            case DECIMAL:
                return Type.DECIMAL64;
            case DOUBLE:
                return Type.DOUBLE;
            case CHAR:
            case VARCHAR:
            case STRING:
                return Type.VARCHAR;
            case BOOLEAN:
                return Type.BOOLEAN;
            case DATE:
                return Type.DATE;
            case TIMESTAMP:
                return Type.TIME;
            case DATETIME:
                return Type.DATETIME;
            default:
                return Type.VARCHAR;
        }
    }

    public static Column convertColumn(com.aliyun.odps.Column column) {
        return new Column(column.getName(), convertType(column.getTypeInfo()), true);
    }
}
