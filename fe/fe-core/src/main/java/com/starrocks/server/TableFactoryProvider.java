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

import com.starrocks.sql.common.EngineType;

import javax.annotation.Nullable;

public class TableFactoryProvider {
    @Nullable
    public static AbstractTableFactory getFactory(String engineName) {
        if (EngineType.OLAP.name().equalsIgnoreCase(engineName)) {
            return OlapTableFactory.INSTANCE;
        }
        if (EngineType.FILE.name().equalsIgnoreCase(engineName)) {
            return FileTableFactory.INSTANCE;
        }
        if (EngineType.HIVE.name().equalsIgnoreCase(engineName)) {
            return HiveTableFactory.INSTANCE;
        }
        if (EngineType.HUDI.name().equalsIgnoreCase(engineName)) {
            return HudiTableFactory.INSTANCE;
        }
        if (EngineType.ICEBERG.name().equalsIgnoreCase(engineName)) {
            return IcebergTableFactory.INSTANCE;
        }
        if (EngineType.JDBC.name().equalsIgnoreCase(engineName)) {
            return JDBCTableFactory.INSTANCE;
        }
        if (EngineType.MYSQL.name().equalsIgnoreCase(engineName)) {
            return MysqlTableFactory.INSTANCE;
        }
        if (EngineType.ELASTICSEARCH.name().equalsIgnoreCase(engineName) || "es".equalsIgnoreCase(engineName)) {
            return ElasticSearchTableFactory.INSTANCE;
        }
        return null;
    }
}
