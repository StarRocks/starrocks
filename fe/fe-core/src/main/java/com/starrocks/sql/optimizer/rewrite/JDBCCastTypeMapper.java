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

package com.starrocks.sql.optimizer.rewrite;

import com.starrocks.catalog.JDBCTable;
import com.starrocks.type.ScalarType;
import com.starrocks.type.Type;

import java.util.Optional;

/**
 * Maps a StarRocks {@link Type} to a JDBC dialect-specific SQL CAST target type name.
 *
 * <p>Push-down emits {@code CAST(... AS <type>)} when a StarRocks predicate needs to be
 * rendered against an external JDBC source. The StarRocks-internal {@link Type#toSql()}
 * output (e.g. {@code bigint(20)}, {@code datetime}, {@code largeint(40)}) is MySQL-flavored
 * and rejected by Postgres / Oracle / ClickHouse, so each dialect needs its own rendering.
 *
 * <p>The whitelist is intentionally conservative — only the seven StarRocks PrimitiveTypes
 * historically allowed on the MySQL push-down path are supported, and only when the target
 * dialect has a portable mapping. Any unsupported (type, dialect) combination returns
 * {@link Optional#empty()}; callers must treat that as "predicate not pushable".
 */
public final class JDBCCastTypeMapper {

    private JDBCCastTypeMapper() {
    }

    public static Optional<String> renderCastType(Type type, JDBCTable.ProtocolType dialect) {
        if (!(type instanceof ScalarType)) {
            return Optional.empty();
        }
        ScalarType scalar = (ScalarType) type;
        switch (dialect) {
            case MYSQL:
            case MARIADB:
                return mysqlLike(scalar);
            case POSTGRES:
                return postgres(scalar);
            case ORACLE:
                return oracle(scalar);
            case CLICKHOUSE:
                return clickhouse(scalar);
            default:
                return Optional.empty();
        }
    }

    private static Optional<String> mysqlLike(ScalarType type) {
        switch (type.getPrimitiveType()) {
            case DATE:
                return Optional.of("date");
            case CHAR:
                return Optional.of("char(" + type.getLength() + ")");
            case DATETIME:
                return Optional.of("datetime");
            case DECIMALV2:
                return Optional.of("decimal(" + type.decimalPrecision() + "," + type.decimalScale() + ")");
            case DOUBLE:
                return Optional.of("double");
            case FLOAT:
                return Optional.of("float");
            case JSON:
                return Optional.of("json");
            default:
                return Optional.empty();
        }
    }

    private static Optional<String> postgres(ScalarType type) {
        switch (type.getPrimitiveType()) {
            case DATE:
                return Optional.of("date");
            case CHAR:
                return Optional.of("char(" + type.getLength() + ")");
            case DATETIME:
                return Optional.of("timestamp");
            case DECIMALV2:
                return Optional.of("numeric(" + type.decimalPrecision() + "," + type.decimalScale() + ")");
            case DOUBLE:
                return Optional.of("double precision");
            case FLOAT:
                return Optional.of("real");
            case JSON:
                return Optional.of("json");
            default:
                return Optional.empty();
        }
    }

    private static Optional<String> oracle(ScalarType type) {
        switch (type.getPrimitiveType()) {
            case DATE:
                return Optional.of("DATE");
            case CHAR:
                return Optional.of("CHAR(" + type.getLength() + ")");
            case DATETIME:
                return Optional.of("TIMESTAMP");
            case DECIMALV2:
                return Optional.of("NUMBER(" + type.decimalPrecision() + "," + type.decimalScale() + ")");
            case DOUBLE:
                return Optional.of("BINARY_DOUBLE");
            case FLOAT:
                return Optional.of("BINARY_FLOAT");
            case JSON:
                // Oracle JSON cast form is not portable across versions.
                return Optional.empty();
            default:
                return Optional.empty();
        }
    }

    private static Optional<String> clickhouse(ScalarType type) {
        switch (type.getPrimitiveType()) {
            case DATE:
                return Optional.of("Date");
            case CHAR:
                return Optional.of("FixedString(" + type.getLength() + ")");
            case DATETIME:
                return Optional.of("DateTime64(6)");
            case DECIMALV2:
                return Optional.of("Decimal(" + type.decimalPrecision() + "," + type.decimalScale() + ")");
            case DOUBLE:
                return Optional.of("Float64");
            case FLOAT:
                return Optional.of("Float32");
            case JSON:
                // ClickHouse JSON type added in 22.4; older versions fail at runtime.
                return Optional.of("JSON");
            default:
                return Optional.empty();
        }
    }
}
