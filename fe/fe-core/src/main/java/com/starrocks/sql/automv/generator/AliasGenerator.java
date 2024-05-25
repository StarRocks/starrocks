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

package com.starrocks.sql.automv.generator;

import com.google.common.base.Strings;
import com.google.common.collect.Sets;
import com.starrocks.sql.automv.column.ColumnAlias;

import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nullable;

public interface AliasGenerator {
    static AliasGenerator getDefaultAliasGenerator() {
        return new AliasGenerator() {
            final int nextTableId = 0;
            final Set<String> tableAliasCollision = Sets.newHashSet();
            int nextColumnId = 0;
            String currentTableAlias = null;

            private String suffix(int n, int next) {
                String suffix = Strings.repeat("0", n) + next;
                return suffix.substring(suffix.length() - 4);
            }

            @Override
            public String nextAliasIfTableNameAbsent(@Nullable String tableName) {
                if (tableName != null && !tableAliasCollision.contains(tableName)) {
                    currentTableAlias = tableName;
                } else {
                    currentTableAlias = "_ta" + suffix(4, nextColumnId++);
                }
                tableAliasCollision.add(currentTableAlias);
                return currentTableAlias;
            }

            @Override
            public ColumnAlias nextAliasIfColumnNameAbsent(@Nullable String columnName) {
                return ColumnAlias.of(
                        Objects.requireNonNull(currentTableAlias),
                        Optional.ofNullable(columnName).orElseGet(() -> "_ca" + suffix(4, nextColumnId++)));
            }
        };
    }

    String nextAliasIfTableNameAbsent(@Nullable String tableName);

    ColumnAlias nextAliasIfColumnNameAbsent(@Nullable String columnName);
}
