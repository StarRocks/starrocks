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

package com.starrocks.connector.paimon;

import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.List;
import java.util.Objects;

public class PaimonFilter {
    private final String databaseName;
    private final String tableName;
    private final ScalarOperator predicate;
    private final List<String> fieldNames;

    public PaimonFilter(String databaseName, String tableName, ScalarOperator predicate, List<String> fieldNames) {
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.predicate = predicate;
        this.fieldNames = fieldNames;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        PaimonFilter that = (PaimonFilter) o;
        boolean predicateEqual = false;
        if (predicate != null && that.predicate != null) {
            predicateEqual = predicate.equals(that.predicate);
        } else if (predicate == null && that.predicate == null) {
            predicateEqual = true;
        }

        return Objects.equals(databaseName, that.databaseName) &&
                Objects.equals(tableName, that.tableName) &&
                predicateEqual &&
                fieldNames.equals(that.fieldNames);
    }

    @Override
    public int hashCode() {
        return Objects.hash(databaseName, tableName, fieldNames);
    }
}
