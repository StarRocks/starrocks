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

package com.starrocks.statistic.columns;

import com.starrocks.analysis.TableName;
import org.apache.commons.lang3.StringUtils;

import java.util.function.Predicate;

/**
 * The predicate to identify a table
 */
public class TableNamePredicate implements Predicate<TableName> {

    private final TableName tableName;

    public TableNamePredicate(TableName tableName) {
        this.tableName = tableName;
    }

    @Override
    public boolean test(TableName other) {
        if (tableName == null || other == null) {
            return true;
        }
        // TODO: support catalog
        // if (StringUtils.isNotEmpty(tableName.getCatalog())) {
        //     if (!StringUtils.equalsIgnoreCase(other.getCatalog(), tableName.getCatalog())) {
        //         return false;
        //     }
        // }
        if (StringUtils.isNotEmpty(tableName.getDb())) {
            if (!StringUtils.equalsIgnoreCase(other.getDb(), tableName.getDb())) {
                return false;
            }
        }
        if (StringUtils.isNotEmpty(tableName.getTbl())) {
            if (!StringUtils.equalsIgnoreCase(other.getTbl(), tableName.getTbl())) {
                return false;
            }
        }

        return true;
    }
}
