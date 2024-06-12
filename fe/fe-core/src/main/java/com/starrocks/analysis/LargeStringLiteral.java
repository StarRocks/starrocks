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

import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.starrocks.sql.parser.NodePosition;

public class LargeStringLiteral extends StringLiteral {

    public static final int LEN_LIMIT = 50;

    private Supplier<String> shortSqlStr;

    public LargeStringLiteral(String value, NodePosition pos) {
        super(value, pos);
        Preconditions.checkState(value.length() > LEN_LIMIT);
        shortSqlStr = Suppliers.memoize(() -> {
            String fullSql = sqlStr.get();
            fullSql = fullSql.substring(0, LEN_LIMIT);
            return "'" + fullSql + "...'";
        });
    }

    public String toFullSqlImpl() {
        return sqlStr.get();
    }

    @Override
    public String toSqlImpl() {
        return shortSqlStr.get();
    }
}
