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

import com.starrocks.sql.parser.NodePosition;

/**
 * For explain or profile, we only need to display a portion of the string content.
 * When converting to SQL text, the complete content is required.
 */
public class LargeStringLiteral extends StringLiteral {

    public static final int LEN_LIMIT = 50;

    private String shortSqlStr;

    public LargeStringLiteral(String value, NodePosition pos) {
        super(value, pos);
    }

    public String toFullSqlImpl() {
        return super.toSqlImpl();
    }

    @Override
    public String toSqlImpl() {
        if (shortSqlStr == null) {
            String fullSql = toFullSqlImpl();
            fullSql = fullSql.substring(0, LEN_LIMIT);
            shortSqlStr = fullSql + "...'";
        }
        return shortSqlStr;
    }

    @Override
    public Expr clone() {
        return new LargeStringLiteral(value, pos);
    }
}
