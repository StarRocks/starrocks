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

import com.starrocks.qe.SqlModeHelper;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.parser.NodePosition;

import java.util.Map;

public class SetVarHint extends HintNode {

    public static final String SET_VAR = "SET_VAR";

    public static final String PREFIX = "(";

    public static final String SUFFIX = ")";

    public static final int LEAST_LEN = 9;

    public SetVarHint(NodePosition pos, Map<String, String> value, String hintStr) {
        super(pos, value, hintStr);
    }

    public long getSqlModeHintValue() {
        long sqlMode = 0L;
        if (value.containsKey("sql_mode")) {
            try {
                sqlMode = SqlModeHelper.encode(value.get("sql_mode"));
            } catch (Exception e) {
                // do nothing
            }
        }
        return sqlMode;
    }

    @Override
    public String toSql() {
        return hintStr;
    }


    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitSetVarHint(this, context);
    }
}
